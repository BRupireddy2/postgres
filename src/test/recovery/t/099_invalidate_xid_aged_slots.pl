# Copyright (c) 2026, PostgreSQL Global Development Group
#
# Test for replication slots invalidation due to XID-age

use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Utils;
use PostgreSQL::Test::Cluster;
use Test::More;

# Wait for the given slot to be invalidated due to its XID age
sub wait_for_xid_aged_invalidation
{
	my ($node, $slot_name) = @_;
	$node->poll_query_until(
		'postgres', qq[
		SELECT COUNT(slot_name) = 1 FROM pg_replication_slots
			WHERE slot_name = '$slot_name' AND
			invalidation_reason = 'xid_aged';
	]) or die "Timed out waiting for slot $slot_name to be invalidated";
}

# A small age lets slots reach the limit after just a few XIDs
my $slot_xid_age = 100;

# Defines a procedure that consumes XIDs, one per committed transaction, to
# age a slot's xmin or catalog_xmin. Created on each test primary below.
my $consume_xid_proc = qq{
	CREATE PROCEDURE consume_xid(cnt int)
	AS \$\$
	DECLARE
	    i int;
	BEGIN
	    FOR i IN 1..cnt LOOP
	        EXECUTE 'SELECT pg_current_xact_id()';
	        COMMIT;
	    END LOOP;
	END;
	\$\$ LANGUAGE plpgsql;
};

my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(allows_streaming => 'logical');
$primary->append_conf(
	'postgresql.conf', qq{
max_slot_xid_age = $slot_xid_age
autovacuum = off
checkpoint_timeout = 1h
});
$primary->start;
$primary->safe_psql('postgres', $consume_xid_proc);
$primary->safe_psql('postgres',
	"CREATE TABLE tbl_user AS SELECT generate_series(1,10) AS a");
my $backup_name = 'backup';
$primary->backup($backup_name);

my $standby = PostgreSQL::Test::Cluster->new('standby');
$standby->init_from_backup($primary, $backup_name, has_streaming => 1);

# Testcase 1: an active physical slot (aged xmin) is invalidated at a
# checkpoint. A running standby keeps the slot active; an open transaction
# there, reported via feedback, freezes its xmin.
$primary->safe_psql('postgres',
	"SELECT pg_create_physical_replication_slot('phys_slot_a', true)");

$standby->append_conf(
	'postgresql.conf', q{
primary_slot_name = 'phys_slot_a'
hot_standby_feedback = on
wal_receiver_status_interval = 1
});
$standby->start;
$primary->wait_for_catchup($standby);

# Confirm streaming works
$primary->safe_psql('postgres',
	"INSERT INTO tbl_user SELECT generate_series(11,20)");
$primary->wait_for_replay_catchup($standby);
is($standby->safe_psql('postgres', "SELECT count(*) FROM tbl_user"),
	'20', 'check streamed content on standby');

$primary->poll_query_until(
	'postgres', qq[
	SELECT xmin IS NOT NULL FROM pg_replication_slots
		WHERE slot_name = 'phys_slot_a';
]) or die "Timed out waiting for slot phys_slot_a xmin from hs_feedback";

# Open a transaction on the standby to pin its reported xmin
my $held = $standby->background_psql('postgres');
$held->query_safe("BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT 1;");

$primary->safe_psql('postgres', qq{CALL consume_xid(2 * $slot_xid_age)});

# The checkpoint invalidates the slot
$primary->safe_psql('postgres', "CHECKPOINT");
wait_for_xid_aged_invalidation($primary, 'phys_slot_a');

$held->quit;
$standby->stop;

# Testcase 2: a slot still being created holds vacuum back with an in-memory
# effective_xmin that is never written to disk. Such a slot shows no xmin in
# pg_replication_slots, but its age still counts.
my $blocker = $primary->background_psql('postgres');
$blocker->query_safe('BEGIN; SELECT pg_current_xact_id();');

# The open transaction keeps this slot from reaching a consistent point, so it
# stays in creation and keeps holding its xmin.
my $export = $primary->background_psql('postgres', replication => 'database');
$export->query_until(
	qr/create_started/, q(
\echo create_started
CREATE_REPLICATION_SLOT logical_export_slot LOGICAL pgoutput (SNAPSHOT 'export');
));
$primary->poll_query_until(
	'postgres', qq[
	SELECT count(*) = 1 FROM pg_replication_slots
		WHERE slot_name = 'logical_export_slot' AND catalog_xmin IS NOT NULL;
])
  or die
  "Timed out waiting for slot logical_export_slot to reserve a horizon";

is( $primary->safe_psql(
		'postgres',
		qq[SELECT xmin IS NULL FROM pg_replication_slots WHERE slot_name = 'logical_export_slot';]
	),
	't',
	'slot holding an effective xmin reports no xmin');

$primary->safe_psql('postgres', qq{CALL consume_xid(2 * $slot_xid_age)});

# The slot is in use, so invalidation terminates its owner to release it.
my $log_offset = -s $primary->logfile;
$primary->safe_psql('postgres', "CHECKPOINT");

# The slot holds both an xmin and a catalog_xmin, both aged, so the message
# reports both ages.
ok( $primary->log_contains(
		qr/terminating process \d+ to release replication slot "logical_export_slot"\n.*DETAIL:.*The slot's xmin age of \d+ transactions and catalog xmin age of \d+ transactions exceed the configured "max_slot_xid_age" of $slot_xid_age\./,
		$log_offset),
	'aged slot holding an effective xmin has its owner terminated');

$blocker->quit;
$export->{run}->finish;

# Testcase 3: an inactive logical slot on a standby (aged catalog_xmin) is
# invalidated by a restartpoint.
$primary->safe_psql('postgres',
	"SELECT pg_create_physical_replication_slot('phys_slot_b', true)");

# Reuse the same standby, pointed at the new slot. It inherited the age limit
# from the primary's configuration in the backup, so the restartpoint below
# applies the same limit to its own slot.
$standby->adjust_conf('postgresql.conf', 'primary_slot_name',
	"'phys_slot_b'");
$standby->adjust_conf('postgresql.conf', 'hot_standby_feedback', 'off');
$standby->start;
$primary->wait_for_catchup($standby);

$standby->create_logical_slot_on_standby($primary, 'logical_standby_slot',
	'postgres');
$standby->poll_query_until(
	'postgres', qq[
	SELECT catalog_xmin IS NOT NULL FROM pg_replication_slots
		WHERE slot_name = 'logical_standby_slot';
]) or die "Timed out waiting for logical_standby_slot catalog_xmin";

$primary->safe_psql('postgres', qq{CALL consume_xid(2 * $slot_xid_age)});
$primary->safe_psql('postgres', "CHECKPOINT");
$primary->wait_for_replay_catchup($standby);
$standby->safe_psql('postgres', "CHECKPOINT");
wait_for_xid_aged_invalidation($standby, 'logical_standby_slot');

$standby->stop;
$primary->stop;

done_testing();
