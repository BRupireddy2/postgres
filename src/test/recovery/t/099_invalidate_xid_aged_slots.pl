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
# Autovacuum stays off until the testcase that needs it, so its naptime and
# logging are set here and turning it on then needs only a reload.
$primary->append_conf(
	'postgresql.conf', qq{
max_slot_xid_age = $slot_xid_age
autovacuum = off
autovacuum_naptime = 1s
log_autovacuum_min_duration = 0
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

# Testcase 1: an active physical slot (aged xmin) is skipped by the VACUUM
# command, which never blocks on an active slot, and invalidated at a
# checkpoint. A running standby keeps the slot active, with an open transaction
# there, reported via feedback, freezing its xmin.
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

$primary->safe_psql('postgres', "VACUUM tbl_user");
is( $primary->safe_psql(
		'postgres',
		qq[SELECT invalidation_reason IS NULL AND active FROM pg_replication_slots WHERE slot_name = 'phys_slot_a';]
	),
	't',
	'active physical slot not invalidated by VACUUM');

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

# Testcase 4: an inactive logical slot (aged catalog_xmin) is invalidated by
# vacuuming a system catalog, whose cutoff includes catalog_xmin. VACUUM is in
# the foreground and the slot is inactive, so it is invalidated synchronously.
$primary->safe_psql('postgres',
	"SELECT pg_create_logical_replication_slot('logical_slot_a', 'pgoutput')"
);
$primary->poll_query_until(
	'postgres', qq[
	SELECT catalog_xmin IS NOT NULL FROM pg_replication_slots
		WHERE slot_name = 'logical_slot_a';
]) or die "Timed out waiting for slot logical_slot_a catalog_xmin";

$primary->safe_psql('postgres', qq{CALL consume_xid(2 * $slot_xid_age)});

$primary->safe_psql('postgres', "VACUUM pg_class");
is( $primary->safe_psql(
		'postgres',
		qq[SELECT invalidation_reason = 'xid_aged' FROM pg_replication_slots WHERE slot_name = 'logical_slot_a';]
	),
	't',
	'inactive logical slot invalidated by vacuuming a system catalog');

# Testcase 5: with an aged physical slot (xmin) and an aged logical slot
# (catalog_xmin) both present, vacuuming a user table invalidates only the
# physical slot. A user table's cutoff uses xmin, not catalog_xmin, so the
# logical slot is not considered. Vacuuming a system catalog then invalidates
# it.
$primary->safe_psql('postgres',
	"SELECT pg_create_logical_replication_slot('logical_slot_b', 'pgoutput')"
);
$primary->poll_query_until(
	'postgres', qq[
	SELECT catalog_xmin IS NOT NULL FROM pg_replication_slots
		WHERE slot_name = 'logical_slot_b';
]) or die "Timed out waiting for slot logical_slot_b catalog_xmin";

# hs_feedback gives the physical slot an xmin, and stopping the standby
# freezes it.
$standby->adjust_conf('postgresql.conf', 'hot_standby_feedback', 'on');
$standby->start;
$primary->wait_for_catchup($standby);

$primary->poll_query_until(
	'postgres', qq[
	SELECT xmin IS NOT NULL FROM pg_replication_slots
		WHERE slot_name = 'phys_slot_b';
]) or die "Timed out waiting for slot phys_slot_b xmin from hs_feedback";

$standby->stop;

$primary->safe_psql('postgres', qq{CALL consume_xid(2 * $slot_xid_age)});

# Remember what the slot holds the cutoff at, before invalidation clears it.
# The vacuum below starts out with this xmin as its cutoff.
my $slot_xmin = $primary->safe_psql('postgres',
	qq[SELECT xmin FROM pg_replication_slots WHERE slot_name = 'phys_slot_b';]
);

$primary->safe_psql('postgres', "VACUUM tbl_user");
is( $primary->safe_psql(
		'postgres',
		qq[SELECT invalidation_reason = 'xid_aged' FROM pg_replication_slots WHERE slot_name = 'phys_slot_b';]
	),
	't',
	'physical slot invalidated by vacuuming a user table');

# The vacuum that invalidated the slot recomputes its cutoff, so it freezes
# past the xmin the slot was holding rather than stopping there.
is( $primary->safe_psql(
		'postgres',
		qq[SELECT age(relfrozenxid) < age('$slot_xmin'::xid) FROM pg_class WHERE relname = 'tbl_user';]
	),
	't',
	'vacuum advances relfrozenxid past the invalidated slot xmin');
is( $primary->safe_psql(
		'postgres',
		qq[SELECT invalidation_reason IS NULL FROM pg_replication_slots WHERE slot_name = 'logical_slot_b';]
	),
	't',
	'logical slot not invalidated by vacuuming a user table');

$primary->safe_psql('postgres', "VACUUM pg_class");
is( $primary->safe_psql(
		'postgres',
		qq[SELECT invalidation_reason = 'xid_aged' FROM pg_replication_slots WHERE slot_name = 'logical_slot_b';]
	),
	't',
	'logical slot invalidated by vacuuming a system catalog');

# Testcase 6: an inactive physical slot (aged xmin) is invalidated by
# autovacuum.
#
# A fresh physical slot for the standby, since the previous one was
# invalidated.
$primary->safe_psql('postgres',
	"SELECT pg_create_physical_replication_slot('phys_slot_c', true)");
$standby->adjust_conf('postgresql.conf', 'primary_slot_name',
	"'phys_slot_c'");
$standby->start;
$primary->wait_for_catchup($standby);

$primary->poll_query_until(
	'postgres', qq[
	SELECT xmin IS NOT NULL FROM pg_replication_slots
		WHERE slot_name = 'phys_slot_c';
]) or die "Timed out waiting for slot phys_slot_c xmin from hs_feedback";

$standby->stop;

$primary->safe_psql('postgres', qq{CALL consume_xid(2 * $slot_xid_age)});

# Turn autovacuum on. The dead tuples only give a worker a reason to vacuum;
# the age check happens in any vacuum, whatever relation it runs on.
$primary->adjust_conf('postgresql.conf', 'autovacuum', 'on');
$primary->reload;
$primary->safe_psql(
	'postgres', q{
	CREATE TABLE tbl_dead (a int);
	INSERT INTO tbl_dead SELECT generate_series(1, 10000);
	DELETE FROM tbl_dead;
});
wait_for_xid_aged_invalidation($primary, 'phys_slot_c');

$primary->stop;

# Testcase 7: a synced slot on a standby (aged catalog_xmin) is invalidated
# by a restartpoint, which releases the catalog_xmin it had pinned on the
# primary's physical slot via hs_feedback. The age limit stays off on the
# primary, or its own checkpoints invalidate the failover slot first.
$primary->adjust_conf('postgresql.conf', 'max_slot_xid_age', '0');
$primary->adjust_conf('postgresql.conf', 'autovacuum', 'off');
$primary->start;

# A fresh slot, as an invalidated one cannot be streamed from.
$primary->safe_psql('postgres',
	"SELECT pg_create_physical_replication_slot('phys_sync_slot', true)");

# Created before the standby, or its xmin lags the standby and never syncs.
$primary->safe_psql('postgres',
	"SELECT pg_create_logical_replication_slot('logical_failover_slot', 'pgoutput', false, false, true)"
);

# Sync needs a dbname; the age limit and hs_feedback are inherited here.
my $connstr = $primary->connstr;
$standby->adjust_conf('postgresql.conf', 'primary_slot_name',
	"'phys_sync_slot'");
$standby->adjust_conf('postgresql.conf', 'primary_conninfo',
	"'$connstr dbname=postgres'");

# One manual sync only, so the synced catalog_xmin stays frozen.
$standby->append_conf('postgresql.conf', "sync_replication_slots = off");
$standby->start;

# The standby has to replay past the new slot, or the sync only retries.
$primary->wait_for_replay_catchup($standby);
$standby->safe_psql('postgres', "SELECT pg_sync_replication_slots()");
is( $standby->safe_psql(
		'postgres',
		qq[SELECT count(*) = 1 FROM pg_replication_slots WHERE slot_name = 'logical_failover_slot' AND synced AND NOT temporary AND catalog_xmin IS NOT NULL AND invalidation_reason IS NULL;]
	),
	't',
	'logical failover slot is synced to the standby');

# The synced slot's catalog_xmin, pinned onto phys_sync_slot via hs_feedback.
my $frozen = $standby->safe_psql('postgres',
	"SELECT catalog_xmin FROM pg_replication_slots WHERE slot_name = 'logical_failover_slot'"
);
$primary->poll_query_until(
	'postgres', qq[
	SELECT catalog_xmin = '$frozen' FROM pg_replication_slots
		WHERE slot_name = 'phys_sync_slot';
])
  or die
  "Timed out waiting for slot phys_sync_slot to hold the synced catalog_xmin";

# Age it out; the primary's checkpoint gives the standby a restartpoint.
$primary->safe_psql('postgres', qq{CALL consume_xid(2 * $slot_xid_age)});
$primary->safe_psql('postgres', "CHECKPOINT");
$primary->wait_for_replay_catchup($standby);
$standby->safe_psql('postgres', "CHECKPOINT");
wait_for_xid_aged_invalidation($standby, 'logical_failover_slot');

# Invalidation does not propagate, so a later sync recreates the slot.
is( $primary->safe_psql(
		'postgres',
		qq[SELECT invalidation_reason IS NULL FROM pg_replication_slots WHERE slot_name = 'logical_failover_slot';]
	),
	't',
	'slot on the primary not invalidated by the standby');

# An invalidated slot drops out of the horizon the standby feeds back.
$primary->poll_query_until(
	'postgres', qq[
	SELECT catalog_xmin IS NULL FROM pg_replication_slots
		WHERE slot_name = 'phys_sync_slot';
])
  or die
  "Timed out waiting for slot phys_sync_slot catalog_xmin to be released";

$standby->stop;
$primary->stop;

done_testing();
