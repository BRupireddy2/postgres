# Copyright (c) 2024, PostgreSQL Global Development Group

# Test for replication slots invalidation
use strict;
use warnings FATAL => 'all';

use PostgreSQL::Test::Utils;
use PostgreSQL::Test::Cluster;
use Test::More;
use Time::HiRes qw(usleep);

# =============================================================================
# Testcase start
#
# Invalidate streaming standby slot and logical failover slot on primary due to
# inactive timeout. Also, check logical failover slot synced to standby from
# primary doesn't invalidate on its own, but gets the invalidated state from the
# primary.

# Initialize primary
my $primary = PostgreSQL::Test::Cluster->new('primary');
$primary->init(allows_streaming => 'logical');

# Avoid unpredictability
$primary->append_conf(
	'postgresql.conf', qq{
checkpoint_timeout = 1h
autovacuum = off
});
$primary->start;

# Take backup
my $backup_name = 'my_backup';
$primary->backup($backup_name);

# Create standby
my $standby1 = PostgreSQL::Test::Cluster->new('standby1');
$standby1->init_from_backup($primary, $backup_name, has_streaming => 1);

my $connstr = $primary->connstr;
$standby1->append_conf(
	'postgresql.conf', qq(
hot_standby_feedback = on
primary_slot_name = 'sb_slot1'
primary_conninfo = '$connstr dbname=postgres'
));

# Create sync slot on primary
$primary->psql('postgres',
	q{SELECT pg_create_logical_replication_slot('sync_slot1', 'test_decoding', false, false, true);}
);

$primary->safe_psql(
	'postgres', qq[
    SELECT pg_create_physical_replication_slot(slot_name := 'sb_slot1', immediately_reserve := true);
]);

$standby1->start;

# Wait until standby has replayed enough data
$primary->wait_for_catchup($standby1);

# Sync primary slot to standby
$standby1->safe_psql('postgres', "SELECT pg_sync_replication_slots();");

# Confirm that logical failover slot is created on standby
is( $standby1->safe_psql(
		'postgres',
		q{SELECT count(*) = 1 FROM pg_replication_slots
		  WHERE slot_name = 'sync_slot1' AND synced AND NOT temporary;}
	),
	"t",
	'logical slot sync_slot1 has synced as true on standby');

my $logstart = -s $primary->logfile;
my $inactive_timeout = 1;

# Set timeout so that next checkpoint will invalidate inactive slot
$primary->safe_psql(
	'postgres', qq[
    ALTER SYSTEM SET replication_slot_inactive_timeout TO '${inactive_timeout}s';
]);
$primary->reload;

# Check for logical failover slot to become inactive on primary. Note that
# nobody has acquired slot yet, so it must get invalidated due to
# inactive timeout.
check_for_slot_invalidation($primary, 'sync_slot1', $logstart,
	$inactive_timeout);

# Sync primary slot to standby. Note that primary slot has already been
# invalidated due to inactive timeout. Standby must just sync inavalidated
# state.
$standby1->safe_psql('postgres', "SELECT pg_sync_replication_slots();");
$standby1->poll_query_until(
	'postgres', qq[
	SELECT COUNT(slot_name) = 1 FROM pg_replication_slots
		WHERE slot_name = 'sync_slot1' AND
		invalidation_reason = 'inactive_timeout';
])
  or die
  "Timed out while waiting for sync_slot1 invalidation to be synced on standby";

# Make standby slot on primary inactive and check for invalidation
$standby1->stop;
check_for_slot_invalidation($primary, 'sb_slot1', $logstart,
	$inactive_timeout);

# Testcase end
# =============================================================================

# =============================================================================
# Testcase start
# Synced slot mustn't get invalidated on standby on its own due to inactive
# timeout.

# Disable inactive timeout on primary
$primary->safe_psql(
	'postgres', qq[
    ALTER SYSTEM SET replication_slot_inactive_timeout TO '0';
]);
$primary->reload;

# Create standby
my $standby2 = PostgreSQL::Test::Cluster->new('standby2');
$standby2->init_from_backup($primary, $backup_name, has_streaming => 1);

$connstr = $primary->connstr;
$standby2->append_conf(
	'postgresql.conf', qq(
hot_standby_feedback = on
primary_slot_name = 'sb_slot2'
primary_conninfo = '$connstr dbname=postgres'
));

# Create sync slot on primary
$primary->psql('postgres',
	q{SELECT pg_create_logical_replication_slot('sync_slot2', 'test_decoding', false, false, true);}
);

$primary->safe_psql(
	'postgres', qq[
    SELECT pg_create_physical_replication_slot(slot_name := 'sb_slot2', immediately_reserve := true);
]);

$standby2->start;

# Wait until standby has replayed enough data
$primary->wait_for_catchup($standby2);


$standby2->safe_psql(
	'postgres', qq[
    ALTER SYSTEM SET replication_slot_inactive_timeout TO '${inactive_timeout}s';
]);
$standby2->reload;

# Sync primary slot to standby
$standby2->safe_psql('postgres', "SELECT pg_sync_replication_slots();");

# Confirm that logical failover slot is created on standby
is( $standby2->safe_psql(
		'postgres',
		q{SELECT count(*) = 1 FROM pg_replication_slots
		  WHERE slot_name = 'sync_slot2' AND synced AND NOT temporary;}
	),
	"t",
	'logical slot sync_slot2 has synced as true on standby');

$logstart = -s $standby2->logfile;

# Give enough time
sleep($inactive_timeout+1);

# Despite inactive timeout being set, synced slot won't get invalidated on its
# own on standby. So, we must not see invalidation message in server log.
$standby2->safe_psql('postgres', "CHECKPOINT");
ok( !$standby2->log_contains(
		"invalidating obsolete replication slot \"sync_slot2\"",
		$logstart),
	'check that syned sync_slot2 has not been invalidated on the standby'
);

$standby2->stop;

# Testcase end
# =============================================================================

# =============================================================================
# Testcase start
# Invalidate logical subscriber slot due to inactive timeout.

my $publisher = $primary;

# Prepare for test
$publisher->safe_psql(
	'postgres', qq[
    ALTER SYSTEM SET replication_slot_inactive_timeout TO '0';
]);
$publisher->reload;

# Create subscriber
my $subscriber = PostgreSQL::Test::Cluster->new('sub');
$subscriber->init;
$subscriber->start;

# Create tables
$publisher->safe_psql('postgres', "CREATE TABLE test_tbl (id int)");
$subscriber->safe_psql('postgres', "CREATE TABLE test_tbl (id int)");

# Insert some data
$publisher->safe_psql('postgres',
	"INSERT INTO test_tbl VALUES (generate_series(1, 5));");

# Setup logical replication
my $publisher_connstr = $publisher->connstr . ' dbname=postgres';
$publisher->safe_psql('postgres', "CREATE PUBLICATION pub FOR ALL TABLES");
$publisher->safe_psql(
	'postgres', qq[
    SELECT pg_create_logical_replication_slot(slot_name := 'lsub1_slot', plugin := 'pgoutput');
]);

$subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION sub CONNECTION '$publisher_connstr' PUBLICATION pub WITH (slot_name = 'lsub1_slot', create_slot = false)"
);

$subscriber->wait_for_subscription_sync($publisher, 'sub');

my $result =
  $subscriber->safe_psql('postgres', "SELECT count(*) FROM test_tbl");

is($result, qq(5), "check initial copy was done");

$publisher->safe_psql(
	'postgres', qq[
    ALTER SYSTEM SET replication_slot_inactive_timeout TO ' ${inactive_timeout}s';
]);
$publisher->reload;

$logstart = -s $publisher->logfile;

# Make subscriber slot on publisher inactive and check for invalidation
$subscriber->stop;
check_for_slot_invalidation($publisher, 'lsub1_slot', $logstart,
	$inactive_timeout);

# Testcase end
# =============================================================================

# Check for slot to first become inactive and then get invalidated
sub check_for_slot_invalidation
{
	my ($node, $slot, $offset, $inactive_timeout) = @_;
	my $name = $node->name;

	# Wait for slot to become inactive
	$node->poll_query_until(
		'postgres', qq[
		SELECT COUNT(slot_name) = 1 FROM pg_replication_slots
			WHERE slot_name = '$slot' AND active = 'f' AND
				  inactive_since IS NOT NULL;
	])
	  or die
	  "Timed out while waiting for slot $slot to become inactive on node $name";

	trigger_slot_invalidation($node, $slot, $offset, $inactive_timeout);

	# Wait for invalidation reason to be set
	$node->poll_query_until(
		'postgres', qq[
		SELECT COUNT(slot_name) = 1 FROM pg_replication_slots
			WHERE slot_name = '$slot' AND
			invalidation_reason = 'inactive_timeout';
	])
	  or die
	  "Timed out while waiting for invalidation reason of slot $slot to be set on node $name";

	# Check that invalidated slot cannot be acquired
	my ($result, $stdout, $stderr);

	($result, $stdout, $stderr) = $node->psql(
		'postgres', qq[
			SELECT pg_replication_slot_advance('$slot', '0/1');
	]);

	ok( $stderr =~
		  /can no longer get changes from replication slot "$slot"/,
		"detected error upon trying to acquire invalidated slot $slot on node $name"
	  )
	  or die
	  "could not detect error upon trying to acquire invalidated slot $slot on node $name";
}

# Trigger slot invalidation and confirm it in server log
sub trigger_slot_invalidation
{
	my ($node, $slot, $offset, $inactive_timeout) = @_;
	my $name = $node->name;
	my $invalidated = 0;

	# Give enough time to avoid multiple checkpoints
	sleep($inactive_timeout+1);

	for (my $i = 0; $i < 10 * $PostgreSQL::Test::Utils::timeout_default; $i++)
	{
		$node->safe_psql('postgres', "CHECKPOINT");
		if ($node->log_contains(
				"invalidating obsolete replication slot \"$slot\"",
				$offset))
		{
			$invalidated = 1;
			last;
		}
		usleep(100_000);
	}
	ok($invalidated,
		"check that slot $slot invalidation has been logged on node $name"
	);
}

done_testing();
