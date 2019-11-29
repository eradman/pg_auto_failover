import pgautofailover_utils as pgautofailover
from nose.tools import *

cluster = None
monitor = None
node1 = None
node2 = None

def setup_module():
    global cluster
    cluster = pgautofailover.Cluster()

def teardown_module():
    cluster.destroy()

def test_000_create_monitor():
    global monitor
    monitor = cluster.create_monitor("/tmp/slot/monitor")

def test_001_init_primary():
    global node1
    node1 = cluster.create_datanode("/tmp/slot/node1")
    node1.create()
    node1.run()
    assert node1.wait_until_state(target_state="single")

def test_002_create_t1():
    node1.run_sql_query("CREATE TABLE t1(a int)")
    node1.run_sql_query("INSERT INTO t1 VALUES (1), (2)")

def test_003_init_secondary():
    global node2
    node2 = cluster.create_datanode("/tmp/slot/node2")
    node2.create()
    node2.run()
    assert node2.wait_until_state(target_state="secondary")
    assert node1.wait_until_state(target_state="primary")

def test_004_read_from_secondary():
    results = node2.run_sql_query("SELECT * FROM t1")
    assert results == [(1,), (2,)]

def test_005_replication_slot_first_failover():
    count_slots_query = "select count(*) from pg_replication_slots"

    monitor.failover()
    assert node2.wait_until_state(target_state="primary")
    assert node1.wait_until_state(target_state="secondary")
    node1_replication_slots = node1.run_sql_query(count_slots_query)
    assert node1_replication_slots == [(0,)]
    node2_replication_slots = node2.run_sql_query(count_slots_query)
    assert node2_replication_slots == [(1,)]

def test_006_replication_slot_second_failover():
    count_slots_query = "select count(*) from pg_replication_slots"

    monitor.failover()
    assert node1.wait_until_state(target_state="primary")
    assert node2.wait_until_state(target_state="secondary")
    node1_replication_slots = node1.run_sql_query(count_slots_query);
    assert node1_replication_slots == [(1,)]
    node2_replication_slots = node2.run_sql_query(count_slots_query);
    assert node2_replication_slots == [(0,)]

def test_007_drop_primary():
    node1.drop()
    assert not node1.pg_is_running()
    assert node2.wait_until_state(target_state="single")
