def test_api_key(api_key):
    assert api_key == "MOCK_KEY1234"


def test_channel_handle(channel_handle):
    assert channel_handle == "MOCK_HANDLE1234"

def test_postgres_con(mock_postgres_conn_vars):
    conn = mock_postgres_conn_vars
    assert conn.host == 'mock_host'
    assert conn.login == 'mock_host'
    assert conn.password == 'mock_password'
    assert conn.port == 1234
    assert conn.schema == 'mock_db'

def test_dags_integrity(dagbag):
    assert dagbag.import_errors == {}, f"Import errors found: {dagbag.import_errors}"
    print("=======")
    print(dagbag.import_errors)

    expected_dag_ids = ['produce_json', 'update_db', 'data_quality']
    loaded_dag_ids = list(dagbag.dags.keys())
    print("=======")
    print(dagbag.dags.keys())

    for dag_id in expected_dag_ids:
        assert dag_id in dagbag.dags, f"DAG '{dag_id}' not found in DAGBag"

    assert dagbag.size() == 3
    print("=============")
    print(f"Total DAGs found: {dagbag.size()}")

    expected_task_counts ={
        'produce_json': 5,
        'update_db': 3,
        'data_quality': 2
    }
    print("=============")
    for dag_id, dag in dagbag.dags.items():
        expected_count = expected_task_counts.get(dag_id, 0)
        task_count = len(dag.tasks)
        assert task_count == expected_count, f"DAG '{dag_id}' has {task_count} tasks, expected {expected_count}"
        print(f"DAG '{dag_id}' has correct number of tasks: {task_count}")