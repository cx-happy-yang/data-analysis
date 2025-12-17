from sqlite3 import (
    connect,
    Connection,
)


def create_db() -> Connection:
    db_connection = connect(":memory:")
    db_connection.execute("""CREATE TABLE IF NOT EXISTS results 
    (SCAN_ID VARCHAR, PROJECT_ID VARCHAR, PROJECT_NAME VARCHAR, BRANCH VARCHAR,
    QUERY_NAME VARCHAR, RESULT_SEVERITY VARCHAR, RESULT_QUANTITY INTEGER, 
    PRIMARY KEY (PROJECT_ID, BRANCH, QUERY_NAME) )""")
    return db_connection

