import psycopg2
import pytest
from connect_to_database_func import connect_db


def fetch_data(query):
    conn = connect_db()
    cur = conn.cursor()
    cur.execute(query)
    data = cur.fetchall()
    cur.close()
    conn.close()
    return data

def test_connect_and_get_data():
    result = fetch_data("SELECT 1;")
    assert result[0][0] == 1
    row = fetch_data("SELECT * FROM comments LIMIT 1;")
    print(row)