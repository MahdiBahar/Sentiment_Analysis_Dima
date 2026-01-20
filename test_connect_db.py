import pandas as pd
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
    row = fetch_data("SELECT * FROM dima_comments LIMIT 1;")
    print(row)

def fetch_comments():
    query = "SELECT id, description,created_at FROM dima_comments LIMIT 100;"
    data = fetch_data(query)
    df = pd.DataFrame(data, columns=["id", "description", "created_at"])  # Adjust columns to match your table
    return df

def test_fetch_comments():
    df = fetch_comments()
    assert not df.empty
    assert "description" in df.columns
    print(df.head())



df = fetch_comments()
print(df.head())