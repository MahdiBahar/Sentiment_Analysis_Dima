from import_AI_assistant import  create_table, parse_csv, insert_comments
import sys

from connect_to_database_func import connect_db
from dotenv import load_dotenv



load_dotenv()

def main():
    import sys

    if len(sys.argv) < 2:
        print("Usage: python main_import_AI_assitant.py <csv_file>")
        return

    csv_file = sys.argv[1]

    conn = connect_db()
    try:
        # 🔍 1) Print where we are really connected to
        dsn = conn.get_dsn_parameters()
        print("Connected to DB:",
              dsn.get("dbname"), "@", dsn.get("host"), "port", dsn.get("port"),
              "as user", dsn.get("user"))
        create_table(conn)         
        # 🔍 2) Count rows BEFORE import
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM dima_AI_assistant;")
            before_count = cur.fetchone()[0]
        print("Row count BEFORE:", before_count)

        # your existing logic:
        
        comments = parse_csv(csv_file)
        insert_comments(conn, comments)

        # 🔍 3) Count rows AFTER import
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM dima_AI_assistant;")
            after_count = cur.fetchone()[0]
        print("Row count AFTER:", after_count)

 
        print("🎉 Data import completed successfully!")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

