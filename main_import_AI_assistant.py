from import_AI_assistant import  create_table, parse_csv, insert_comments
import sys

from connect_to_database_func import connect_db
from dotenv import load_dotenv



load_dotenv()

def main():
    import sys

    if len(sys.argv) < 3:
        print("Usage: python main_import_AI_assistant.py <csv_file> <csv_type>")
        return

    csv_file = sys.argv[1]
    csv_type = int(sys.argv[2])  # <-- read the second argument
    if csv_type not in (0, 1):
        print("csv_type must be 0 or 1, defaulting to 0")
        csv_type = 0

    conn = connect_db()
    try:
        # 🔍 1) Print connection info
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

        # 🔍 3) Parse CSV with type
        comments = parse_csv(csv_file, csv_type)
        insert_comments(conn, comments)

        # 🔍 4) Count rows AFTER import
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM dima_AI_assistant;")
            after_count = cur.fetchone()[0]
        print("Row count AFTER:", after_count)

        print("🎉 Data import completed successfully!")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

