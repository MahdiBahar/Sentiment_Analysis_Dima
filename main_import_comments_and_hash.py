from import_comments import get_db_connection, create_table, parse_csv, insert_comments
import sys

from connect_to_database_func import connect_db
from dotenv import load_dotenv
from psycopg2.extras import execute_batch


# def main():
#     if len(sys.argv) < 2:
#         print("Usage: python import_comments.py <csv_file>")
#         sys.exit(1)

#     csv_file = sys.argv[1]

#     conn = get_db_connection()
#     try:
#         print("✅ Connected to PostgreSQL")
#         create_table(conn)

#         comments = parse_csv(csv_file)
#         insert_comments(conn, comments)

#         print("🎉 Data import completed successfully!")
#     finally:
#         conn.close()


# if __name__ == "__main__":
#     main()


from connect_to_database_func import connect_db
from dotenv import load_dotenv
from psycopg2.extras import execute_batch

load_dotenv()

def main():
    import sys

    if len(sys.argv) < 2:
        print("Usage: python main_import_comments_and_hash.py <csv_file>")
        return

    csv_file = sys.argv[1]

    conn = connect_db()
    try:
        # 🔍 1) Print where we are really connected to
        dsn = conn.get_dsn_parameters()
        print("Connected to DB:",
              dsn.get("dbname"), "@", dsn.get("host"), "port", dsn.get("port"),
              "as user", dsn.get("user"))

        # 🔍 2) Count rows BEFORE import
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM comments;")
            before_count = cur.fetchone()[0]
        print("Row count BEFORE:", before_count)

        # your existing logic:
        create_table(conn)           # or remove if you don't want it anymore
        comments = parse_csv(csv_file)
        insert_comments(conn, comments)

        # 🔍 3) Count rows AFTER import
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM comments;")
            after_count = cur.fetchone()[0]
        print("Row count AFTER:", after_count)

        # 🔍 4) Show the 5 most recently imported rows
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, title, grade, description, national_code_hash,
                       created_at, imported_at
                FROM comments
                ORDER BY imported_at DESC
                LIMIT 11;
            """)
            print("Last 11 rows by imported_at:")
            # for row in cur.fetchall():
            #     print(row)



            # for r in cur.fetchall():
    
            #     (id_, title, grade, desc, nc_hash, created_at, imported_at) = r
            #     print(
            #         id_,
            #         title,
            #         grade,
            #         desc,
            #         nc_hash,
            #         created_at.strftime("%Y-%m-%d %H:%M:%S.%f"),
            #         imported_at.strftime("%Y-%m-%d %H:%M:%S.%f"),
                            # )   
        print("🎉 Data import completed successfully!")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
