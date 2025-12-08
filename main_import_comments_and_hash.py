from import_comments import get_db_connection, create_table, parse_csv, insert_comments
import sys




def main():
    if len(sys.argv) < 2:
        print("Usage: python import_comments.py <csv_file>")
        sys.exit(1)

    csv_file = sys.argv[1]

    conn = get_db_connection()
    try:
        print("✅ Connected to PostgreSQL")
        create_table(conn)

        comments = parse_csv(csv_file)
        insert_comments(conn, comments)

        print("🎉 Data import completed successfully!")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
