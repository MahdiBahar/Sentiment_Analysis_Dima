from convert_date_AI_aasistant.convert_jalali_to_gregorian_func import parse_timestamp, persian_to_gregorian


from datetime import datetime



import csv
from datetime import datetime

INPUT_CSV = "./convert_date_AI_aasistant/AI_assistant_15Aban_30Dey_test.csv"
OUTPUT_CSV = "./convert_date_AI_aasistant/AI_assistant_15Mehr_2Aban_converted_date.csv"

with open(INPUT_CSV, newline="", encoding="utf-8") as infile, \
     open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as outfile:

    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames + ["date_converted"]

    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()

    for row in reader:
        raw_date = row.get("date", "")

        try:
            parsed_dt = parse_timestamp(raw_date)
            row["date_converted"] = parsed_dt.isoformat(sep=" ")
        except Exception:
            # if parsing fails, keep it empty or log it
            row["date_converted"] = ""

        writer.writerow(row)

print("✅ CSV processed successfully")

