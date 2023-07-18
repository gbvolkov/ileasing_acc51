import re
import os
from datetime import datetime
import csv
from argparse import (
    ArgumentParser,
    ArgumentDefaultsHelpFormatter,
    BooleanOptionalAction,
)
import sys
from os.path import exists as file_exists


def format_date(date_str):
    """
    Formats a date string in the format "dd.mm.yyyy" to a datetime object.

    :param date_str: Date string in the format "dd.mm.yyyy".
    :type date_str: str
    :return: Formatted datetime object.
    :rtype: datetime.datetime
    """
    try:
        # Replace "-" or "/" with "."
        new_str = re.sub("[-/]", ".", date_str)
        return datetime.strptime(new_str, "%d.%m.%Y")
    except Exception:
        # If the date string is not in the correct format, return current datetime
        return datetime.now()

def split_csv(output_folder, input_file):
    """
    Splits a CSV file based on the "CLIENTID" column and saves each split into separate files.

    :param output_folder: Output folder path.
    :type output_folder: str
    :param input_file: Input CSV file path.
    :type input_file: str
    """
    with open(input_file, "r", encoding="utf-8") as file:
        reader = csv.reader(file)
        header = next(reader)
        client_id_idx = header.index("CLIENTID")
        date_idx = header.index("Date")

        current_client = ""
        writer = None
        write_header = False
        csv_writer = None
        for row in reader:
            row[date_idx] = datetime.strftime(format_date(row[date_idx]), "%d.%m.%Y")
            client_id = row[client_id_idx]
            if current_client != client_id:
                current_client = client_id
                if writer is not None:
                    writer.close()
                output_filename = os.path.join(output_folder, f"{current_client}.csv")
                write_header = not file_exists(output_filename)
                writer = open(output_filename, "a+", encoding="utf-8", newline='')
                csv_writer = csv.writer(writer)
            if csv_writer is not None:
                if write_header:
                    write_header = False
                    csv_writer.writerow(header)
                csv_writer.writerow(row)
        if writer is not None:
            writer.close()

def main():
    """
    Main function to split the CSV file based on CLIENTID column.
    """
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("-i", "--input", default="./acc51parsed_full.csv", help="Input file")
    parser.add_argument("-o", "--output", default="./DataSplit", help="Output folder. No ending delimiter")
    args = vars(parser.parse_args())

    input_file = args["input"]
    output_folder = args["output"]

    sys.stdout.reconfigure(encoding="utf-8") # type: ignore
    print("START:", datetime.now())
    print("input:", input_file)
    print("output:", output_folder)

    print("Start transformation")
    split_csv(output_folder, input_file)
    print("FINISHED")

if __name__ == "__main__":
    main()
