# converts energy consumption files
import csv
import datetime
import os
import sys
from typing import Dict, List, Sequence, TextIO

expected_header = ["MålepunktsID", "Fra", "Til", "Navn", "Volum", "Enhet", "Kvalitet", "Opprettet"]
output_header = ["MålepunktsID", "Fra", "Til", "Volum"]


def decode_datetime_field(iso_datetime_with_tz: str):
    """
    Decodes an iso datetime on the form YYYY-MM-DDTHH:MM[:DD][Europe/Oslo]
    """
    iso_datetime = iso_datetime_with_tz.replace("[Europe/Oslo]", "")
    dt = datetime.datetime.fromisoformat(iso_datetime)
    return dt


def merge_observation(items: Dict, new_item: Dict):
    """
    Merges in a new observation, keeping only one observation per timepoint.
    If a conflicting observation already exists, the one with
    a) the higher quality, and b) the latest created time is kept
    """
    key = (new_item["metering_point_id"], new_item["from_datetime"])
    existing_item = items.get(key)

    use_item = (
        existing_item is None
        or existing_item["quality"] < new_item["quality"]
        or (
            existing_item["quality"] == new_item["quality"]
            and existing_item["created_datetime"] < new_item["created_datetime"]
        )
    )

    if use_item:
        items[key] = new_item


def get_observations_in_file(csvfile: TextIO) -> Sequence[Dict]:
    """
    Returns the observations in the file as a dictionary without any filtering
    """
    quality_mapper = {
        "TPC": 0,
        "PPC": 1,
        "FPPC": 2,
        "FPC": 3,
        "21": 4,
        "56": 5,
        "81": 6,
        "127": 7,
    }

    lines_reader = csv.reader(csvfile, delimiter=",", quotechar='"')
    lines = list(lines_reader)

    # verify header
    header = lines[0]
    if any([a != b for a, b in zip(header, expected_header)]):
        raise ValueError(f"Invalid file header.  Expected {','.join(expected_header)} but was {','.join(header)}")

    # parse rows
    print(f"There are {len(lines)} lines in the input file, including header")

    for data_row in lines[1:]:

        yield {
            "metering_point_id": data_row[0],
            "from_datetime": decode_datetime_field(data_row[1]),
            "to_datetime": decode_datetime_field(data_row[2]),
            "name": data_row[3],
            "volume": float(data_row[4].replace(".", "").replace(",", ".")),
            "unit": data_row[5],
            "quality": int(quality_mapper[data_row[6]]),
            "created_datetime": decode_datetime_field(data_row[7]),
        }


def sort_observations(observations: Dict) -> List:
    result = [observations[k] for k in sorted(observations.keys())]
    return result


def save_to_csv():
    pass


if __name__ == "__main__":
    unique_observations = {}
    folder_name = sys.argv[1]

    for path, _, filelist in sorted(os.walk(folder_name)):
        for filename in filelist:
            if filename.endswith('.csv'):
                with open(os.path.join(path, filename), encoding="utf-8") as csv_file:
                    for observation in get_observations_in_file(csv_file):
                        merge_observation(unique_observations, observation)


    print(f"There are {len(unique_observations)} unique data points after processing all files")

    with open("deduplicated.csv", "w", encoding="utf-8") as output_file:
        output_file.write(f"{','.join(output_header)}\n")
        for obs in sort_observations(unique_observations):
            output_file.write(
                f'{obs["metering_point_id"]},{obs["from_datetime"].date()} {obs["from_datetime"].time()},'
                f'{obs["to_datetime"].date()} {obs["to_datetime"].time()},'
                f'{obs["volume"]}'
                f'\n'
                )
