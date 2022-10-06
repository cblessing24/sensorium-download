import argparse
import json
import os

from minio import Minio
from pymongo import MongoClient
from tqdm import tqdm


def main():
    parser = argparse.ArgumentParser(description="Download competition data.")
    parser.add_argument("path")
    args = parser.parse_args()

    download_path = args.path
    try:
        os.mkdir(download_path)
    except FileExistsError:
        print(f"Error: Path {repr(download_path)} already exists. Please remove it.")
        exit(1)
    except FileNotFoundError:
        print(f"Error: Path {repr(os.path.dirname(download_path))} does not exist.")
        exit(1)

    mongo_client = MongoClient(os.environ["MONGODB_URI"])
    minio_client = Minio(os.environ["ENDPOINT"], os.environ["ACCESS_KEY"], os.environ["SECRET_KEY"])

    count = mongo_client.benchmark.Submission.count_documents({})
    for submission in tqdm(mongo_client.benchmark.Submission.find(), total=count):
        submission_path = os.path.join(download_path, str(submission["_id"]))
        os.mkdir(submission_path)
        info_path = os.path.join(submission_path, "info.json")

        submission["id"] = str(submission["_id"])
        del submission["_id"]

        submission["competition"] = {
            "id": str(submission["competition"]),
            "name": mongo_client.benchmark.Competition.find_one({"_id": submission["competition"]})["title"],
        }

        submission["team"] = {
            "id": str(submission["team"]),
            "name": mongo_client.benchmark.Team.find_one({"_id": submission["team"]})["team_name"],
        }

        for key in ["modified_timestamp", "submission_timestamp"]:
            submission[key] = submission[key].isoformat()

        conversion = {"submission": "live_submission", "unknown_submission": "final_submission"}
        for old, new in conversion.items():
            obj_name = submission[old]["path"]
            file_path = os.path.join(submission_path, new + ".csv")
            minio_client.fget_object(os.environ["BUCKET"], obj_name, file_path)
            submission[new] = file_path
            del submission[old]

        with open(info_path, "w") as file:
            json.dump(submission, file, sort_keys=True, indent=4)


if __name__ == "__main__":
    main()
