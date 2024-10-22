import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from itertools import repeat
import json
from io import StringIO

from tqdm import tqdm
import requests
from ebrains_iam.client_credential import ClientCredentialsSession
from ebrains_drive import BucketApiClient

EBRAINS_CCFLOW_CLIENT_ID = os.getenv("EBRAINS_CCFLOW_CLIENT_ID")
EBRAINS_CCFLOW_CLIENT_SECRET = os.getenv("EBRAINS_CCFLOW_CLIENT_SECRET")

EBRAINS_QUERY_ID = os.getenv("EBRAINS_QUERY_ID")
EBRAINS_ARTEFACTS_DIR = os.getenv("EBRAINS_ARTEFACTS_DIR")

KG_ROOT = os.getenv("KG_ROOT", "https://core.kg.ebrains.eu")


def get_paginated(url: str, size: int, from_: int, token: str):
    resp = requests.get(
        url,
        params={"stage": "RELEASED", "size": str(size), "from": str(from_)},
        headers={"Authorization": f"Bearer {token}"},
    )
    resp.raise_for_status()
    return resp.json()


def write_to_file(path: str, data):
    with open(path, "w") as fp:
        json.dump(data, indent="\t", fp=fp)
        fp.write("\n")


def main():
    assert EBRAINS_CCFLOW_CLIENT_ID, "EBRAINS_CCFLOW_CLIENT_ID must be defined"
    assert EBRAINS_CCFLOW_CLIENT_SECRET, "EBRAINS_CCFLOW_CLIENT_SECRET must be defined"
    assert EBRAINS_QUERY_ID, "EBRAINS_QUERY_ID must be defined"
    assert EBRAINS_ARTEFACTS_DIR, "EBRAINS_ARTEFACTS_DIR must be defined!"

    iamclient = ClientCredentialsSession(
        EBRAINS_CCFLOW_CLIENT_ID, EBRAINS_CCFLOW_CLIENT_SECRET, scope=["team"]
    )

    path = Path(EBRAINS_ARTEFACTS_DIR)
    path.mkdir(parents=True, exist_ok=True)

    token = iamclient.get_token()

    url = f"{KG_ROOT}/v3/queries/{EBRAINS_QUERY_ID}/instances"
    resp = get_paginated(url, 50, 0, token=token)

    total = resp.get("total")

    print(f"Getting {total=} instances, paginated by 50...")

    with ThreadPoolExecutor(max_workers=6) as ex:
        remaining_results = list(
            ex.map(
                get_paginated,
                repeat(url),
                repeat(50),
                range(50, total, 50),
                repeat(token),
            )
        )

    print("Uploading files ...")

    bucketclient = BucketApiClient(token=token)
    bucket = bucketclient.buckets.get_bucket("reference-atlas-data")

    all_atlas_annotations = [
        datum for r in [resp, *remaining_results] for datum in r.get("data")
    ]

    progress = tqdm(total=len(all_atlas_annotations), desc="Uploading", unit="Files")

    for datum in all_atlas_annotations:
        io = StringIO(json.dumps(datum, indent="\t"))
        io.seek(0)
        _id = datum.get("id").split("/")[-1]

        retry_counter = 0
        while True:
            try:
                bucket.upload(io, f"{EBRAINS_ARTEFACTS_DIR}/{_id}.json")
                progress.update()
                break
            except Exception as e:
                print(f"Error: {str(e)}")
                if retry_counter >= 5:
                    print("Retry max hit, terminating")
                    raise e from e
                retry_counter += 1
                print("Retrying ...")

                token = iamclient.get_token()
                bucketclient = BucketApiClient(token=token)
                bucket = bucketclient.buckets.get_bucket("reference-atlas-data")


if __name__ == "__main__":
    main()
