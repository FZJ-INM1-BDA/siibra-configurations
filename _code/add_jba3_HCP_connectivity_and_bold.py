import siibra
import json
import os
import re
from io import BytesIO


LOCAL_CONFIG_FOLDER = os.path.abspath("")
siibra.use_configuration(LOCAL_CONFIG_FOLDER)

conn_zip_url = "https://data-proxy.ebrains.eu/api/v1/buckets/reference-atlas-data/temp/julich_brain_3/314-JuBrain_SC-FC.zip"
bold_zip_url = "https://data-proxy.ebrains.eu/api/v1/buckets/reference-atlas-data/temp/julich_brain_3/314-Julich-Brain_BOLD.zip"

req = siibra.retrieval.requests.ZipfileRequest(
    conn_zip_url, "314-JuBrain/0ImageProcessing/Link.txt", func=lambda b: b
)
REGION_LIST = [
    line.decode().strip().split(maxsplit=1)[-1]
    for line in BytesIO(req.data).readlines()
    if re.match(r"^\d+ +\w+", line.decode())
]


def create_jba3_duplicate(
    config: dict, zip_url: str, str2replace: str, dataset_uuid: str
):
    if (
        config.get("parcellation").get("@id")
        != "minds/core/parcellationatlas/v1.0.0/94c1125b-b87e-45e4-901c-00daee7f2579-290"
    ):
        return None
    else:
        config.get("parcellation")[
            "@id"
        ] = "minds/core/parcellationatlas/v1.0.0/94c1125b-b87e-45e4-901c-00daee7f2579-300"
    if config.get("cohort") != "HCP":
        return None
    config_dataset_uuid = config.get("ebrains").get("openminds/Dataset")
    if config_dataset_uuid is not None:
        if config_dataset_uuid != dataset_uuid:
            return None

    _ = config.get("ebrains").pop(
        "openminds/DatasetVersion"
    )  # replace this with dataset version uuid when it is released

    config.get("repository")["url"] = zip_url
    config["files"] = {
        sub: file.replace("294-Julich-Brain", str2replace)
        for sub, file in config.get("files").items()
    }
    config["regions"] = REGION_LIST
    return config


def main():
    conn_dataset_uuid = "0f1ccc4a-9a11-4697-b43f-9c9c8ac543e6"
    connectivity_folder = f"{LOCAL_CONFIG_FOLDER}/features/connectivity/regional"

    for conn_type in os.listdir(connectivity_folder):
        conn_type_folder = os.path.join(connectivity_folder, conn_type)
        for file in os.listdir(conn_type_folder):
            file_path = os.path.join(conn_type_folder, file)
            with open(file_path, "r") as fp:
                config = json.load(fp)
            jba3_config = create_jba3_duplicate(
                config, conn_zip_url, "314-JuBrain", conn_dataset_uuid
            )
            if jba3_config is None:
                continue
            else:
                with open(file_path.replace("2_9", "3_0"), "w") as fp:
                    json.dump(jba3_config, fp, indent="\t")

    BOLD_folder = f"{LOCAL_CONFIG_FOLDER}/features/tabular/activity_timeseries/bold"
    for file in os.listdir(BOLD_folder):
        file_path = os.path.join(BOLD_folder, file)
        with open(file_path, "r") as fp:
            config = json.load(fp)
        jba3_config = create_jba3_duplicate(
            config, bold_zip_url, "314-Julich-Brain", ""
        )
        if jba3_config is None:
            continue
        else:
            with open(file_path.replace("2_9", "3_0"), "w") as fp:
                json.dump(jba3_config, fp, indent="\t")


if __name__ == "__main__":
    main()
