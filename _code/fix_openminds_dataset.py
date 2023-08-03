import os
from pathlib import Path
import json

ds_ids_of_interest = [
    "efee2eae-f940-405e-a6f8-90dc29ed50c7",
    "b22db19b-06c6-4ef6-8020-a06b860b7137",
    "fdcda060-bf0b-46b1-8e55-21ef8a56e487",
    "601335dc-510b-41fe-be61-2f814cce4539",
    "4b65d1e7-5655-4749-a000-db0ca0a29c89",

    "c747a346-df2a-4041-af39-1c7e977ff075",
    "265f1e5e-94b5-4d08-b04c-d802f070f918",
    "edbb2baa-f855-44dc-a822-014cf798c784",
    "c747a346-df2a-4041-af39-1c7e977ff075",
    "792eca38-dd69-4c55-8c6a-78c420620f0f",

    "307effe7-4834-42bb-8e66-f2c0b006be1a",
    "2ebf072e-5424-4b4a-9f08-0e3d86d060d4",

    "02d80977-a6c0-44c3-aa50-b739e48559f7",
]

key = "openminds/Dataset"
prev_key = "minds/core/dataset/v1.0.0"
new_key = "openminds/DatasetVersion"

# path to local siibra-snapshot@d56604748ada50713a8f16959ced54b323c0afb5
path_to_snapshot = "../siibra-snapshots"

def main():
    for dirpath, dirnames, filenames in os.walk("features/tabular/fingerprints/receptor"):
        for filename in filenames:
            path_to_json = Path(dirpath, filename)
            if path_to_json.suffix != ".json":
                continue
            with open(path_to_json, "r") as fp:
                feature_json = json.load(fp=fp)
            if key not in feature_json.get("ebrains", {}):
                continue
            ds_id = feature_json.get("ebrains", {}).get(key)
            if ds_id not in ds_ids_of_interest:
                continue
            
            prev_id = feature_json.get("ebrains", {}).get(prev_key)
            assert prev_id

            del feature_json["ebrains"][key]
            feature_json["ebrains"][new_key] = prev_id

            assert Path(path_to_snapshot, "ebrainsquery/v3/DatasetVersion", f"{prev_id}.json").is_file()

            with open(path_to_json, "w") as fp:
                json.dump(feature_json, indent="\t", fp=fp)
                fp.write("\n")
    pass

if __name__ == "__main__":
    main()