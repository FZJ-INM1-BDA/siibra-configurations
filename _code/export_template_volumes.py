"""
usage:

at root, run

python _code/export_template_volumes.py
"""

import os
import json
from copy import deepcopy
from typing import Dict, Any

exported_volume_type = (
    "neuroglancer/precomputed",
    "neuroglancer/precompmesh",
    "gii",
)

def convert_dataset_to_vol(dataset: Dict[str, Any]):
    dataset = deepcopy(dataset)
    assert dataset.get("@type") == "fzj/tmp/volume_type/v0.0.1"
    dataset_name = dataset.pop("name")
    assert dataset_name
    unused_keys = (
        "@id",
        "map_type",
        "space_id",
    )

    for key in unused_keys:
        try:
            dataset.pop(key)
        except KeyError:
            ...

    dataset["@type"] = "siibra/volume/v0.0.1"

    volume_type = dataset.pop("volume_type")
    volume_url = dataset.pop("url")

    if isinstance(volume_url, dict):
        return {
            f"{dataset_name}/{key}": {
                **dataset,
                "urls": { "gii-mesh": url }
            }
            for key, url in volume_url.items()
        }
    return {
        f"{dataset_name}": {
            **dataset,
            "urls": { volume_type: volume_url }
        }
    }

def main():
    for dirpath, dirname, filenames in os.walk("./spaces"):
        if "./spaces" != dirpath:
            continue
        for filename in filenames:
            if not filename.endswith(".json"):
                continue
            path_to_json = f"{dirpath}/{filename}"
            with open(path_to_json, "r") as fp:
                space_json = json.load(fp)

            space_json['templates'] = {}
            for ds in space_json.get("datasets", []):
                if ds.get("@type") != "fzj/tmp/volume_type/v0.0.1":
                    continue
                space_json['templates'] = {
                    **space_json.get("templates", {}),
                    **convert_dataset_to_vol(ds)
                }
            with open(path_to_json, "w") as fp:
                json.dump(space_json, indent="\t", fp=fp)
                fp.write("\n")


if __name__ == "__main__":
    main()