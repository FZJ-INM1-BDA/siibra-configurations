"""
usage:

at root, run

python _code/export_template_volumes.py
"""

import os
import json
from copy import deepcopy
from typing import Dict, Any, List

exported_volume_type = (
    "neuroglancer/precomputed",
    "neuroglancer/precompmesh",
    "gii",
)

def convert_dataset_to_vol(dataset: Dict[str, Any]) -> List:
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
    zipped_file = dataset.pop("zipped_file", None)


    if isinstance(volume_url, dict):
        return [
            {
                **dataset,
                "name": f"{dataset_name}/{key}",
                "urls": { "gii-mesh": url }
            }
            for key, url in volume_url.items()
        ]
    if zipped_file:
        return [{
            **dataset,
            "urls": {
                f"zip/{volume_type}" : f"{volume_url} {zipped_file}"
            }
        }]
    return [{
        **dataset,
        "urls": { volume_type: volume_url }
    }]

primitive_types = (
    int,
    float,
    str,
    bool,
)
def deepmerge(dst: Dict[str, Any], src: Dict[str, Any]):
    for key in src:
        if key not in dst:
            dst[key] = src[key]
            continue

        if type(src[key]) in primitive_types:
            if type(dst[key]) not in primitive_types:
                raise RuntimeError(f"dst key {key} is of type {type(dst[key])}, but src has type {type(src[key])}")
            dst[key] = src[key]
            continue

        if type(src[key]) is list:
            assert type(dst[key]) == list
            dst[key].extend(src[key])
            continue
        
        if type(src[key]) is dict:
            assert type(dst[key]) == dict
            deepmerge(dst[key], src[key])
            continue

        raise RuntimeError(f"type {type(src[key])} not in the list above")
    return dst

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

            # remove templates 
            space_json.pop("templates")

            # rename srcVolumeType to modality
            modality = space_json.pop("srcVolumeType", None)
            space_json['modality'] = modality

            # remove templateType
            space_json.pop("templateType", None)

            # remove displayName
            space_json.pop("displayName", None)

            # extract volumes from datasets
            volumes_to_append = []
            for ds in space_json.get("datasets", []):
                if ds.get("@type") != "fzj/tmp/volume_type/v0.0.1":
                    continue
                volumes_to_append.extend(
                    convert_dataset_to_vol(ds)
                )

            # if zipped_nii exists, merge with existin ng volume
            zipped_nii = [vol for vol in volumes_to_append if "zip/nii" in vol.get("urls")]
            if len(zipped_nii) > 0:
                volumes_to_append = [vol for vol in volumes_to_append if "zip/nii" not in vol.get("urls")]
                ng_precomp = [vol for vol in volumes_to_append if "neuroglancer/precomputed" in vol.get("urls")]
                assert len(ng_precomp) == 1
                deepmerge(ng_precomp[0], zipped_nii[0])

            space_json['volumes'] = volumes_to_append
            space_json['datasets'] = [ds for ds in space_json['datasets'] if ds.get("@type") != "fzj/tmp/volume_type/v0.0.1"]

            # extract ebrains reference from datasets
            ebrains_dss = [ds for ds in space_json['datasets'] if ds.get("@type") == "minds/core/dataset/v1.0.0"]
            if len(ebrains_dss) > 0:
                space_json['ebrains'] = {
                    ebrains_ds.get("kgSchema"): ebrains_ds.get("kgId")
                    for ebrains_ds in ebrains_dss
                }
            space_json['datasets'] = [ds for ds in space_json['datasets'] if ds.get("@type") != "minds/core/dataset/v1.0.0"]

            # extract simple desc from datasets
            simple_dss = [ds for ds in space_json['datasets'] if ds.get("@type") == "fzj/tmp/simpleOriginInfo/v0.0.1"]
            if len(simple_dss) > 0:
                assert len(simple_dss) == 1
                space_json['description'] = simple_dss[0].get("description")
            space_json['datasets'] = [ds for ds in space_json['datasets'] if ds.get("@type") != "fzj/tmp/simpleOriginInfo/v0.0.1"]


            # assert no datasets left
            assert len(space_json['datasets']) == 0
            space_json.pop('datasets', None)

            with open(path_to_json, "w") as fp:
                json.dump(space_json, indent="\t", fp=fp)
                fp.write("\n")


if __name__ == "__main__":
    main()