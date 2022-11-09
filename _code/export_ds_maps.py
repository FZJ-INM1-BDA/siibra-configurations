"""
Usage:

At root, run

mkdir -p ./maps/ && rm ./maps/* && python _code/export_ds_maps.py
"""

import os
import json
from typing import Dict, Any, List
from copy import deepcopy

space_dict = {
    "minds/core/referencespace/v1.0.0/7f39f7be-445b-47c0-9791-e971c0b6d992": "colin27",
    "minds/core/referencespace/v1.0.0/dafcffc5-4826-4bf1-8ff6-46b8a31ff8e2": "mni152",
    "minds/core/referencespace/v1.0.0/a1655b99-82f1-420f-a3c2-fe80fd4c8588": "bigbrain",
    "minds/core/referencespace/v1.0.0/d5717c4a-0fa1-46e6-918c-b8003069ade8": "rat",
    "minds/core/referencespace/v1.0.0/265d32a0-3d84-40a5-926f-bf89f68212b9": "mouse",
    "minds/core/referencespace/v1.0.0/MEBRAINS": "monkey",
    "minds/core/referencespace/v1.0.0/tmp-fsaverage": "fsaverage",
    "minds/core/referencespace/v1.0.0/tmp-fsaverage6": "fsaverage6",
    "minds/core/referencespace/v1.0.0/tmp-hcp32k": "hcp32k",
}

parc_dict = {
    "minds/core/parcellationatlas/v1.0.0/94c1125b-b87e-45e4-901c-00daee7f2579-290": "jba29",
    "juelich/iav/atlas/v1.0.0/79cbeaa4ee96d5d3dfe2876e9f74b3dc3d3ffb84304fb9b965b1776563a1069c": "sw_hcp",
    "minds/core/parcellationatlas/v1.0.0/ebb923ba-b4d5-4b82-8088-fa9215c2e1fe": "waxholmv3",
    "minds/core/parcellationatlas/v1.0.0/12fca5c5-b02c-46ce-ab9f-f12babf4c7e1": "difumo1024",
    "minds/core/parcellationatlas/v1.0.0/2449a7f0-6dd0-4b5a-8f1e-aec0db03679d": "waxholmv2",
    "https://identifiers.org/neurovault.image:23262": "dk",
    "minds/core/parcellationatlas/v1.0.0/d80fbab2-ce7f-4901-a3a2-3c8ef8a3b721": "difumo64",
    "juelich/iav/atlas/v1.0.0/3": "cortical",
    "juelich/iav/atlas/v1.0.0/6": "swm",
    "minds/core/parcellationatlas/v1.0.0/ebb923ba-b4d5-4b82-8088-fa9215c2e1fe-v4": "waxholmv4",
    "minds/core/parcellationatlas/v1.0.0/39a1384b-8413-4d27-af8d-22432225401f": "ccf2015",
    "minds/core/parcellationatlas/v1.0.0/141d510f-0342-4f94-ace7-c97d5f160235": "difumo256",
    "minds/core/parcellationatlas/v1.0.0/63b5794f-79a4-4464-8dc1-b32e170f3d16": "difumo512",
    "https://identifiers.org/neurovault.image:1705": "cort_thr25",
    "https://identifiers.org/neurovault.image:1702": "cort_thr0",
    "minds/core/parcellationatlas/v1.0.0/11017b35-7056-4593-baad-3934d211daba": "waxholmv1_01",
    "juelich/iav/atlas/v1.0.0/5": "dwm",
    "minds/core/parcellationatlas/v1.0.0/mebrains-tmp-id": "mebrains",
    "minds/core/parcellationatlas/v1.0.0/94c1125b-b87e-45e4-901c-00daee7f2579": "jba118",
    "minds/core/parcellationatlas/v1.0.0/05655b58-3b6f-49db-b285-64b5a0276f83": "ccf2017",
    "juelich/iav/atlas/v1.0.0/4": "isocortex",
    "minds/core/parcellationatlas/v1.0.0/73f41e04-b7ee-4301-a828-4b298ad05ab8": "difumo128",
    "minds/core/parcellationatlas/v1.0.0/94c1125b-b87e-45e4-901c-00daee7f2579-25": "jba25",
    "https://doi.org/10.1016/j.jneumeth.2020.108983/mni152": "vep",
}

def convert_dataset_to_vol(dataset: Dict[str, Any]):
    assert dataset.get("@type") == "fzj/tmp/volume_type/v0.0.1"
    unused_keys = (
        "@id",
        "map_type",
        "space_id",
    )

    for key in unused_keys:
        dataset.pop(key)
    dataset["@type"] == "siibra/volume/v0.0.1"
    return dataset

def get_map_name(spc_id, parc_id, map_type):
    assert spc_id and parc_id
    return f"{space_dict[spc_id]}-{parc_dict[parc_id]}-{map_type}"


expected_map_types = (
    "labelled",
    "continuous",
)

expected_volume_type = (
    "neuroglancer/precomputed",
)


MAP_TEMPLATE = {
	"space": None,
	"parcellation": None,
    "map_type": None,
    "regions": {},
	"volumes": [],
}

class AppendMap:

    def __init__(self, parc_id, space_id, map_type):
        self.parc_id = parc_id
        self.space_id = space_id

        abbrev_name = get_map_name(space_id, parc_id, map_type)
        self.filename = f"maps/{abbrev_name}.json"

        if not os.path.exists(self.filename):
            self.json = deepcopy(MAP_TEMPLATE)

            self.json['space'] = { "@id": space_id }
            self.json['parcellation'] = { "@id": parc_id }
        else:
            with open(self.filename, 'r') as fp:
                self.json = json.load(fp)

    def __enter__(self):
        def set_region(region_name: str, region_values: List[int]):
            assert isinstance(region_name, str)
            if region_name in self.json['regions']:
                raise RuntimeError(f"{region_name} already defined, but being redefined!")
            self.json['regions'][region_name] = region_values

        return (
            lambda vol: self.json["volumes"].append(vol),
            set_region,
        )
    
    def __exit__(self, type, value, traceback):
        with open(self.filename, "w") as fp:
            json.dump(self.json, fp=fp, indent="\t")
            fp.write("\n")

def expand_region(region):
    return [region, *[_r for r in region.get("children", []) for _r in expand_region(r)]]

def process_parc(parc):
    volumes = [v for v in parc.get("datasets", [])
        if v.get("@type") == "fzj/tmp/volume_type/v0.0.1"
        and v.get("volume_type") in expected_volume_type]
    expanded_regions = [r 
        for region in parc.get('regions', [])
        for r in expand_region(region)
        if r.get("labelIndex")]
    for idx, v in enumerate(volumes):
        assert v.get("space_id")
        assert v.get("map_type")
        print(f"Processing {v.get('url')}")
        with AppendMap(parc.get("@id"), v.get("space_id"), v.get("map_type")) as (append_vol, append_region):
            if len(expanded_regions) == 0:
                import pdb
                pdb.set_trace()
            for r in expanded_regions:
                append_region(r.get("name"), [idx, r.get("labelIndex")])
            append_vol(convert_dataset_to_vol(v))

def main():
    for dirpath, dirname, filenames in os.walk("./parcellations"):
        if "./parcellations" != dirpath:
            continue

        for f in filenames:
            if not f.endswith(".json"):
                continue

            print(f"Processing {dirpath}/{f}...")
            with open(f"{dirpath}/{f}", "r") as fp:
                p = json.load(fp)
            process_parc(p)

            print(dirpath)
            pass
    pass

if __name__ == "__main__":
    main()
