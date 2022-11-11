"""
Usage:

At root, run

mkdir -p ./maps/ && rm ./maps/* && python _code/export_ds_maps.py
"""

import os
import json
from typing import Dict, Any, List
from copy import deepcopy
from enum import Enum

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

expect_map_index_parc_ids = (
    "minds/core/parcellationatlas/v1.0.0/94c1125b-b87e-45e4-901c-00daee7f2579-290",
    "minds/core/parcellationatlas/v1.0.0/94c1125b-b87e-45e4-901c-00daee7f2579-25",
    "minds/core/parcellationatlas/v1.0.0/94c1125b-b87e-45e4-901c-00daee7f2579",
)

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
    dataset = deepcopy(dataset)
    assert dataset.get("@type") == "fzj/tmp/volume_type/v0.0.1"
    unused_keys = (
        "@id",
        "map_type",
        "name",
        "space_id",
    )

    for key in unused_keys:
        dataset.pop(key)
    dataset["@type"] = "siibra/volume/v0.0.1"

    dataset['urls'] = {
        dataset.pop('volume_type'): dataset.pop('url'),
        **dataset.pop("urls", {})
    }
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
    "nii",
    "gii-label",
)


MAP_TEMPLATE = {
	"space": None,
	"parcellation": None,
    "map_type": None,
    "regions": {},
	"volumes": [],
}

class Hemisphere(Enum):
    LEFT="LEFT"
    RIGHT="RIGHT"

def volume_url_is_hemisphere(url: str):
    if "left" in url or "_l" in url:
        return Hemisphere.LEFT
    if "right" in url or "_r" in url:
        return Hemisphere.RIGHT
    return None

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

        self.json['map_type'] = map_type

    def __enter__(self):
        def append_region(region_name: str, region_values: Dict[str, int]):
            assert isinstance(region_name, str)
            if region_name in self.json['regions']:
                print(f"{region_name} already defined, but being redefined!")
                if any([indices.get("map") == region_values.get("map") and indices.get("index") == region_values.get("index")
                    for region_name in self.json['regions']
                    for indices in self.json['regions'][region_name]
                ]):
                    return
            else:
                self.json['regions'][region_name] = []
            self.json['regions'][region_name].append(region_values)

        def append_vol(vol, map_type):

            assert len(vol.get("urls").keys()) == 1

            url_to_idx = {}
            for idx, _vol in enumerate(self.json.get("volumes", [])):
                for volume_type, volume_url in _vol.get("urls", {}).items():
                    url_to_idx[volume_url] = idx
                
            append_url, = [volume_url for (volume_type, volume_url) in vol.get("urls").items()]

            # if the url has already been appended, simply return the index
            if append_url in url_to_idx:
                return url_to_idx[append_url]

            # labelled maps sometimes (often), esp. julich brain, have multiple representations
            # e.g. nii and precomputed
            if map_type == "labelled":
                hemisphere, = [volume_url_is_hemisphere(url) for volume_type, url in vol.get("urls", {}).items()]
                if hemisphere:
                    for idx, existing_vol in enumerate(self.json.get("volumes", [])):
                        existing_hemispheres = {volume_url_is_hemisphere(url) for volume_type, url in existing_vol.get("urls", {}).items()}
                        assert len(existing_hemispheres) > 0
                        if hemisphere in existing_hemispheres:
                            existing_vol['urls'] = {
                                **existing_vol.get("urls", {}),
                                **vol.get("urls", {}),
                            }
                            if "detail" in vol:
                                existing_vol["detail"] = {
                                    **existing_vol.get("detail", {}),
                                    **vol.get("detail", {}),
                                }
                            return idx

            self.json["volumes"].append(vol)
            return len(self.json["volumes"]) - 1

        return (
            append_vol,
            append_region,
            self.json,
        )
    
    def __exit__(self, type, value, traceback):
        with open(self.filename, "w") as fp:
            json.dump(self.json, fp=fp, indent="\t")
            fp.write("\n")

def expand_region(region):
    return [region, *[_r for r in region.get("children", []) for _r in expand_region(r)]]

break_point = (
    "Ch 123 (Basal Forebrain) - left hemisphere",
    "Ch 123 (Basal Forebrain) - right hemisphere",
    "https://neuroglancer.humanbrainproject.eu/precomputed/data-repo-ng-bot/20210616-julichbrain-v2.9.0-complete-mpm/precomputed/GapMapPublicMPMAtlas_l_N10_nlin2StdColin27_29_publicDOI_7f7bae194464eb71431c9916614d5f89",
)

def process_parc(parc):
    volumes = [v for v in parc.get("datasets", [])
        if v.get("@type") == "fzj/tmp/volume_type/v0.0.1"
        and v.get("volume_type") in expected_volume_type]

    flattened_regions = [r 
        for region in parc.get('regions', [])
        for r in expand_region(region)]

    flattened_regions_with_labelindicies = [region
        for region in flattened_regions
        if region.get("labelIndex")]
    
    flattened_regions_with_volumes = [region
        for region in flattened_regions
        if len(
            [ds for ds in region.get("datasets", []) if ds.get("@type") == "fzj/tmp/volume_type/v0.0.1"]
        ) > 0]
    
    distinguish_lrh = parc.get("@id") in expect_map_index_parc_ids

    for v in volumes:
        assert v.get("space_id")
        assert v.get("map_type")

        print(f"Processing {v.get('url')}")
        with AppendMap(parc.get("@id"), v.get("space_id"), v.get("map_type")) as (append_vol, append_region, self_json):
            idx = append_vol(convert_dataset_to_vol(v), v.get("map_type"))
            for region in flattened_regions_with_labelindicies:
                if distinguish_lrh: 
                    if region.get("mapIndex") is None:
                        continue
                    is_lh = "left" in region.get("name")
                    is_rh = "right" in region.get("name")
                    hemisphere = volume_url_is_hemisphere(v.get("url"))
                    
                    if is_lh and hemisphere == Hemisphere.RIGHT:
                        continue
                    if is_rh and hemisphere == Hemisphere.LEFT:
                        continue
                
                append_region(region.get("name"), { "map": idx, "index": region.get("labelIndex") })

    for region in flattened_regions_with_volumes:
        embedded_volumes = [ds for ds in region.get("datasets", []) if ds.get("@type") == "fzj/tmp/volume_type/v0.0.1"]
        for vol in embedded_volumes:

            assert vol.get("space_id")
            assert vol.get('map_type')
            assert vol.get("space_id") == "minds/core/referencespace/v1.0.0/a1655b99-82f1-420f-a3c2-fe80fd4c8588" \
                or vol.get("map_type") == "continuous"
            with AppendMap(parc.get("@id"), vol.get("space_id"), vol.get("map_type")) as (append_vol, append_region, self_json):
                idx = append_vol(convert_dataset_to_vol(vol), vol.get("map_type"))
                label = None
                if vol.get("map_type") == "labelled":
                    label = vol.get("detail", {}).get("neuroglancer/precomputed", {}).get("labelIndex")
                append_region(region.get("name"), { "map": idx, "index": label })
        pass

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
