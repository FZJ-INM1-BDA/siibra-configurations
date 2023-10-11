# pip install version_query
# pip pinstall git+https://github.com/xgui3783/ebrains-drive.git@feat_allowPublicBktAccess
# until https://github.com/HumanBrainProject/ebrains-drive/pull/23 is merged, and new version is published
from ebrains_drive import BucketApiClient

import os
from pathlib import Path
import json
from typing import Mapping, List
import re
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor


def get_stripped_id(obj: Mapping):
    assert "id" in obj, f"{str(obj)} should have id, but does not"
    _id = obj.get("id")
    searched = re.search(r'(?P<stripped_id>[0-9a-f-]+)$', _id)
    return searched and searched.group('stripped_id')

_COMMON_PREFIX = "ebrainsquery/v3"

_TYPE_DIR_MAPPING = {
    "https://openminds.ebrains.eu/sands/ParcellationTerminologyVersion": "ParcellationTerminologyVersion",
    "https://openminds.ebrains.eu/sands/ParcellationEntityVersion": "ParcellationEntityVersion",
    "https://openminds.ebrains.eu/sands/ParcellationTerminology": "ParcellationTerminology",
    "https://openminds.ebrains.eu/sands/CustomAnatomicalEntity": "CustomAnatomicalEntity"
}

def get_directory(obj: Mapping):
    assert "type" in obj, f"{str(obj)} should have type, but does not"
    _type = obj.get("type")
    assert isinstance(_type, list),  f"{str(_type)} type should be of type list, but is not"
    assert all(isinstance(l, str) for l in _type), f"{str(_type)} type should all be of type str, but they are not"
    assert len(_type) == 1, f"expecting one and only one entry for _type, but got {len(_type)}: {str(_type)}"
    return _TYPE_DIR_MAPPING[_type[0]]

def get_obj_path(obj: Mapping):
    _dir = Path(_COMMON_PREFIX) / get_directory(obj)
    _id = get_stripped_id(obj)
    return str(Path(_dir) / f"{_id}.json")

def get_mutate_region(pevs: List[Mapping], compare_fn=lambda reg, pev: True):

    def get_ebrain_refs(filtered_pevs: List[Mapping]):
        return_obj = {}
        for item in filtered_pevs:
            _item = item.get("type", [])
            assert len(_item) == 1, "expecting one and only one type"
            _item, = _item
            assert _item in _TYPE_DIR_MAPPING
            key = f"openminds/{_TYPE_DIR_MAPPING[_item]}"
            if key in return_obj:
                if not isinstance(return_obj[key], list):
                    return_obj[key] = [return_obj[key]]
                return_obj[key].append(get_stripped_id(item))
            else:
                return_obj[key] = get_stripped_id(item)
        return return_obj

    def mutate_region(region: Mapping):
        filtered_pevs = [pev for pev in pevs if compare_fn(region, pev)]
        if len(filtered_pevs) > 0:
            _ebrains = region.get("ebrains", {})
            region["ebrains"] = {
                **_ebrains,
                **get_ebrain_refs(filtered_pevs),
            }
            
        
        for reg in region.get("children", []):
            mutate_region(reg)

    return mutate_region

def populate_cae():
    client = BucketApiClient()
    bucket = client.buckets.get_bucket("reference-atlas-data")
    files = [file for file in bucket.ls(prefix="ebrainsquery/v3/CustomAnatomicalEntity") if ".json" in file.name]
    
    with ThreadPoolExecutor() as ex:
        file_contents = [
            content for content in tqdm(
                ex.map(
                    lambda a: json.loads(a.get_content().decode()),
                    files
                ),
                total=len(files)
            )
        ]
    
    with open("parcellations/mebrains_parcellation.json", "r") as fp:
        mebrains_parc = json.load(fp=fp)

    regions = mebrains_parc.get("regions", [])

    def is_mebrain(region, pev):
        pev_name: str = pev.get("name")
        return pev_name.endswith(f'{region.get("name")} (MEBRAINS Atlas)')
        

    mutate_region = get_mutate_region(file_contents, is_mebrain)
    for region in regions:
        mutate_region(region)
    
    mebrains_parc["regions"] = regions

    
    with open("parcellations/mebrains_parcellation.json", "w") as fp:
        json.dump(mebrains_parc, indent="\t", fp=fp)
        fp.write("\n")

def main():
    
    populate_cae()

    client = BucketApiClient()
    bucket = client.buckets.get_bucket("reference-atlas-data")

    def get_obj(obj: Mapping):
        obj_path = get_obj_path(obj)
        return json.loads(bucket.get_file(obj_path).get_content())

    def get_pt_ptv_from_bav(bav_json: Mapping):
        terminologies = bav_json.get("hasTerminology", [])
        assert len(terminologies) == 1
        yield from (get_obj(term_ref) for term_ref in terminologies)

    
    for f in os.listdir("parcellations"):
        parc_file = Path("parcellations", f)
        if parc_file.suffix != ".json":
            continue
        with open(parc_file, "r") as fp:
            parc_json = json.load(fp=fp)
        bav = parc_json.get("ebrains", {}).get("openminds/BrainAtlasVersion")
        if not bav:
            continue
        if isinstance(bav, list):
            assert all(isinstance(v, str) for v in bav), f"all known bav should be of instance str"
        elif isinstance(bav, str):
            bav = [bav]
        else:
            raise RuntimeError(f"bav needs to be str or list")
        

        pevs = []

        for _bav in bav:
            print("Processing BAV:", _bav)
            
            file = bucket.get_file(f"{_COMMON_PREFIX}/BrainAtlasVersion/{_bav}.json")
            bav_json = json.loads(file.get_content())
            for term in get_pt_ptv_from_bav(bav_json):
                entities = term.get("hasEntity")
                with ThreadPoolExecutor() as ex:
                    for b in tqdm(
                        ex.map(get_obj, entities),
                        total=len(entities)
                    ):
                        pevs.append(b)
        
        def compare_reg(reg: Mapping, pev: Mapping):
            pev_name = pev.get("name")
            possible_pev_names = [
                pev_name,
                f"{pev_name} - left hemisphere",
                f"{pev_name} left",
                f"{pev_name} - right hemisphere",
                f"{pev_name} right",
            ]
            return reg.get("name") in possible_pev_names
            

        mutate_regionn = get_mutate_region(pevs, compare_reg)
        regions = parc_json.get("regions", [])
        
        for reg in regions:
            mutate_regionn(reg)
        
        parc_json["regions"] = regions

        with open(parc_file, "w") as fp:
            json.dump(parc_json, indent="\t", fp=fp)
            fp.write("\n")

if __name__ == "__main__":
    populate_cae()