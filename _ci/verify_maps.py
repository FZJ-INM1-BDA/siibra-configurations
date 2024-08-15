import json
from pathlib import Path
import os

path_to_maps = Path("maps")
path_to_space = Path("spaces")
path_to_parcellations = Path("parcellationschemes")

PARCELLATION_MAP_TYPE = "siibra/atlases/parcellationmap/v0.1"
SPACE_TYPE = "siibra/atlases/space/v0.1"
PARCELLATION_TYPE = "siibra/atlases/parcellationscheme/v0.1"

REMOVE_FROM_NAME = [
    "hemisphere",
    " -",
    "-brain",
    "both",
    "Both",
]

REPLACE_IN_NAME = {
    "ctx-lh-": "left ",
    "ctx-rh-": "right ",
}


def clear_name(name):
    """ clean up a region name to the for matching"""
    result = name
    for word in REMOVE_FROM_NAME:
        result = result.replace(word, "")
    for search, repl in REPLACE_IN_NAME.items():
        result = result.replace(search, repl)
    return " ".join(w for w in result.split(" ") if len(w))

def get_parcellation_region_names(region_tree):
    region_names = []
    for region in region_tree:
        region_names.append(get_name(region))
        children_names = get_parcellation_region_names(region.get("children", []))
        region_names.extend(children_names)
    return region_names

def compare_regions_to_parcellation(map_json, parc_region_tree):
    unmatched_regions = []
    parc_regions = get_parcellation_region_names(parc_region_tree)
    for image in find_images(map_json):
        mapping: dict = image.get("mapping", {})
        for region in mapping.keys():
            if not any(region == parc_region for parc_region in parc_regions):
                unmatched_regions.append(region)
    return unmatched_regions

def get_json(path: Path):
    for dirname, _, filenames in os.walk(path):
        for filename in filenames:
            file = Path(dirname, filename)
            if file.suffix != ".json":
                continue
            with open(file, "r") as fp:
                yield json.load(fp=fp), str(file)

def get_by_type(dir: str|Path, _type: str):
    for f in Path(dir).glob("*.json"):
        json_obj = json.loads(f.read_bytes())
        if json_obj.get("@type") != _type:
            continue
        yield json_obj

def get_maps(dir: str|Path):
    yield from get_by_type(dir, PARCELLATION_MAP_TYPE)

def get_spaces(dir: str|Path):
    yield from get_by_type(dir, SPACE_TYPE)

def get_parcs(dir: str|Path):
    yield from get_by_type(dir, PARCELLATION_TYPE)

def get_id(obj: dict):
    id_attrs = [attr for attr in obj.get("attributes", []) if attr.get("@type") == "siibra/attr/desc/id/v0.1"]
    assert len(id_attrs) == 1, f"expected one and only one id attr"
    return id_attrs[0]["value"]

def get_name(obj: dict):
    
    name_attrs = [attr for attr in obj.get("attributes", []) if attr.get("@type") == "siibra/attr/desc/name/v0.1"]
    assert len(name_attrs) == 1, f"expected one and only one id attr"
    return name_attrs[0]["value"]

def find_images(obj: dict):
    return [attr for attr in obj.get("attributes", []) if attr.get("@type") == "siibra/attr/data/image/v0.1"]

def main():
    parcs = dict()
    space_ids = set()
    fail_flag = False
    for json_obj, _ in get_json(path_to_parcellations):
        parcs[get_id(json_obj)] = json_obj.get("regions")
    for json_obj, _ in get_json(path_to_space):
        space_ids.add(get_id(json_obj))
    for json_obj, filepath in get_json(path_to_maps):
        errmsg = f"map with path {str(filepath)}"
        space_id = json_obj.get("space_id")
        parc_id = json_obj.get("parcellation_id")
        assert space_id, f"{errmsg}, space id not defined!"
        assert parc_id, f"{errmsg}, parc id not defined!"
        assert parc_id in parcs.keys(), f"{errmsg}, parc id for {parc_id} not found in parcellations"
        assert space_id in space_ids, f"{errmsg}, space id for {space_id} not found in spaces"
        unmatched_regions = compare_regions_to_parcellation(json_obj, parcs[parc_id])
        if len(unmatched_regions) > 0:
            fail_flag = True
            print(f"Following regions in {filepath} have no correspondence in parcellation {parc_id}: {unmatched_regions}")
    if fail_flag:
        raise Exception

if __name__ == "__main__":
    main()