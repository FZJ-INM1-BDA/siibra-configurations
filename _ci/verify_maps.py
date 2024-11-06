import json
from pathlib import Path
import os

path_to_maps = Path("maps")
path_to_space = Path("spaces")
path_to_parcellations = Path("parcellations")

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
    """clean up a region name to the for matching"""
    result = name
    for word in REMOVE_FROM_NAME:
        result = result.replace(word, "")
    for search, repl in REPLACE_IN_NAME.items():
        result = result.replace(search, repl)
    return " ".join(w for w in result.split(" ") if len(w))


def get_parcellation_region_names(region_tree):
    region_names = []
    for region in region_tree:
        for key in region.keys():
            if key == "name":
                region_names.append(region["name"])
            if key == "children":
                children_names = get_parcellation_region_names(region["children"])
                region_names.extend(children_names)
    return region_names


def compare_regions_to_parcellation(map_json, parc_region_tree):
    unmatched_regions = []
    parc_regions = get_parcellation_region_names(parc_region_tree)
    for region in map_json.get("indices").keys():
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


def main():
    parcs = dict()
    space_ids = set()
    for json_obj, _ in get_json(path_to_parcellations):
        parcs[json_obj.get("@id")] = json_obj.get("regions")
    for json_obj, _ in get_json(path_to_space):
        space_ids.add(json_obj.get("@id"))
    for json_obj, filepath in get_json(path_to_maps):
        errmsg = f"map with path {str(filepath)}"
        space_id = json_obj.get("space", {}).get("@id")
        parc_id = json_obj.get("parcellation", {}).get("@id")
        assert space_id, f"{errmsg}, space id not defined!"
        assert parc_id, f"{errmsg}, parc id not defined!"
        assert (
            parc_id in parcs.keys()
        ), f"{errmsg}, parc id for {parc_id} not found in parcellations"
        assert (
            space_id in space_ids
        ), f"{errmsg}, space id for {space_id} not found in spaces"
        unmatched_regions = compare_regions_to_parcellation(json_obj, parcs[parc_id])
        assert len(unmatched_regions) == 0, f"Following regions in {filepath} have no correspondence in parcellation {parc_id}: {unmatched_regions}"


if __name__ == "__main__":
    main()
