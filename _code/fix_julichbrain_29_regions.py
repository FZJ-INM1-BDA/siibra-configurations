import json
import os
from typing import Set, Tuple, Dict

PATH_TO_JULICHBRAIN_29 = "./parcellations/julichbrain_v2_9_0.json"
PATH_TO_MAP_DIR = "./maps"

def iterate_jsons(path_to_walk:str="."):
    for dirpath, dirnames, filenames in os.walk(path_to_walk):
        for filename in filenames:
            if not filename.endswith(".json"):
                continue
            full_filename = f"{dirpath}/{filename}"
            with open(full_filename, "r") as fp:
                text_content = fp.read()
                loaded_json = json.loads(text_content)
            yield (full_filename, text_content, loaded_json)

voltype_url_set: Dict[Tuple[str, str], Dict[str, str]] = dict()

def parse_region(region):
    full_id = region.pop("fullId", None)
    if full_id:
        assert "kgSchema" in full_id and "kgId" in full_id
        if full_id['kgSchema'] != "julich/tmp/parellationregion/v0.0.1":
            region["ebrains"] = {
                full_id["kgSchema"]: full_id["kgId"]
            }
    datasets = region.pop("datasets", [])
    for dataset in datasets:
        assert dataset.get("@type") == "fzj/tmp/volume_type/v0.0.1"
        
        assert "volume_type" in dataset and "url" in dataset
        key = (dataset["volume_type"], dataset["url"])
        assert key in voltype_url_set
        assert voltype_url_set[key] is None

    for c in region.get("children", []):
        parse_region(c)

def main():
    
    for _, text_content, loaded_json in iterate_jsons(PATH_TO_MAP_DIR):
        for volume in loaded_json.get("volumes"):
            ebrains_ref = volume.get("ebrains", None)
            for volume_type, url in volume.get("urls").items():
                voltype_url_set[(volume_type, url)] = ebrains_ref
    
    with open(PATH_TO_JULICHBRAIN_29, "r") as fp:
        julichbrain_json = json.load(fp)
        regions = julichbrain_json.get("regions")
        for region in regions:
            parse_region(region)
        with open(PATH_TO_JULICHBRAIN_29, "w") as fp:
            json.dump(julichbrain_json, indent="\t", fp=fp)
            fp.write("\n")

if __name__ == "__main__":
    main()