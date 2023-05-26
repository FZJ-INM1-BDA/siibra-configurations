import json
from pathlib import Path
import os

path_to_maps = Path("maps")
path_to_space = Path("spaces")
path_to_parcellations = Path("parcellations")

def get_json(path: Path):
    for dirname, _, filenames in os.walk(path):
        for filename in filenames:
            file = Path(dirname, filename)
            if file.suffix != ".json":
                continue
            with open(file, "r") as fp:
                yield json.load(fp=fp), str(file)

def main():
    parc_ids = set()
    space_ids = set()
    for json_obj, _ in get_json(path_to_parcellations):
        parc_ids.add(json_obj.get("@id"))
    for json_obj, _ in get_json(path_to_space):
        space_ids.add(json_obj.get("@id"))
    for json_obj, filepath in get_json(path_to_maps):
        errmsg = f"map with path {str(filepath)}"
        space_id = json_obj.get("space", {}).get("@id")
        parc_id = json_obj.get("parcellation", {}).get("@id")
        assert space_id, f"{errmsg}, space id not defined!"
        assert parc_id, f"{errmsg}, parc id not defined!"
        assert parc_id in parc_ids, f"{errmsg}, parc id for {parc_id} not found in parcellations"
        assert space_id in space_ids, f"{errmsg}, space id for {space_id} not found in spaces"

if __name__ == "__main__":
    main()