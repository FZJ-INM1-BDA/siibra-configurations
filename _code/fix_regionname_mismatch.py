import json
import os
import siibra
from pprint import pprint
LOCAL_CONFIG_FOLDER = "./"
REPLACE_IN_NAME = {
    "left": "- left hemisphere",
    "right": "- right hemisphere"
}


def create_key(regionname):
    for search, repl in REPLACE_IN_NAME.items():
        regionname = regionname.replace(search, repl)
    return regionname


def main():
    siibra.use_configuration(LOCAL_CONFIG_FOLDER)

    mismatched = {}

    maps_folder = f"{LOCAL_CONFIG_FOLDER}/maps"
    for file in os.listdir(maps_folder):
        file_path = os.path.join(maps_folder, file)
        with open(file_path, "r") as fp:
            config = json.load(fp)

        config["filename"] = file
        mp = siibra.from_json(config)
        mismatched_regions = []
        for r in mp.regions:
            try:
                pr = mp.parcellation.get_region(r)
            except Exception:
                print(f"Cannot find the region {r} in {mp.parcellation}")
            if pr.name.lower() != r.lower():
                mismatched_regions.append((r, pr.name))
        if len(mismatched_regions) > 0:
            mismatched[mp.key] = mismatched_regions
        else:
            continue
        indices = config["indices"]
        for (r, pr) in mismatched_regions:
            new_indices = {}
            check_key = create_key(r)
            for key, val in indices.items():
                if check_key.lower() == key.lower():
                    new_key = create_key(pr)
                    new_indices[new_key] = val
                else:
                    new_indices[key] = val
            indices = new_indices
        _ = config.pop("filename")
        config["indices"] = new_indices
        with open(file_path, "w") as fp:
            json.dump(config, fp, indent="\t")

    print("Mismatched region names per map (regionname_in_map, regionname_in_tree):")
    pprint(mismatched)
    print("\nChanged the names in the maps to the ones in the region tree.")


if __name__ == "__main__":
    main()
