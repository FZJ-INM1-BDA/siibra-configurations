import os
import json

PATH_TO_PARC = "./parcellations"

def process_region(region):
    rgb = region.pop("rgb", None)
    if rgb:
        rgb_string = '#' + ''.join('{:02X}'.format(v) for v in rgb)
        region['rgb'] = rgb_string

    children = region.get("children", [])
    for child in children:
        process_region(child)

id_to_path = {}
def populate_dict():
    for dirpath, dirnames, filenames in os.walk(PATH_TO_PARC):
        for filename in filenames:
            if not filename.endswith(".json"):
                continue
            with open(f"{dirpath}/{filename}", "r") as fp:
                parc = json.load(fp)
            id_to_path[parc.get("@id")] = f"./{filename}"

def main():
    populate_dict()
    for dirpath, dirnames, filenames in os.walk(PATH_TO_PARC):
        for filename in filenames:
            if not filename.endswith(".json"):
                continue
            full_filename = f"{dirpath}/{filename}"
            with open(full_filename, "r") as fp:
                parcellation_json = json.load(fp)
            regions = parcellation_json.get("regions", [])
            for r in regions:
                process_region(r)
            
            version = parcellation_json.get("@version", None)
            if version:
                prev = version.get("@prev", None)
                if prev:
                    assert prev in id_to_path
                    version['@prev'] = id_to_path[prev]

            with open(full_filename, "w") as fp:
                json.dump(parcellation_json, indent="\t", fp=fp)
                fp.write("\n")


if __name__ == "__main__":
    main()