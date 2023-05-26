import os, json

def change_key(obj):
    assert obj.get("@type") == "siibra/volume/v0.0.1"
    obj['providers'] = obj.pop("urls")
    

def process_obj(obj):
    if isinstance(obj, dict):
        if obj.get("@type") == "siibra/volume/v0.0.1":
            change_key(obj)
        for key in obj:
            process_obj(obj[key])
    if isinstance(obj, list):
        for item in obj:
            process_obj(item)

def main():
    for dirpath, dirnames, filenames in os.walk("."):
        for filename in filenames:
            if not filename.endswith(".json"):
                continue
            full_filename = f"{dirpath}/{filename}"
            with open(full_filename, "r") as fp:
                json_obj = json.load(fp)
            process_obj(json_obj)
            with open(full_filename, "w") as fp:
                json.dump(json_obj, fp=fp, indent="\t")
                fp.write("\n")


if __name__ == "__main__":
    main()