from pathlib import Path
import json
import requests

root = Path("_maps")

def main():
    include = []
    print_result = {
        "include": include
    }
    
    for f in root.glob("*.json"):
        map_obj = json.loads(f.read_text())
        parcellation_id = map_obj.get("parcellation_id")
        space_id = map_obj.get("space_id")
        name_attrs = [attr
                      for attr in map_obj.get("attributes", [])
                      if attr.get("@type") == "siibra/attr/desc/name/v0.1"]
        assert len(name_attrs) == 1, f"{str(f)} has more than 1 name attr"
        name = name_attrs[0]["value"]
        for attr in map_obj.get("attributes", []):
            if attr.get("format") == "sparseindex":
                url = attr.get("url")
                try:
                    resp = requests.get(url)
                    resp.raise_for_status()
                except requests.HTTPError:
                    filename = Path(attr.get("url"))
                    include.append({
                        "parcellation_id": parcellation_id,
                        "space_id": space_id,
                        "filename": filename.name,
                        "mapname": name,
                    })
                    print(json.dumps(print_result))
                    return
    print(print_result)
    return

if __name__ == "__main__":
    main()
