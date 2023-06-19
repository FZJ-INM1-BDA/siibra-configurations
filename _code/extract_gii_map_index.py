import requests
from xml.etree import ElementTree as ET
import json

rh_url = "https://object.cscs.ch/v1/AUTH_7e4157014a3d4c1f8ffe270b57008fd4/d-7ad727a1-537d-4f80-a69b-ac8b184a823c/fsaverage_surface/rh.JulichBrainAtlas_3.0.2.label.gii"
lh_url = "https://object.cscs.ch/v1/AUTH_7e4157014a3d4c1f8ffe270b57008fd4/d-7ad727a1-537d-4f80-a69b-ac8b184a823c/fsaverage_surface/lh.JulichBrainAtlas_3.0.2.label.gii"

path_to_map = "maps/fsaverage-jba30-labelled.json"

def main():
    indices = {}
    for hem in [lh_url, rh_url]:
        fragment = "left hemisphere" if hem == lh_url else "right hemisphere"
        resp = requests.get(hem)
        resp.raise_for_status()
        tree = ET.ElementTree(ET.fromstring(resp.text))
        labels = tree.findall(".//LabelTable/*")
        for label in labels:
            if label.get("Key") == "0":
                continue
            print(label.get("Key"), label.text)
            indices = {
                **indices,
                f"{label.text} - {fragment}": [{
                    "volume": 0,
                    "label": int(label.get("Key")),
                    "fragment": fragment
                }]
            }

    with open(path_to_map, "r") as fp:
        fsaverage_map = json.load(fp)
        fsaverage_map["indices"] = indices
    with open(path_to_map, "w") as fp:
        json.dump(fsaverage_map, indent="\t", fp=fp)
        fp.write("\n")
        

if __name__ == "__main__":
    main()
