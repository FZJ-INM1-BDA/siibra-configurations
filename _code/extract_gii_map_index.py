import requests
from xml.etree import ElementTree as ET
import json
from pathlib import Path

rh_url = "/home/xgui3783/dev/projects/brainscapes-configurations/rh.JulichBrainAtlas_3.0.2.label.gii" or "https://object.cscs.ch/v1/AUTH_7e4157014a3d4c1f8ffe270b57008fd4/d-7ad727a1-537d-4f80-a69b-ac8b184a823c/fsaverage_surface/rh.JulichBrainAtlas_3.0.2.label.gii"
lh_url = "/home/xgui3783/dev/projects/brainscapes-configurations/lh.JulichBrainAtlas_3.0.2.label.gii" or "https://object.cscs.ch/v1/AUTH_7e4157014a3d4c1f8ffe270b57008fd4/d-7ad727a1-537d-4f80-a69b-ac8b184a823c/fsaverage_surface/lh.JulichBrainAtlas_3.0.2.label.gii"

path_to_map = "maps/fsaverage-jba30-labelled.json"
path_to_parcellation = "parcellations/julichbrain_v3_0_2.json"

region_name_mapping = {
}
    
def get_mapped_name(name: str):
    if name.startswith("GapMap"):
        return name.replace("GapMap-", "", 1) + " (GapMap)"
    return region_name_mapping.get(name, name)

def get_regions(r):
    
    return [
        r,
        *[cc
          for c in r.get("children", [])
          for cc in get_regions(c)]
    ]
    

def main():
    indices = {}
    for hem in [lh_url, rh_url]:
        fragment = "left hemisphere" if hem == lh_url else "right hemisphere"
        if Path(hem).is_file():
            with open(Path(hem), "r") as fp:
                content = fp.read()
        else:
            resp = requests.get(hem)
            resp.raise_for_status()
            content=resp.text
        tree = ET.ElementTree(ET.fromstring(content))
        labels = tree.findall(".//LabelTable/*")
        for label in labels:
            if label.get("Key") == "0":
                continue
            print(label.get("Key"), label.text)
            region_name = label.text.strip()
            region_name = get_mapped_name(region_name)
            indices = {
                **indices,
                f"{region_name} - {fragment}": [{
                    "volume": 0,
                    "label": int(label.get("Key")),
                    "fragment": fragment
                }]
            }

    exceptions = []
    name_set = set()
    with open(path_to_parcellation, "r") as fp:
        all_regions = [c 
                       for r in json.load(fp=fp).get("regions")
                       for c in get_regions(r)]
        for r in all_regions:
            name_set.add(r.get("name"))
    
    with open(path_to_map, "r") as fp:
        fsaverage_map = json.load(fp)
        for regionname in indices:
            if regionname not in name_set:
                exceptions.append(f"region with name {regionname}  is not in the new indices... please add it to region_name mapping")
        fsaverage_map["indices"] = indices

    with open(path_to_map, "w") as fp:
        json.dump(fsaverage_map, indent="\t", fp=fp)
        fp.write("\n")
        

if __name__ == "__main__":
    main()
