import os
import json
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import requests
from collections import defaultdict

PATH_TO_MAPS = "./maps"

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

def check_precompsrc(precomp_url):
    resp = requests.get(f"{precomp_url}/info")
    resp_json = resp.json()
    return precomp_url, "mesh" in resp_json

def main():

    full_filenames_to_json = {}
    precomp_url_to_volume_dict = defaultdict(list)
    list_of_volumes = [(full_filename, map_json, vol_idx, volume) 
        for full_filename, _, map_json in iterate_jsons(PATH_TO_MAPS)
        for vol_idx, volume in enumerate(map_json.get("volumes", []))
        if "neuroglancer/precomputed" in volume.get('providers')]

    for full_filename, map_json, vol_idx, volume in list_of_volumes:
        precomp_url = volume["providers"]["neuroglancer/precomputed"]
        precomp_url_to_volume_dict[precomp_url].append(volume)
        full_filenames_to_json[full_filename] = map_json
    
    precomp_urls = [
        volume["providers"]["neuroglancer/precomputed"]
        for full_filename, map_json, vol_idx, volume in list_of_volumes
    ]

    with ThreadPoolExecutor(max_workers=16) as ex:
        result = list(
            tqdm(
                ex.map(check_precompsrc, precomp_urls),
                total=len(precomp_urls),
                unit="precomp src",
            )
        )

    for precomp_url, has_mesh in result:
        if has_mesh:
            for volume in precomp_url_to_volume_dict[precomp_url]:
                providers = volume.get("providers")
                providers["neuroglancer/precompmesh"] = providers["neuroglancer/precomputed"]
    
    for full_filename, parc_json in full_filenames_to_json.items():
        with open(full_filename, "w") as fp:
            json.dump(parc_json, indent="\t", fp=fp)
            fp.write("\n")
    

if __name__ == "__main__":
    main()