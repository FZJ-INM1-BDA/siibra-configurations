import os
import json
from concurrent.futures import ThreadPoolExecutor
import requests
from enum import Enum
from typing import List

PATH_TO_MAPS = "./maps"
MAX_WORKERS = 4

class ValidationResult(Enum):
    PASSED="PASSED"
    FAILED="FAILED"
    SKIPPED="SKIPPED"

PRECOMPMESH = "neuroglancer/precompmesh"
PRECOMPUTED = "neuroglancer/precomputed"
IMAGE_TYPE = "siibra/attr/data/image/v0.1"

def check_url(url: str):
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        assert "fragments" in resp.json(), f"fragment key not found for {url}"
        return (
            url,
            ValidationResult.PASSED,
            None
        )
    except Exception as e:
        return (
            url,
            ValidationResult.FAILED,
            str(e)
        )

def check_ng_precomp_volume(url: str, indices: List[int]):

    resp = requests.get(f"{url}/info")
    resp.raise_for_status()
    precomp_info = resp.json()
    assert "mesh" in precomp_info, f"mesh key does not exist in precompmesh"

    mesh_path = precomp_info["mesh"]

    urls_to_check = [f"{url}/{mesh_path}/{idx}:0" for idx in indices]
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        return [
            (result, err)
            for url, result, err in ex.map(
                check_url,
                indices
            )
        ]

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


def main():
    failed = []
    skipped = []
    attrs = [
        (full_filname, attr_idx, attr, 
        [
            (regionname, mapping.get("label"))
            for regionname, mapping in attr.get("mappin", {}).items()
        ])
        for full_filname, _, map_json in iterate_jsons()
        for attr_idx, attr in enumerate(map_json.get("attributes", []))
        if (
            attr.get("@type") == IMAGE_TYPE
            and attr.get("format") == PRECOMPMESH
        )
    ]

    filtered_attrs = []
    for full_filname, attr_idx, attr, list_t_regionname_label  in attrs:
        if any(label is None for regionname, label in list_t_regionname_label):
            failed.append(
                (ValidationResult.FAILED, f"{full_filname} validation failed. attibute at index {attr_idx}, some mapping does not have label key")
            )
            continue
        filtered_attrs.append(
            (full_filname, attr_idx, attr, list_t_regionname_label)
        )
    urls = [attr.get("url") for full_filname, attr_idx, attr, list_t_regionname_label in filtered_attrs]
    indices = [[label for regionname, label in list_t_regionname_label]
               for full_filname, attr_idx, attr, list_t_regionname_label in filtered_attrs]

    print(f"Main: {len(attrs)} maps.")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        result = [v for ll in list(ex.map(check_ng_precomp_volume, urls, indices))
                  for v in ll]

    passed = [(r, text) for r, text in result if r == ValidationResult.PASSED]
    failed += [(r, text) for r, text in result if r == ValidationResult.FAILED]
    skipped += [(r, text) for r, text in result if r == ValidationResult.SKIPPED]

    print(f"PASSED: {len(passed)}, FAILED: {len(failed)}, SKIPPED: {len(skipped)}, TOTAL: {len(attrs)} {len(result)}")
    with open("./missing.txt", "w") as fp:
        fp.write("\n".join([text for f, text in failed]))
        fp.write("\n")
    assert len(failed) == 0, "\n".join([text for f, text in failed])
if __name__ == "__main__":
    main()
