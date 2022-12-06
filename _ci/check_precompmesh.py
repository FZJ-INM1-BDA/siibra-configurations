import os
import json
from concurrent.futures import ThreadPoolExecutor
import requests
from enum import Enum

PATH_TO_MAPS = "./maps"
MAX_WORKERS = 4

class ValidationResult(Enum):
    PASSED="PASSED"
    FAILED="FAILED"
    SKIPPED="SKIPPED"

PRECOMPMESH = "neuroglancer/precompmesh"
PRECOMPUTED = "neuroglancer/precomputed"

def check_url(url: str, regionname: str):
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        assert "fragments" in resp.json()
        return (
            url, regionname,
            ValidationResult.PASSED,
            None
        )
    except Exception as e:
        return (
            url, regionname,
            ValidationResult.FAILED,
            str(e)
        )

def check_volume(arg):
    full_filname, vol_idx, volume, indices = arg
    try:
        assert PRECOMPUTED in volume.get("urls"), f"volume should have neuroglancer/precompmesh, but does not. url keys are: {volume.get('urls').keys()}"
        precomp_url = volume["urls"][PRECOMPUTED]
        resp = requests.get(f"{precomp_url}/info")
        resp.raise_for_status()
        precomp_info = resp.json()
        assert ("mesh" in precomp_info) == (PRECOMPMESH in volume.get("urls")), f"Error in: {full_filname}: mesh key exist in precomputed: {'mesh' in precomp_info}, precomputed mesh url exists: {PRECOMPMESH in volume.get('urls')}"

        if "mesh" in precomp_info:
            mesh_path = precomp_info["mesh"]
            regions_to_check = [(region, mapped_index.get("label"))
                for region, mapped_indicies in indices.items()
                for mapped_index in mapped_indicies
                if mapped_index.get("volume") == vol_idx]
            
            print(f"Checking {precomp_url} ... {len(regions_to_check)} labels.")
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
                indicies_result = ex.map(
                    check_url,
                    [f"{precomp_url}/{mesh_path}/{item[1]}:0" for item in regions_to_check],
                    [item[0] for item in regions_to_check]
                )
            failed = [result for result in indicies_result if result[-2] == ValidationResult.FAILED]
            assert len(failed) == 0, f"""region indices mapping failed, {', '.join([f[-1] for f in failed])}"""
        return (
            full_filname, vol_idx, volume, indices,
            ValidationResult.PASSED,
            None
        )
    except Exception as e:
        return (
            full_filname, vol_idx, volume, indices,
            ValidationResult.FAILED,
            str(e)
        )

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
    args = [
        (full_filname, vol_idx, volume, map_json.get("indices") )
        for full_filname, _, map_json in iterate_jsons(PATH_TO_MAPS)
        for vol_idx, volume in enumerate(map_json.get("volumes"))
        if PRECOMPUTED in volume.get("urls") or PRECOMPMESH in volume.get("urls")
    ]

    print(f"Main: {len(args)} maps.")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        result = list(ex.map(check_volume, args))

    passed = [r for r in result if r[-2] == ValidationResult.PASSED]
    failed = [r for r in result if r[-2] == ValidationResult.FAILED]
    skipped = [r for r in result if r[-2] == ValidationResult.SKIPPED]

    print(f"PASSED: {len(passed)}, FAILED: {len(failed)}, SKIPPED: {len(skipped)}, TOTAL: {len(args)} {len(result)}")
    with open("./missing.txt", "w") as fp:
        fp.write("\n".join([f[-1] for f in failed]))
        fp.write("\n")
    assert len(failed) == 0, "\n".join([f[-1] for f in failed])
if __name__ == "__main__":
    main()
