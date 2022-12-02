import json
import os
import requests

primitive_types = (
    str,
    int,
    float,
    bool,
)

volume_types = (
    "neuroglancer/precomputed",
    "neuroglancer/precompmesh"
)

ignore_urls = (
    "https://object.cscs.ch/v1/AUTH_08c08f9f119744cbbf77e216988da3eb/imgsvc-97d7cf5f-9bb7-4143-8b1e-757ab4cb7354",
    "https://object.cscs.ch/v1/AUTH_08c08f9f119744cbbf77e216988da3eb/imgsvc-d37f81c3-0449-4ace-83dc-e4d21a045142",
    "https://object.cscs.ch/v1/AUTH_08c08f9f119744cbbf77e216988da3eb/imgsvc-ae7dc2f2-8135-4a64-8c3b-e28008ec9d72",
    "https://object.cscs.ch/v1/AUTH_08c08f9f119744cbbf77e216988da3eb/imgsvc-3ac156f3-ba17-4253-8d76-fa78d6958081",
    "https://object.cscs.ch/v1/AUTH_08c08f9f119744cbbf77e216988da3eb/imgsvc-9b021cdc-8c27-4b5a-ab24-6561ce22d1fc",
)

threshold = 10

dry_run = False

def transform_equal(mat0, mat1):
    assert all([
        abs(mat0[row][col] - mat1[row][col]) < threshold
        for row in range(4) for col in range(4)])

def check_volume(volume):
    detail = volume.get("detail")
    urls = volume.get("urls")

    for volume_type in volume_types:
        if volume_type not in urls:
            assert detail is None or volume_type not in detail, f"volume type {volume_type} not in urls {urls}, but in detail"
            continue
        
        if urls[volume_type] in ignore_urls:
            continue

        assert detail and volume_type in detail, f"volume_type: {volume_type}, obj: {volume} url: {urls[volume_type]} has no corresponding transform"
        if not dry_run:
            detail[volume_type].pop("transform")
            detail[volume_type].pop("labelIndex", None)
            if len(detail[volume_type]) == 0:
                detail.pop(volume_type)
            if len(detail) == 0:
                volume.pop("detail")
            return

        assert "transform" in detail.get(volume_type, {})
        resp = requests.get(f"{urls[volume_type]}/transform.json")
        resp.raise_for_status()
        server_transform = resp.json()

        try:
            transform_equal(server_transform, detail[volume_type].get("transform"))
        except AssertionError:
            print(f"Incorrect transform: {urls[volume_type]} {server_transform} compared to {detail[volume_type].get('transform')}")

def parse_old_volumes(volumes: list):
    def cvt_old_volume(vol):
        assert "url" in vol
        assert "detail" in vol
        assert vol.get("volume_type") == "neuroglancer/precomputed"

        return {
            "@type": "siibra/volume/v0.0.1",
            "urls": {
                "neuroglancer/precomputed": vol['url']
            },
            "detail": vol.get("detail")
        }
    volume_copy = []
    while len(volumes) > 0:
        volume_copy.append(volumes.pop(0))
    for vol in volume_copy:
        volumes.append(cvt_old_volume(vol))
    for vol in volumes:
        check_volume(vol)


def parse_obj(obj):
    if isinstance(obj, dict):
        _type = obj.get("@type", None)
        if _type == "siibra/volume/v0.0.1":
            # bingo
            check_volume(obj)
            return
        for key, value in obj.items():
            parse_obj(value)
        return
    if isinstance(obj, list):
        if all([item.get("@type") == "fzj/tmp/volume_type/v0.0.1" for item in obj]):
            parse_old_volumes(obj)
            return
        for item in obj:
            parse_obj(item)
        return
    if any([isinstance(obj, pt) for pt in primitive_types]):
        return
    
    import pdb
    pdb.set_trace()

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
    for full_filename, text_content, loaded_json, in iterate_jsons("."):
        if "neuroglancer/precomputed" in text_content or "neuroglancer/precompmesh" in text_content:
            parse_obj(loaded_json)
            if not dry_run:
                with open(full_filename, "w") as fp:
                    json.dump(loaded_json, indent="\t", fp=fp)
                    fp.write("\n")
        pass

if __name__ == "__main__":
    main()
