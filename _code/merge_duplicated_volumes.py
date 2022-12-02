import os
import json
from collections import defaultdict
from typing import Dict
from itertools import combinations

PATH_TO_MAPS = "./maps"

def volume_equal(idx_reg_mapping_0: Dict[int, str], idx_reg_mapping_1: Dict[int, str]):
    
    try:
        assert set(idx_reg_mapping_0.keys()) == set(idx_reg_mapping_1.keys())
        for key in idx_reg_mapping_0:
            assert idx_reg_mapping_0[key] == idx_reg_mapping_1[key]
        return True
    except AssertionError:
        return False

def main():
    for dirpath, dirnames, filenames in os.walk(PATH_TO_MAPS):
        for filename in filenames:
            full_filename = f"{dirpath}/{filename}"
            with open(full_filename, "r") as fp:
                parc_json = json.load(fp)
            volumes = parc_json.get("volumes")
            if len(volumes) == 1:
                continue
            indices = parc_json.get("indices")
            volidx_label_region = defaultdict(dict)
            for regionname, mappings in indices.items():
                for mapping in mappings:
                    volume_idx = mapping.get("volume", None)
                    label_idx = mapping.get("label", None)
                    if volume_idx is not None and label_idx is not None:
                        try:
                            assert label_idx not in volidx_label_region[volume_idx]
                            volidx_label_region[volume_idx][label_idx] = regionname
                        except AssertionError:
                            import pdb
                            pdb.set_trace()
            
            volume_alias: Dict[int, int] = {}
            for keyval0, keyval1 in combinations(volidx_label_region.items(), 2):
                vol_idx0, dict0 = keyval0
                vol_idx1, dict1 = keyval1
                if volume_equal(dict0, dict1):
                    src_vol_idx = max(vol_idx0, vol_idx1)
                    dst_vol_idx = min(vol_idx0, vol_idx1)
                    # assuming only 1 duplicated volume
                    assert dst_vol_idx not in volume_alias
                    volume_alias[dst_vol_idx] = src_vol_idx
            if len(volume_alias) > 0:
                assert len(volume_alias) == 1
                for dst_vol_idx in volume_alias:
                    src_vol_idx = volume_alias[dst_vol_idx]
                    removed_volume = volumes.pop(src_vol_idx)

                    detail = removed_volume.get("detail", {})
                    urls = removed_volume.get("urls")

                    append_target = volumes[dst_vol_idx]
                    if len(detail) > 0:
                        for key in detail:
                            if "detail" not in append_target:
                                append_target["detail"] = {}
                            append_target["detail"][key] = detail[key]
                    for key in urls:
                        assert key not in append_target['urls']
                        append_target['urls'][key] = urls[key]
                    for regionname in indices:
                        tmp = [mapping
                            for mapping in 
                            indices[regionname]
                            if mapping.get("volume") != src_vol_idx]
                        indices[regionname] = tmp

            with open(full_filename, "w") as fp:
                json.dump(parc_json, indent="\t", fp=fp)
                fp.write("\n")

if __name__ == "__main__":
    main()