import os
import json
from enum import Enum

DIR_TO_MAPS = "./maps"

class ValidationResult(Enum):
    SUCCESS="SUCCESS"
    FAILURE="FAILURE"
    SKIPPED="SKIPPED"

def main():
    failures = []
    for dirpath, dirnames, filenames in os.walk(DIR_TO_MAPS):
        for filename in filenames:
            full_filename = f"{dirpath}/{filename}"
            with open(full_filename, "r") as fp:
                map_json = json.load(fp)
            indices = map_json.get("indices")
            volumes = map_json.get("volumes")
            len_volumes = len(volumes)
            for regionname, mapped_indicies in indices.items():
                for mapped_index in mapped_indicies:
                    volume_idx = mapped_index.get("volume")
                    if volume_idx >= len_volumes:
                        failures.append((ValidationResult.FAILURE, f"In {full_filename}, {regionname} has volume idx {volume_idx}, but volume has len of {len_volumes}"))
    assert len(failures) == 0, "\n" + "\n".join(message for reason, message in failures)
if __name__ == "__main__":
    main()