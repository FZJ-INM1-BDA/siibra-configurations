import os
import json
from enum import Enum

DIR_TO_MAPS = "./maps"
DIR_TO_SPACES = "./spaces"

class ValidationResult(Enum):
    SUCCESS="SUCCESS"
    FAILURE="FAILURE"
    SKIPPED="SKIPPED"

def main():
    failures = []
    space_volumes = {}

    for dirpath, dirnames, filenames in os.walk(DIR_TO_SPACES):
        for filename in filenames:
            if not filename.endswith(".json"):
                continue
            with open(f"{dirpath}/{filename}", "r") as fp:
                space_json = json.load(fp)
            assert space_json.get("@id")
            assert len(space_json.get("volumes")) >= 1
            space_volumes[
                space_json.get("@id")
            ] = space_json.get("volumes")

    for dirpath, dirnames, filenames in os.walk(DIR_TO_MAPS):
        for filename in filenames:
            full_filename = f"{dirpath}/{filename}"
            error_message_0 = f"In {full_filename},"
            with open(full_filename, "r") as fp:
                map_json = json.load(fp)
            indices = map_json.get("indices")
            volumes = map_json.get("volumes")
            space_id = map_json.get("space", {}).get("@id")
            
            if space_id is None:
                failures.append( (ValidationResult.FAILURE, f"{error_message_0}, space.@id is not defined") )

            if space_id is not None:
                if space_id not in space_volumes:
                    failures.append( (ValidationResult.FAILURE, f"{error_message_0}, space.@id does not point to a valid space") )

            len_volumes = len(volumes)
            for regionname, mapped_indicies in indices.items():
                error_message_1 = f"{error_message_0}{regionname}"
                for mapped_index in mapped_indicies:
                    volume_idx = mapped_index.get("volume")
                    fragment = mapped_index.get("fragment")
                    if volume_idx is None:
                        if len(volumes) != 1:
                            failures.append((ValidationResult.FAILURE, f"{error_message_1} has volume set to None, expecting only 1 volume, but got {len(volumes)}"))
                    if volume_idx is not None and volume_idx >= len_volumes:
                        failures.append((ValidationResult.FAILURE, f"{error_message_1} has volume idx '{volume_idx}', but volume has len of '{len_volumes}'"))
                    
                    volume_of_interest = volumes[volume_idx or 0]
                    if fragment is not None:
                        
                        # tolerate if parcellation all parcellation volume has neuroglancer/precompmesh
                        # see https://chat.fz-juelich.de/direct/vKmCEz3F6xFaGso8a
                        # search for "some inconsistencies in configuration repo when I was trying to fix the ci"
                        # on 2023-01-03
                        
                        if "neuroglancer/precompmesh" in volume_of_interest.get("providers"):
                            continue

                        if any(
                            fragment not in provided_value
                            for provider, provided_value in volume_of_interest.get("providers").items()
                        ):
                            failures.append(( ValidationResult.FAILURE, f"{error_message_1} has fragment defined as '{fragment}', but non exists for the corresponding volume." ))
                        if space_id is not None:
                            if any(
                                (
                                    provider == "gii-mesh" and
                                    fragment not in provided_value
                                )
                                for volume in space_volumes[space_id]
                                for provider, provided_value in volume.get("providers").items()
                            ):
                                failures.append( (ValidationResult.FAILURE, f"{error_message_1} has frament defined as '{fragment}', but non exist for the corresponding space volume.") )

    assert len(failures) == 0, "\n" + "\n".join(message for reason, message in failures)
if __name__ == "__main__":
    main()
