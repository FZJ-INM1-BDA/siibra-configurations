from pathlib import Path
import json
from dataclasses import dataclass


@dataclass
class DuplicatedErr:
    filename: str
    key: str


class DuplicatedRegionName(Exception):
    pass


FAIL_FAST = False


def iterate_over_children(r):
    yield r
    for c in r.get("children", []):
        for cc in iterate_over_children(c):
            yield cc


def main():
    errors = []
    _dir = Path("parcellations")
    for _file in _dir.glob("*.json"):
        reg_name_set = set()
        with open(_file, "r") as fp:
            parc_json = json.load(fp)
            all_regions = [
                rc for r in parc_json.get("regions") for rc in iterate_over_children(r)
            ]
            for reg in all_regions:
                reg_name = reg.get("name")
                if reg_name in reg_name_set:
                    errors.append(DuplicatedErr(filename=str(_file), key=reg_name))
                    if FAIL_FAST:
                        raise DuplicatedRegionName(
                            "\n".join([str(err) for err in errors])
                        )
                reg_name_set.add(reg_name)

    if len(errors) > 0:
        raise DuplicatedRegionName("\n".join([str(err) for err in errors]))


if __name__ == "__main__":
    main()
