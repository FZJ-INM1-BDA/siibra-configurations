from pathlib import Path
import json
from dataclasses import dataclass


@dataclass
class DuplicatedErr:
    filename: str
    key: str

    def __eq__(self, other: "DuplicatedErr"):
        return self.filename == other.filename and self.key == other.key


class DuplicatedRegionName(Exception):
    pass


FAIL_FAST = False

# There is no solution for the following
XFAIL_LIST = [
    DuplicatedErr(filename="parcellations/rat_waxholmv1_01.json", key="corticofugal pathways"),
    DuplicatedErr(filename="parcellations/rat_waxholmv1_01.json", key="medial lemniscus"),
    DuplicatedErr(filename="parcellations/rat_waxholmv1_01.json", key="facial nerve"),
    DuplicatedErr(filename="parcellations/rat_waxholmv1_01.json", key="spinal cord"),
    DuplicatedErr(filename="parcellations/rat_waxholmv2.json", key="corticofugal pathways"),
    DuplicatedErr(filename="parcellations/rat_waxholmv2.json", key="medial lemniscus"),
    DuplicatedErr(filename="parcellations/rat_waxholmv2.json", key="facial nerve"),
    DuplicatedErr(filename="parcellations/rat_waxholmv2.json", key="spinal cord"),
    DuplicatedErr(filename="parcellations/rat_waxholmv3.json", key="anterior commissure"),
    DuplicatedErr(filename="parcellations/rat_waxholmv3.json", key="medial lemniscus"),
    DuplicatedErr(filename="parcellations/rat_waxholmv3.json", key="facial nerve"),
    DuplicatedErr(filename="parcellations/rat_waxholmv3.json", key="lateral lemniscus"),
    DuplicatedErr(filename="parcellations/rat_waxholmv3.json", key="neocortex"),
    DuplicatedErr(filename="parcellations/rat_waxholmv3.json", key="thalamus"),
    DuplicatedErr(filename="parcellations/rat_waxholmv3.json", key="brainstem"),
    DuplicatedErr(filename="parcellations/rat_waxholmv3.json", key="ventricular system"),
    DuplicatedErr(filename="parcellations/rat_waxholmv3.json", key="spinal cord"),
]


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
        with open(_file, "r", encoding="utf-8") as fp:
            parc_json = json.load(fp)
            all_regions = [
                rc for r in parc_json.get("regions") for rc in iterate_over_children(r)
            ]
            for reg in all_regions:
                reg_name = reg.get("name")
                if reg_name.lower() in reg_name_set:
                    error = DuplicatedErr(filename=_file.as_posix(), key=reg_name)
                    if error in XFAIL_LIST:
                        print(f"xfail: {error}")
                        continue
                    errors.append(error)
                    if FAIL_FAST:
                        raise DuplicatedRegionName(
                            "\n".join([str(err) for err in errors])
                        )
                reg_name_set.add(reg_name.lower())

    if len(errors) > 0:
        raise DuplicatedRegionName("\n".join([str(err) for err in errors]))


if __name__ == "__main__":
    main()
