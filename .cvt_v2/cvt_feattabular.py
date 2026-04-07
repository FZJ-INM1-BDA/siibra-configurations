from pathlib import Path
import json
from hashlib import md5

MODALITY_KEY = "siibra/attr/desc/modality/v0.1"

modality_map = {
    "siibra/feature/profile/celldensity/v0.1": "segmented cell body density",
    "siibra/feature/profile/receptor/v0.1": "neurotransmitter receptor profile",
    "siibra/feature/fingerprint/receptor/v0.1": "neurotransmitter receptor density",
    "siibra/feature/fingerprint/celldensity/v0.1": "cell body density",
}


def process_region(region_str: str | None = None, parcellation_id: str | None = None):
    if region_str is None and parcellation_id is None:
        return []
    spec = {
        "key": "siibra/region/spec/v0.1",
    }
    if region_str:
        spec["name"] = region_str
    if parcellation_id:
        spec["parcellation_id"] = parcellation_id

    return [
        {
            "schema": "siibra/attr/desc/resolvable/v0.1",
            "spec": spec,
        }
    ]


def process_location(loc: dict | None):
    if loc is None:
        return []
    _type = loc.pop("@type")
    if _type == "siibra/location/point/v0.1":
        coordinate = loc.pop("coordinate")
        space_id = loc.pop("space").pop("@id")
        returnval = [
            {
                "schema": "siibra/attr/loc/pt/v0.1",
                "coordinate": coordinate,
                "space_id": space_id,
            }
        ]
        assert loc == {}
        return returnval
    if _type == "siibra/location/pointcloud/v0.1":

        coordinates = loc.pop("coordinates")
        space_id = loc.pop("space").pop("@id")
        returnval = [
            {
                "schema": "siibra/attr/loc/ptcloud/v0.1",
                "coordinates": coordinates,
                "sigmas": [0] * len(coordinates),
                "space_id": space_id,
            }
        ]
        assert loc == {}
        return returnval
    raise Exception(f"{_type=}")


def process_ebrains(loc: dict):
    # TODO NYI
    return []


def process_species(species: str | None):
    if species is None:
        return []
    # TODO NYI
    return []


def process_file(file: str | None, _type: str):
    if file is None:
        return []
    if _type in (
        "siibra/feature/fingerprint/receptor/v0.1",
        "siibra/feature/profile/receptor/v0.1",
    ):
        assert file.endswith(".tsv")
        dr = {
            "schema": "siibra/attr/data/v0.1",
            "origin": file,
            "steps": [{"key": "file"}, {"key": "readByteAsDf", "sep": "\t"}],
            "list_labels": [],
        }
        return [dr]
    if _type == "siibra/feature/profile/celldensity/v0.1":
        assert file.endswith(".txt")
        dr = {
            "schema": "siibra/attr/data/v0.1",
            "origin": file,
            "steps": [
                {"key": "file", "start": 2},
                {"key": "readByteAsDf", "sep": " ", "header": 0},
                {"key": "dfAsType", "dtype": {"layer": "int", "label": "int"}},
            ],
            "list_labels": [],
        }
        return [dr]
    raise Exception(f"{_type}")


def process_layerwise_cell_density(segmentfiles: list[str], layerfiles: list[str]):
    assert len(segmentfiles) == len(layerfiles)
    assert len(segmentfiles) > 0
    assert all(
        sf.removesuffix("/segments.txt") == lf.removesuffix("/layerinfo.txt")
        for sf, lf in zip(segmentfiles, layerfiles)
    )
    stems = [sf.removesuffix("/segments.txt") for sf in segmentfiles]
    commonstems = set(s.rsplit("/", maxsplit=2)[0] for s in stems)
    assert len(commonstems) == 1

    commonstem = list(commonstems)[0]

    segsteps = [
        [
            {"key": "appendStr", "value": sf.removeprefix(commonstem)},
            {"key": "file", "start": 2},
            {"key": "readByteAsDf", "sep": " ", "header": 0},
            {"key": "dfAsType", "dtype": {"layer": "int", "label": "int"}},
        ]
        for sf in segmentfiles
    ]

    layersteps = [
        [
            {"key": "appendStr", "value": lf.removeprefix(commonstem)},
            {"key": "file", "start": 2},
            {"key": "readByteAsDf", "sep": " ", "header": 0, "index_col": 0},
        ]
        for lf in layerfiles
    ]
    contribstep = {"key": "contrib:layerWiseCellDensity"}
    dr = {
        "schema": "siibra/attr/data/v0.1",
        "origin": list(commonstems)[0],
        "steps": [
            {"key": "fork", "list_of_specs": [*segsteps, *layersteps]},
            contribstep,
        ],
        "list_labels": [],
    }
    return [dr]


def process_timeseries(repo: dict, decoder: dict, files: dict, regions: list):
    _repo_type = repo.pop("@type")
    assert _repo_type == "siibra/repository/zippedfile/v1.0.0"
    _repo_url = repo.pop("url")
    assert repo == {}

    sep = decoder.pop("delimiter", None) or decoder.pop("sep")
    header = decoder.pop("header")
    index_col = decoder.pop("index_col")
    decoder.pop("engine")
    decoder.pop("@type") == "siibra/decoder/csv"
    assert decoder == {}

    assert sep == "  "

    drs = []
    for sub, fname in files.items():
        drs.append(
            {
                "schema": "siibra/attr/data/v0.1",
                "origin": _repo_url,
                "list_labels": [{"subject": sub}],
                "steps": [
                    {"key": "zipGetFile", "filename": fname},
                    {
                        "key": "readByteAsDf",
                        "sep": sep,
                        "header": header,
                        "engine": "python",
                        "index_col": index_col,
                    },
                ],
            }
        )
    return drs


def cvt_tf(_dict: dict):
    # some feature has no id
    _id = _dict.pop("@id", None) or md5(json.dumps(_dict).encode()).hexdigest()
    _type = _dict.pop("@type")

    attributes = []
    datarecipes = []

    parcellation = _dict.pop("parcellation", {})

    # metadata keys
    modality = modality_map.get(_type) or _dict.pop(
        "modality"
    )  # some feature do not have modality defined in json
    attributes.append({"schema": MODALITY_KEY, "value": modality})
    attributes.extend(
        process_region(_dict.pop("region", None), parcellation.get("@id"))
    )
    attributes.extend(process_location(_dict.pop("location", None)))
    attributes.extend(process_ebrains(_dict.pop("ebrains", None)))
    attributes.extend(process_species(_dict.pop("species", None)))

    # highly specialized fields
    cohort = _dict.pop("cohort", None)
    paradigm = _dict.pop("paradigm", None)
    unit = _dict.pop("unit", None)
    patch = _dict.pop("patch", None)
    section = _dict.pop("section", None)
    repository = _dict.pop("repository", None)

    prerelease = _dict.pop("prerelease", None)

    decoder = _dict.pop("decoder", None)
    regions = _dict.pop("regions", None)

    file = _dict.pop("file", None)

    receptor = _dict.pop("receptor", None)
    if file:
        assert _type in (
            "siibra/feature/profile/celldensity/v0.1",
            "siibra/feature/profile/receptor/v0.1",
            "siibra/feature/fingerprint/receptor/v0.1",
        )

        datarecipes.extend(process_file(file, _type))

    if _type == "siibra/feature/fingerprint/celldensity/v0.1":
        segmentfiles = _dict.pop("segmentfiles")
        layerfiles = _dict.pop("layerfiles")
        datarecipes.extend(process_layerwise_cell_density(segmentfiles, layerfiles))

    if _type == "siibra/feature/timeseries/activity/v0.1":
        timestep = _dict.pop("timestep")
        files = _dict.pop("files")
        # not used
        _dict.pop("files_indexed_by")
        assert repository and files and timestep

        datarecipes.extend(process_timeseries(repository, decoder, files, regions))

        for dr in datarecipes:
            dr["list_labels"].append({"fmri/timestep": timestep})

    if patch:
        for dr in datarecipes:
            dr["list_labels"].append({"bigbrain/section/celldensitypatch": patch})
    if section:
        for dr in datarecipes:
            dr["list_labels"].append({"bigbrain/section": section})
    if unit:
        for dr in datarecipes:
            dr["list_labels"].append({"unit": unit})
    if receptor:
        for dr in datarecipes:
            dr["list_labels"].append({"x-siibra/receptor": receptor})

    leftover_keys = list(_dict.keys())
    assert leftover_keys == [], f"{leftover_keys=}"
    assert len(datarecipes) > 0

    return {
        "schema": "siibra/feature/v0.1",
        "name": f"{modality}",
        "id": _id,
        "attributes": [*datarecipes, *attributes],
    }


def reduce_dict(acc: list[tuple[str, dict]], curr: tuple[str, dict]) -> list[str, dict]:

    suffixes_to_remove = [
        "5_HT1A",
        "5_HT2",
        "ALPHA1",
        "ALPHA2",
        "ALPHA4BETA2",
        "AMPA",
        "BZ",
        "D1",
        "GABAA",
        "GABAB",
        "M1",
        "M2",
        "M3",
        "MGLUR2_3",
        "NMDA",
        "KAINATE",
    ]

    fpath, _curr_dict = curr
    for attr in _curr_dict["attributes"]:
        if attr["schema"] == MODALITY_KEY:
            if attr["value"] != "neurotransmitter receptor profile":
                return [*acc, curr]

    fname = Path(fpath).name
    assert "_HOMO_SAPIENS_" in fname
    prefix, partition, suffix = fname.partition("_HOMO_SAPIENS_")

    suffix = suffix.removesuffix(".json")
    for suf in suffixes_to_remove:
        suffix = suffix.removesuffix(suf)
    suffix.removesuffix("_")
    suffix += ".json"

    final_fname = str(Path(fpath).parent / f"{prefix}_{suffix}")

    for fpath, _dict in acc:
        if fpath == final_fname:
            append_attr = [
                attr
                for attr in _curr_dict["attributes"]
                if attr["schema"] == "siibra/attr/data/v0.1"
            ]
            _dict["attributes"].extend(append_attr)
            return acc

    _id, *rest = _curr_dict["id"].split("--")
    assert len(rest) == 1
    _curr_dict["id"] = _id
    return [*acc, (final_fname, _curr_dict)]


def cvt_tabular():
    _dir = Path("old_configs/features/tabular")

    acc: list[tuple[str, dict]] = []
    for f in _dir.glob("**/*.json"):
        tf = json.loads(f.read_text())
        ntf = cvt_tf(tf)
        if ntf:
            dst = f.relative_to("old_configs/")
            dst = dst.parent / ("siibra_featureset_" + dst.name)
            acc = reduce_dict(acc, (str(dst), ntf))

    for fname, ntf in acc:
        Path(fname).parent.mkdir(exist_ok=True, parents=True)
        Path(fname).write_text(json.dumps(ntf, indent=2))


if __name__ == "__main__":
    cvt_tabular()
