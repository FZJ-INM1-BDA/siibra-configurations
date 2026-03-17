from pathlib import Path
import json
from hashlib import md5

import requests
from joblib import Memory

memory = Memory(".cache", verbose=0)

modality_map = {}

sess = requests.Session()
dsv_url = "https://data-proxy.ebrains.eu/api/v1/buckets/reference-atlas-data/ebrainsquery/v3/DatasetVersion/{dsv_uuid}.json"


strategy = {
    "ftract": None,
    "functional/slc/sll harvard": "get use annotationset (?)",
    "functional/slc/sll jba29": "get 294-Julich-Brain/0ImageProcessing/Link.txt , skip 26 lines",
    "functional/slc/sll jba30": "get 314-JuBrain/0ImageProcessing/Link.txt , skip 19 lines",
    "functional/slc/sll jba31": "get 414-Julich-Brain/0ImageProcessing/Link.txt , skip 27 lines",
    "tracer": "get first row",
}


@memory.cache
def get_doi_from_dsv(dsv_uuid: str):
    resp = sess.get(dsv_url.format(dsv_uuid=dsv_uuid))
    resp.raise_for_status()
    return resp.json()["doi"][0]["identifier"]

def _cvt_base_url(base_url: str, files: dict[str, str], index_by: str):
    assert index_by == "feature"
    return [
        {
            "schema": "siibra/attr/data/v0.1",
            "origin": base_url,
            "steps": [
                {
                    "key": "fork",
                    "list_of_specs": [
                        [
                            {"key": "appendStr", "value": file},
                            {"key": "file"},
                            {"$ref": "#/definitions/decoder"},
                        ]
                    ]
                },
                {"key": "dfRename", "index_col_flags": []},
            ],
            "list_labels": [{"x-index/feature": key}],
        }
        for key, file in files.items()
    ]
    ...

def _cvt_repo(zipurl: str, files: dict[str, str], index_by: str):
    assert index_by == "subject"
    return [
        {
            "schema": "siibra/attr/data/v0.1",
            "origin": zipurl,
            "steps": [
                {
                    "key": "fork",
                    "list_of_specs": [
                        [
                            {"key": "zipGetFile", "filename": file},
                            {"$ref": "#/definitions/decoder"},
                        ]
                    ]
                },
                {"key": "dfRename", "index_col_flags": [[True, True]]},
            ],
            "list_labels": [{"https://schema.org/studySubject": key}],
        }
        for key, file in files.items()
    ]

def cvt_d(_dict: dict):

    def get_fs_name_from_modality():
        match modality:
            case "FunctionalConnectivity":
                return f"Functional Connectivity {cohort=}"
            case "StreamlineLengths":
                return f"Streamline Lengths {cohort=}"
            case "StreamlineCounts":
                return f"Streamline Counts {cohort=}"
            case "TracingConnectivity":
                return f"Tracing Connectivity {cohort=}"
            case "AnatomoFunctionalConnectivity":
                return f"Anatomo Functional Connectivity {cohort=}"
            case _:
                raise Exception(f"{modality=} not caught")

    attributes = []
    
    _id = _dict.pop("@id", None) or md5(json.dumps(_dict).encode()).hexdigest()
    _type = _dict.pop("@type")
    cohort = _dict.pop("cohort")
    paradigm = _dict.pop("paradigm", None)
    # TODO waiting on anchorpointer
    parcellation = _dict.pop("parcellation")
    repository = _dict.pop("repository", None)
    base_url = _dict.pop("base_url", None)
    modality = _dict.pop("modality")
    
    files = _dict.pop("files")

    decoder = _dict.pop("decoder")
    assert decoder.pop("@type") == "siibra/decoder/csv"
    if sep := decoder.pop("delimiter", None):
        decoder["sep"] = sep

    regions = _dict.pop("regions")
    publications = _dict.pop("publications", None)
    prerelease = _dict.pop("prerelease", None)
    files_indexed_by = _dict.pop("files_indexed_by")
    description = _dict.pop("description", None)
    species = _dict.pop("species", None)
    ebrains = _dict.pop("ebrains")
    
    assert modality in (
        "FunctionalConnectivity",
        "StreamlineCounts",
        "StreamlineLengths",
        "TracingConnectivity",

        "AnatomoFunctionalConnectivity",
    )

    definitions = {
        "decoder": {"key": "readByteAsDf", **decoder},
    }

    if repository:

        assert repository.pop("@type") == "siibra/repository/zippedfile/v1.0.0"
        zipurl = repository.pop("url")
        assert repository == {}

        drs = _cvt_repo(zipurl, files, files_indexed_by)
        if zipurl:
            # harvard does not have region mapping in file
            # neither does 1000brains
            if (
                parcellation.get("@id") != "https://identifiers.org/neurovault.image:1702"
                and cohort != "1000brains"
            ):
                remapper_steps = [
                    {
                        "key": "zipGetFile",
                        "filename": None,  # "294-Julich-Brain/0ImageProcessing/Link.txt",
                    },
                    {
                        "key": "contrib:remapperByteToDict",
                        # "flavor": "hcp:infile",
                        # "skiprow": 26,
                    },
                ]
                match parcellation.get("@id"):
                    case "minds/core/parcellationatlas/v1.0.0/94c1125b-b87e-45e4-901c-00daee7f2579-290":
                        remapper_steps[0][
                            "filename"
                        ] = "294-Julich-Brain/0ImageProcessing/Link.txt"
                        remapper_steps[1]["flavor"] = "hcp:infile"
                        remapper_steps[1]["skiprow"] = 26
                    case "minds/core/parcellationatlas/v1.0.0/94c1125b-b87e-45e4-901c-00daee7f2579-300":
                        remapper_steps[0]["filename"] = "314-JuBrain/0ImageProcessing/Link.txt"
                        remapper_steps[1]["flavor"] = "hcp:infile"
                        remapper_steps[1]["skiprow"] = 19
                    case "minds/core/parcellationatlas/v1.0.0/94c1125b-b87e-45e4-901c-00daee7f2579-310":
                        remapper_steps[0][
                            "filename"
                        ] = "414-Julich-Brain/0ImageProcessing/Link.txt"
                        remapper_steps[1]["flavor"] = "hcp:infile"
                        remapper_steps[1]["skiprow"] = 27
                    case "minds/core/parcellationatlas/v1.0.0/ebb923ba-b4d5-4b82-8088-fa9215c2e1fe-v4":
                        remapper_steps[0]["filename"] = "13985-BDA.csv"
                        remapper_steps[1]["flavor"] = "tracing"
                    case _:
                        raise Exception("unknown parcellation")
                
                definitions["remapperGetBytes"] = remapper_steps[0]
                definitions["remapperByteToDict"] = remapper_steps[1]
                for dr in drs:
                    forkstep = dr["steps"][0]
                    assert forkstep.get("key") == "fork"
                    forkstep["list_of_specs"].append([
                        {
                            "$ref": "#/definitions/remapperGetBytes"
                        },
                        {
                            "$ref": "#/definitions/remapperByteToDict"
                        }
                    ])

    if base_url:
        drs = _cvt_base_url(base_url, files, files_indexed_by)

    # metadata time
    attributes.append({"schema": "siibra/attr/desc/modality/v0.1", "value": modality})
    if species:
        attributes.append({"schema": "siibra/attr/desc/species/v0.1", "value": species})
    if publications:
        for pub in publications:
            assert "url" in pub
            assert pub["url"].startswith("https://doi.org")
            attributes.append(
                {"schema": "siibra/attr/desc/doi/v0.1", "value": pub["url"]}
            )
    if description:
        attributes.append(
            {"schema": "siibra/attr/desc/description/v0.1", "value": description}
        )
    if ebrains:
        if dsv_uuid := ebrains.get("openminds/DatasetVersion"):
            doi_url: str = get_doi_from_dsv(dsv_uuid)
            attributes.append({"schema": "siibra/attr/desc/doi/v0.1", "value": doi_url})
        # TODO what about other attributes?
    return {
        "id": _id,
        "name": get_fs_name_from_modality(),
        "schema": "siibra/feature/v0.1",
        "attributes": [*drs, *attributes],
        "definitions": definitions,
    }


def cvt_conn():
    _dir = Path("old_configs/features/connectivity")
    for f in _dir.glob("**/*.json"):
        print("f", f)
        tf = json.loads(f.read_text())
        
        if ntf := cvt_d(tf):
            dst = f.relative_to("old_configs/")
            dst.parent.mkdir(exist_ok=True, parents=True)
            dst = dst.parent / ("siibra_featureset_" + dst.name)
            ntf["id"] = ntf["id"]
            dst.write_text(json.dumps(ntf, indent=2))


if __name__ == "__main__":
    cvt_conn()
