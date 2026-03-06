from pathlib import Path
import json
from hashlib import md5

import requests
from joblib import Memory

memory = Memory(".cache", verbose=0)

modality_map = {}

sess = requests.Session()
dsv_url = "https://data-proxy.ebrains.eu/api/v1/buckets/reference-atlas-data/ebrainsquery/v3/DatasetVersion/{dsv_uuid}.json"


@memory.cache
def get_doi_from_dsv(dsv_uuid: str):
    resp = sess.get(dsv_url.format(dsv_uuid=dsv_uuid))
    resp.raise_for_status()
    return resp.json()["doi"][0]["identifier"]


def cvt_d(_dict: dict, experimental=False):
    _id = _dict.pop("@id", None) or md5(json.dumps(_dict).encode()).hexdigest()
    _type = _dict.pop("@type")

    attributes = []
    datarecipes = []

    modality = _dict.pop("modality")
    species = _dict.pop("species", None)

    # desc attr?
    cohort = _dict.pop("cohort")
    paradigm = _dict.pop("paradigm", None)

    # TODO waiting on anchorpointer
    parcellation = _dict.pop("parcellation")

    ebrains = _dict.pop("ebrains")

    repository = _dict.pop("repository", None)
    base_url = _dict.pop("base_url", None)
    files = _dict.pop("files")

    decoder = _dict.pop("decoder")
    if sep := decoder.pop("delimiter", None):
        decoder["sep"] = sep

    regions = _dict.pop("regions")
    publications = _dict.pop("publications", None)
    prerelease = _dict.pop("prerelease", None)
    files_indexed_by = _dict.pop("files_indexed_by")
    description = _dict.pop("description", None)

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


    if base_url or repository:
        drs = None
        assert files
        assert files_indexed_by
        pid = parcellation.get("@id")
        assert pid
        assert decoder.pop("@type") == "siibra/decoder/csv"

        definitions = {
            "decoder": {"key": "readByteAsDf", **decoder},
            "remapper": {
                "key": "dfRename",
                "apply_index": True,
                "apply_columns": True,
                "remapper": {idx: f"{pid}::{region}" for idx, region in enumerate(regions)},
            },
        }

        if base_url:
            assert (
                base_url
                == "https://data-proxy.ebrains.eu/api/v1/buckets/d-41db823e-7e1b-44c7-9c69-eaa26e226384/MNI-JulichBrain-3.0/"
            )
            drs = [
                {
                    "schema": "siibra/attr/data/v0.1",
                    "origin": f"{base_url}{file}",
                    "steps": [
                        {"key": "file"},
                    ],
                    "list_labels": [{f"x-conn-index/{files_indexed_by}": key}],
                }
                for key, file in files.items()
            ]
        if repository:

            assert repository.pop("@type") == "siibra/repository/zippedfile/v1.0.0"
            zipurl = repository.pop("url")
            assert repository == {}
            drs = [
                {
                    "schema": "siibra/attr/data/v0.1",
                    "origin": zipurl,
                    "steps": [
                        {"key": "zipGetFile", "filename": file},
                    ],
                    "list_labels": [{f"x-conn-index/{files_indexed_by}": key}],
                }
                for key, file in files.items()
            ]

        for dr in drs:
            if experimental:
                dr["steps"].extend(
                    [
                        {"$ref": "#/definitions/decoder"},
                        {"$ref": "#/definitions/remapper"},
                    ]
                )
            else:
                dr["steps"].extend([
                    definitions['decoder'],
                    definitions['remapper'],
                ])

        datarecipes.extend(drs)

    assert len(datarecipes) > 0
    assert _dict == {}, f"{_dict}"
    if experimental:
        return {"id": _id, "schema": "siibra/feature/v0.1", "attributes": [*datarecipes, *attributes], "definitions": definitions}
    else:
        return {"id": _id, "schema": "siibra/feature/v0.1", "attributes": [*datarecipes, *attributes]}

def cvt_conn():
    _dir = Path("old_configs/features/connectivity")
    for f in _dir.glob("**/*.json"):
        print("f", f)
        tf = json.loads(f.read_text())
        
        if ntf:= cvt_d(tf, experimental=True):
            dst = f.relative_to("old_configs/")
            dst = "proposed" / dst
            dst.parent.mkdir(exist_ok=True, parents=True)
            dst = dst.parent / ("siibra_feature_" + dst.name)
            dst.write_text(json.dumps(ntf, indent=2))
            
        tf = json.loads(f.read_text())
        if ntf := cvt_d(tf, experimental=False):
            dst = f.relative_to("old_configs/")
            dst.parent.mkdir(exist_ok=True, parents=True)
            dst = dst.parent / ("siibra_feature_" + dst.name)
            ntf['id'] = "legacy-" + ntf['id']
            dst.write_text(json.dumps(ntf, indent=2))



if __name__ == "__main__":
    cvt_conn()
