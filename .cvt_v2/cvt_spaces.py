from pathlib import Path
import json
import requests


ignore_urls = ("https://nist.mni.mcgill.ca/icbm-152-nonlinear-atlases-2009/",)


sess = requests.Session()
dsv_url = "https://data-proxy.ebrains.eu/api/v1/buckets/reference-atlas-data/ebrainsquery/v3/DatasetVersion/{dsv_uuid}.json"

def cvt_vol(vol: dict):

    volumes = []

    for key, v in vol["providers"].items():
        if key == "gii-mesh":
            assert "left hemisphere" in v
            assert "right hemisphere" in v

            # TODO variant ignore
            # assert "variant" in v, f"{v} huh?"

            # TODO hemisphere not labelled
            for hemi in ["left hemisphere", "right hemisphere"]:
                volumes.append(
                    {
                        "schema": "siibra/attr/data/v0.1",
                        "origin": v[hemi],
                        "steps": [{"key": "file"}, {"key": "gifti-mesh"}],
                    }
                )
            continue
        assert isinstance(v, str), f"{v} is not str"
        if v.startswith("http://"):
            old_v = v
            v = "https://" + v.removeprefix("http://")
            print(f"converting {old_v} to {v}")
        if key == "neuroglancer/precomputed":
            volumes.append(
                {
                    "schema": "siibra/attr/data/v0.1",
                    "origin": v,
                    "steps": [{"key": "neuroglancer-precomputed"}],
                }
            )
            continue
        if key == "neuroglancer/precompmesh/surface":
            url, label = v.split()
            volumes.append(
                {
                    "schema": "siibra/attr/data/v0.1",
                    "origin": url,
                    "steps": [
                        {"key": "neuroglancer-precompmesh", "labels": [int(label)]},
                        {"key": "merge-mesh"},
                    ],
                }
            )
            continue
        if key == "zip/nii":
            url, fname = v.split()
            volumes.append(
                {
                    "schema": "siibra/attr/data/v0.1",
                    "origin": url,
                    "steps": [
                        {"key": "zipGetFile", "filename": fname},
                        {"key": "nifti"},
                    ],
                }
            )
            continue
        if key == "nii":
            assert v.endswith(".nii.gz")
            volumes.append(
                {
                    "schema": "siibra/attr/data/v0.1",
                    "origin": v,
                    "steps": [{"key": "file"}, {"key": "gunzip"}, {"key": "nifti"}],
                }
            )
            continue
        raise Exception(f"{key} not supported, {v}")
    return volumes


def cvt_sp(sp: dict):
    assert sp["@type"] == "siibra/space/v0.0.1"
    attr = []
    n_d = {
        "schema": "siibra/space/v0.1",
        "id": sp["@id"],
        "name": sp["name"],
        "species": sp["species"],
    }

    if modality := sp.get("modality"):
        attr.append({"schema": "siibra/attr/desc/modality/v0.1", "value": modality})

    if desc := sp.get("description"):
        attr.append({"schema": "siibra/attr/desc/description/v0.1", "value": desc})

    if shortname := sp.get("shortName"):
        attr.append({
            "schema": "siibra/attr/desc/name/v0.1",
            "value": sp["name"],
            "shortform": shortname
        })

    for pub in sp.get("publications", []):
        url: str = pub.get("url")
        assert url
        if url.startswith("https://doi.org"):
            attr.append({"schema": "siibra/attr/desc/doi/v0.1", "value": url})
            continue
        if url.startswith("http://dx.doi.org/"):
            attr.append({"schema": "siibra/attr/desc/doi/v0.1", "value": "https://doi.org/" + url.removeprefix("http://dx.doi.org/")})
            continue
        assert "doi" not in url, f"{url}, huh?"
        attr.append(
            {
                "schema": "siibra/attr/desc/url/v0.1",
                "value": url,
                **({"text": citation} if (citation := pub.get("citation")) else {}),
            }
        )
    for vol in sp["volumes"]:
        attr.extend(cvt_vol(vol))
    
    if dsv_uuid := sp.get("ebrains", {}).get("openminds/DatasetVersion"):
        resp = sess.get(dsv_url.format(dsv_uuid=dsv_uuid))
        resp.raise_for_status()
        doi_url = resp.json()["doi"][0]["identifier"]
        attr.append({"schema": "siibra/attr/desc/doi/v0.1", "value": doi_url})
    
    n_d["attributes"] = attr

    return n_d


def cvt_spaces():
    _dir = Path("old_configs/spaces")
    for f in _dir.glob("*.json"):
        sp = json.loads(f.read_text())
        nsp = cvt_sp(sp)
        
        f.relative_to("old_configs/").write_text(json.dumps(nsp, indent=4))
        
        pass
    pass


if __name__ == "__main__":
    cvt_spaces()
