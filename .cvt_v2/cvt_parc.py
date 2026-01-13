from pathlib import Path
import json
import requests

sess = requests.Session()
dsv_url = "https://data-proxy.ebrains.eu/api/v1/buckets/reference-atlas-data/ebrainsquery/v3/DatasetVersion/{dsv_uuid}.json"

"""

dsvs that do have dois
f40e466b-8247-463a-a4cb-56dfe68e7059
2c8ec4fb-45ca-4fe7-accf-c41b5e92c43d
fadcd2cb-9e8b-4e01-9777-f4d4df8f1ebc
"""


def cvt_parc(d):
    assert d["@type"] == "siibra/parcellation/v0.0.1"
    
    attr = []
    n_d = {
        "schema": "siibra/parcellationscheme/v0.1",
        "id": d["@id"],
        "name": d["name"],
        "species": d["species"],
    }
    
    if modality := d.get("modality"):
        attr.append({"schema": "siibra/attr/desc/modality/v0.1", "value": modality})

    if shortname := d.get("shortName"):
        
        attr.append({
            "schema": "siibra/attr/desc/name/v0.1",
            "value": d["name"],
            "shortform": shortname
        })

    if desc := d.get("description"):
        attr.append({"schema": "siibra/attr/desc/description/v0.1", "value": desc})

    for pub in d.get("publications", []):
        url: str = pub.get("url")
        assert url
        if url.startswith("https://doi.org"):
            attr.append({"schema": "siibra/attr/desc/doi/v0.1", "value": url})
            continue
        if url.startswith("http://dx.doi.org/"):
            attr.append({"schema": "siibra/attr/desc/doi/v0.1", "value": "https://doi.org/" + url.removeprefix("http://dx.doi.org/")})
            continue
        if url == "https://www.science.org/doi/10.1126/science.abb4588":
            attr.append({"schema": "siibra/attr/desc/doi/v0.1", "value": "https://doi.org/10.1126/science.abb4588"})
            continue
        assert "doi" not in url, f"{url}, huh?"
        attr.append(
            {
                "schema": "siibra/attr/desc/url/v0.1",
                "value": url,
                **({"text": citation} if (citation := pub.get("citation")) else {}),
            }
        )
        
    if dsv_uuid := d.get("ebrains", {}).get("openminds/DatasetVersion"):
        resp = sess.get(dsv_url.format(dsv_uuid=dsv_uuid))
        resp.raise_for_status()
        try:
            doi_url = resp.json()["doi"][0]["identifier"]
            attr.append({"schema": "siibra/attr/desc/doi/v0.1", "value": doi_url})
        except IndexError:
            print(dsv_uuid)
            ...
    
    new_r = []

    regionnameset = set()
    def process_region(region_d, parent_id=None):
        assert "name" in region_d
        rname = region_d.pop("name")
        assert rname not in regionnameset
        regionnameset.add(rname)
        for c in region_d.pop("children", []):
            process_region(c, rname)

        attr = {
            "name": rname,
        }
        if parent_id:
            attr["parent_id"] = parent_id
        region_attr = []

        if ebrains := region_d.pop("ebrains", None):
            region_attr.append(
                {
                    "schema": "siibra/attr/desc/ebrainsrefs/v0.1",
                    "ebrains": ebrains
                }
            )

        if rgb := region_d.pop("rgb", None):
            region_attr.append(
                {
                    "schema": "siibra/attr/desc/rgb/v0.1",
                    "value": rgb
                }
            )
        
        if region_attr:
            attr["attributes"] = region_attr

        region_d.pop("granularity", None)
        assert not region_d, region_d
        new_r.append(attr)

    for r in d["regions"]:
        process_region(r)

    n_d["attributes"] = attr
    n_d["regions"] = new_r
    return n_d

def cvt_parcs():
    
    _dir = Path("old_configs/parcellations")
    for f in _dir.glob("*.json"):
        parc = json.loads(f.read_text())
        nparc = cvt_parc(parc)
        
        ("parcellationschemes" / f.relative_to("old_configs/parcellations")).write_text(json.dumps(nparc, indent=4))
        

if __name__ == "__main__":
    cvt_parcs()
