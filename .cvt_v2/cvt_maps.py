from pathlib import Path
import json

import requests
from joblib import Memory

memory = Memory(".cache", verbose=0)

spaceid_to_species = {
    # marmoset
    "nencki/nm/referencespace/v1.0.0/MARMOSET_NM_NISSL_2020": "Callithrix Jacchus",
    # monkey
    "minds/core/referencespace/v1.0.0/MEBRAINS": "macaca mulatta",
    # mouse
    "minds/core/referencespace/v1.0.0/265d32a0-3d84-40a5-926f-bf89f68212b9": "mus musculus",
    # rat
    "minds/core/referencespace/v1.0.0/d5717c4a-0fa1-46e6-918c-b8003069ade8": "rattus norvegicus"
}

sparse_index_root = "https://data-proxy.ebrains.eu/api/v1/buckets/reference-atlas-data/sparse-indices/"

fname_to_sparseindex = {
    "colin27-": {
        "jba118": "colin27-jba118",
        "jba29": "colin27-jba29",
        "jba30_157regions": "colin27-jba30-lg",
        "jba30_175regions": "colin27-jba30-hg",
        "jba31_207": "colin27-jba31-lg",
        "jba31_227": "colin27-jba31-hg",
    },
    "mni152-": {
        "jba29": "mni152-jba29",
        "jba30_157regions": "mni152-jba30-lg",
        "jba30_175regions": "mni152-jba30-hg",
        "jba31_207": "mni152-jba31-lg",
        "jba31_227": "mni152-jba31-hg",
        "difumo1024": "mni152-difumo1024",
        "difumo512": "mni152-difumo512",
        "difumo256": "mni152-difumo256",
        "difumo128": "mni152-difumo128",
        "difumo64": "mni152-difumo64",
        "dwm": "mni152-dwm",
        "swm": "mni152-swm",
        "sw_hcp": "mni152-sw_hcp",
    }
}

resp_gzipped = set()
resp_not_gzipped = set()

def check_url_gzipped(url: str):
    if url in resp_gzipped:
        return f"{url}.gz", True
    if url in resp_not_gzipped:
        return url, False
    
    resp = requests.get(url)
    resp.raise_for_status()
    if resp.headers.get("Content-Encoding") == "gzip":
        resp_gzipped.add(url)
        return f"{url}.gz", True
    resp_not_gzipped.add(url)
    return url, False
    

sess = requests.Session()
dsv_url = "https://data-proxy.ebrains.eu/api/v1/buckets/reference-atlas-data/ebrainsquery/v3/DatasetVersion/{dsv_uuid}.json"

def process_nii(url: str, label:int=None, z:int=None):
    assert not (bool(label) == bool(z) == True)
    steps = [{"key": "file"}]
    if url.endswith(".nii.gz"):
        steps.append({"key": "gunzip"})
    
    steps.append({ "key": "nifti" })
    
    if label:
        steps.append({ "key": "extract-label-nii", "labels": [label] })
    
    if z:
        steps.append({ "key": "extract-z-nii", "z": z })
    return {
        "schema": "siibra/attr/data/v0.1",
        "origin": url,
        "steps": steps,
        "list_labels": [
            {
                "https://openminds.om-i.org/types/ContentType": "https://openminds.om-i.org/instances/contentTypes/application_vnd.nifti.1"
            }
        ]
    }

def process_ng_mesh(url: str, label: int):
    assert label
    return {
        "schema": "siibra/attr/data/v0.1",
        "origin": url,
        "steps": [
            {"key": "neuroglancer-precomputed"},
            {"key": "extract-label-nii", "labels": [label]},
        ],
        "list_labels": [
            {
                "https://openminds.om-i.org/types/ContentType": "tmp/contenttypes/neuroglancer.precompmesh"
            }
        ]
    }

def process_ng_vol(url: str, label: int):
    assert label
    return {
        "schema": "siibra/attr/data/v0.1",
        "origin": url,
        "steps": [
            {"key": "neuroglancer-precomputed"},
            {"key": "extract-label-nii", "labels": [label]},
        ],
        "list_labels": [
            {
                "https://openminds.om-i.org/types/ContentType": "https://openminds.om-i.org/instances/contentTypes/application_vnd.ebrains.image-service.neuroglancer.precomputed"
            }
        ]
    }

def process_gii_label(url: str, label: int):
    
    actual_url, gzipped = check_url_gzipped(url)
    
    steps = [
        { "key": "file" }
    ]
    if gzipped:
        steps.append({
            "key": "gunzip"
        })
    steps.append({
        "key": "gifti-label",
        "labels": [label]
    })
    return {
        "schema": "siibra/attr/data/v0.1",
        "origin": actual_url,
        "steps": steps,
        "list_labels": [
            {
                "https://openminds.om-i.org/types/ContentType": "https://openminds.om-i.org/instances/contentTypes/application_vnd.gifti"
            }
        ]
    }


@memory.cache
def get_doi_from_dsv(dsv_uuid: str):
    resp = sess.get(dsv_url.format(dsv_uuid=dsv_uuid))
    resp.raise_for_status()
    return resp.json()["doi"][0]["identifier"]


def process_index(index: dict, volumes: list[dict], parc_id: str, rname: str, **kwargs):
    vol_idx = index.get("volume", 0)
    volume = volumes[vol_idx]
    if _type := volume.get("@type", None):
        assert _type == "siibra/volume/v0.0.1"

    providers = volume.get("providers")
    ebrains = volume.get("ebrains", {})
    
    # ignore bounding box

    common_tags = [
        {
            "siibra/region/v0.1": f"{parc_id}::{rname}"
        }
    ]

    if dsv_uuid := ebrains.get("openminds/DatasetVersion"):
        doi_url: str = get_doi_from_dsv(dsv_uuid)
        common_tags.append({
            "https://doi.org": doi_url.removeprefix("https://doi.org/")
        })
        

    return_list = []

    for key, v in providers.items():
        if isinstance(v, dict):
            fragment = index.get("fragment")
            assert fragment and fragment in v
            url = v[fragment]
            dr = None
            if key == "gii-label":
                label = index.get("label")
                dr = process_gii_label(url, label)
            if key == "nii":
                dr = process_nii(url, label=index.get("label"), z=index.get("z"))
            if key == "neuroglancer/precomputed":
                dr = process_ng_vol(url, label=index.get("label"))
            if key == "neuroglancer/precompmesh":
                dr = process_ng_mesh(url, label=index.get("label"))
            if dr is None:
                raise Exception(f"{key} not caught")
            
            dr['list_labels'] = [
                *common_tags,
                *dr['list_labels'],
            ]
            return_list.append(dr)
            continue

        assert isinstance(v, str), f"not str: {v}"
        
        if key == "nii":
            
            dr = process_nii(v, label=index.get("label"), z=index.get("z"))
            dr['list_labels'] = [
                *common_tags,
                *dr['list_labels'],
            ]
            return_list.append(dr)
            continue

        if key == "neuroglancer/precomputed":
            dr = process_ng_vol(v, label=index.get("label"))
            dr['list_labels'] = [
                *common_tags,
                *dr['list_labels'],
            ]
            return_list.append(dr)
            continue
        
        if key == "neuroglancer/precompmesh":
            
            dr = process_ng_mesh(v, label=index.get("label"))
            dr['list_labels'] = [
                *common_tags,
                *dr['list_labels'],
            ]
            return_list.append(dr)
            continue

        if key == "zip/nii":
            url, filename = v.split(" ", maxsplit=1)
            dr = {
                "schema": "siibra/attr/data/v0.1",
                "origin": url,
                "steps": [
                    {"key": "zipGetFile", "filename": filename},
                    {"key": "gunzip"},
                    {"key": "nifti"},
                    {"key": "extract-label-nii", "labels": [index.get("label")]},
                ],
                "list_labels": [
                    *common_tags,
                    {
                        "https://openminds.om-i.org/types/ContentType": "https://openminds.om-i.org/instances/contentTypes/application_vnd.nifti.1"
                    },
                    
                ]
            }
            return_list.append(dr)
            continue

        raise Exception(f"key {key}")

    return return_list


def cvt_map(d: dict, fname: str):
    
    _type = d.pop("@type")
    assert _type == "siibra/map/v0.0.1", f"{_type}"

    maptype = None
    if fname.endswith("-continuous.json"):
        maptype = "statistical"
    if fname.endswith("-labelled.json"):
        maptype = "labelled"
    assert maptype, f"{fname} is neither statistical or labelled"

    
    d.pop("species", None)
    
    # TODO no way to express relatesto for now
    space_id = d.pop("space")["@id"]
    parc_id = d.pop("parcellation")["@id"]

    n_d = {
        "schema": "siibra/annotationset/v0.1",
        "id": d.pop("@id"),
        "name": d.pop("name"),
        "species": spaceid_to_species.get(space_id, "homo sapien"),
    }

    attr = []

    for pub in d.pop("publications", []):
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
    
    volumes = d.pop("volumes")
    
    for rname, indices in d.pop("indices").items():
        for index in indices:
            drs = process_index(index, volumes, parc_id=parc_id, rname=rname)
            for dr in drs:
                dr['list_labels'].append({
                    "siibra/maptype": maptype
                })
                attr.append(dr)
    
    for prefix, v in fname_to_sparseindex.items():
        if fname.startswith(prefix):
            stem = fname.removeprefix(prefix).removesuffix("-continuous.json")
            if relative_path := v.get(stem):
                attr.append({
                    "schema": "siibra/attr/data/v0.1",
                    "origin": f"{sparse_index_root}{relative_path}",
                    "steps": [
                        {
                            "key": "sparseindex-readcoords"
                        }
                    ],
                    "list_labels": [
                        {
                            "https://openminds.om-i.org/types/ContentType": "tmp/contenttypes/spatial-index.v0"
                        }
                    ]
                })
            break
            
        
    n_d["attributes"] = attr
    return n_d
    
def cvt_maps():
    
    _dir = Path("old_configs/maps")
    for f in _dir.glob("*.json"):
        oldmap = json.loads(f.read_text())
        nmap = cvt_map(oldmap, str(f.relative_to(_dir)))
        
        ("annotationsets" / f.relative_to("old_configs/maps")).write_text(json.dumps(nmap, indent=4))
        

if __name__ == "__main__":
    cvt_maps()
