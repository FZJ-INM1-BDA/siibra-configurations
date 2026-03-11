from pathlib import Path
import json


def cvt_img(feat):
    assert feat.pop("@type") in (
        "siibra/feature/section/v0.1",
        "siibra/feature/voi/v0.1",
    )
    attributes = []
    _id = feat.pop("@id")
    name = feat.pop("name")
    modality = feat.pop("modality")
    boundingbox = feat.pop("boundingbox")
    space = feat.pop("space")
    ebrains = feat.pop("ebrains", None)
    region = feat.pop("region", None)
    prerelease = feat.pop("prerelease", None)
    publications = feat.pop("publications", None)

    providers = feat.pop("providers")
    ngurl = providers.pop("neuroglancer/precomputed")
    assert ngurl
    assert providers == {}
    assert isinstance(ngurl, str)

    assert feat == {}

    attributes.append({
        "schema": "siibra/attr/desc/modality/v0.1",
        "value": modality
    })
    if publications:
        assert isinstance(publications, list)
        for pub in publications:
            url = pub.pop("url")
            assert isinstance(url, str)
            if url.startswith("https://doi.org"):
                attributes.append({
                    "schema": "siibra/attr/desc/doi/v0.1",
                    "value": url
                })
                continue
            assert url.startswith("https://dandiarchive.org/")
            attributes.append({
                "schema": "siibra/attr/desc/url/v0.1",
                "value": url,
                "text": "DANDI archive"
            })
    
    attributes.append({
        "schema": "siibra/attr/data/v0.1",
        "origin": ngurl,
        "steps": [
            {
                "key": "neuroglancer-precomputed"
            }
        ]
    })

    return {
        "schema": "siibra/feature/v0.1",
        "id": _id,
        "name": name,
        "attributes": attributes
    }


def cvt():
    _dir = Path("old_configs/features/images")
    for f in _dir.glob("**/*.json"):
        print("f", f)
        tf = json.loads(f.read_text())
        ntf = cvt_img(tf)
        if ntf:
            dst = f.relative_to("old_configs/")
            dst.parent.mkdir(exist_ok=True, parents=True)
            dst = dst.parent / ("siibra_feature_" + dst.name)
            dst.write_text(json.dumps(ntf, indent=2))


if __name__ == "__main__":
    cvt()
