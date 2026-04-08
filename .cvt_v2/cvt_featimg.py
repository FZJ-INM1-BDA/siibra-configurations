from pathlib import Path
import json


def process_ebrains(spec: dict[str, str]):
    if spec is None:
        return []
    if sp := spec.get("openminds/Species"):
        assert sp in {
            "97c070c6-8e1f-4ee8-9d28-18c7945921dd",  # homo sapien
            "ab532423-1fd7-4255-8c6f-f99dc6df814f",  # Rattus norvegicus
            "627bac2f-2b4c-4f2d-ae4e-9373a7f430f2", # marmoset
        }
    spec = {k: v for k, v in spec.items() if not k.startswith("minds/")}

    return [
        {
            "schema": "siibra/attr/desc/resolvable/v0.1",
            "spec": {
                "ebrains": spec,
            },
        }
    ]


def process_region(region_str: str | None = None, parcellation_id: str | None = None):
    if region_str is None and parcellation_id is None:
        return []
    spec = {}
    if region_str:
        spec["name"] = region_str
    if parcellation_id:
        spec["parcellation_id"] = parcellation_id

    return [
        {
            "schema": "siibra/attr/desc/resolvable/v0.1",
            "spec": {"siibra/region/spec/v0.1": spec},
        }
    ]


def process_space(space_id: str | None = None):
    if space_id is None:
        return []

    return [
        {
            "schema": "siibra/attr/desc/resolvable/v0.1",
            "spec": {"siibra/space/spec/v0.1": {"id": space_id}},
        }
    ]


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
    attributes.extend(process_region(feat.pop("region", None)))
    attributes.extend(process_ebrains(feat.pop("ebrains", None)))

    attributes.extend(process_space(feat.pop("space", {}).get("@id")))

    prerelease = feat.pop("prerelease", None)
    publications = feat.pop("publications", None)

    providers = feat.pop("providers")
    ngurl = providers.pop("neuroglancer/precomputed")
    assert ngurl
    assert providers == {}
    assert isinstance(ngurl, str)

    assert feat == {}

    attributes.append({"schema": "siibra/attr/desc/modality/v0.1", "value": modality})
    if publications:
        assert isinstance(publications, list)
        for pub in publications:
            url = pub.pop("url")
            assert isinstance(url, str)
            if url.startswith("https://doi.org"):
                attributes.append({"schema": "siibra/attr/desc/doi/v0.1", "value": url})
                continue
            assert url.startswith("https://dandiarchive.org/")
            attributes.append(
                {
                    "schema": "siibra/attr/desc/url/v0.1",
                    "value": url,
                    "text": "DANDI archive",
                }
            )

    attributes.append(
        {
            "schema": "siibra/attr/data/v0.1",
            "origin": ngurl,
            "steps": [{"key": "neuroglancer-precomputed"}],
        }
    )

    return {
        "schema": "siibra/feature/v0.1",
        "id": _id,
        "name": name,
        "attributes": attributes,
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
