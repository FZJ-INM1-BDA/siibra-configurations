import os
import json
from collections import defaultdict

def sanitize_publication(pub):
    assert any([
        pub.get("doi") is not None,
        pub.get("cite") is not None,
    ]), f"Oh no, {pub}"
    
    citation = pub.get("cite")
    url = None
    if "doi" in pub and pub['doi'] is not None:
        if pub['doi'].startswith("http"):
            url = pub['doi']
        else:
            url = "https://doi.org/" + pub['doi']

    return {
        **({ "citation": citation } if citation else {}),
        **({ "url": url } if url else {}),
    }

def has_value(dictionary, value):
    for key in dictionary:
        if dictionary[key] == value:
            return True
    return False

def append_ebrains_ref(url_to_filename, urls, ebrains_dss):
    if len(ebrains_dss) == 0:
        return
    try:
        assert len(urls) > 0
    except AssertionError as e:
        if not any([ds.get("kgId") in DIFUMO_DS_IDS for ds in ebrains_dss]):
            import pdb
            pdb.set_trace()
            raise e
    assert all([url is not None for url in urls])
    for url in urls:
        assert url in url_to_filename, f"expected url {url} to be registered, but was not."
        for fullpath in url_to_filename[url]:
            with open(fullpath, 'r') as fp:
                map_json = json.load(fp)
            volumes = map_json.get("volumes", [])
            assert len(volumes) > 0
            map_json['volumes'] = [{
                    **vol,
                    'ebrains': {
                        **vol.get("ebrains", {}),
                        **{ebrains_ds.get("kgSchema"): ebrains_ds.get("kgId") for ebrains_ds in ebrains_dss},
                    }
                }
                if has_value(vol.get("urls"), url)
                else vol
                for vol in volumes]
            with open(fullpath, 'w') as fp:
                json.dump(map_json, fp=fp, indent="\t")
                fp.write("\n")

# some difumo region datasets were using an archaic way to fetching prob maps
# they are no longer needed, and thus filtered out.
DIFUMO_DS_IDS = (
    "e472a8c7-d9f9-4e75-9d0b-b137cecbc6a2",
    "b8d0ba16-5543-4594-a6f0-ecbacfc9fb04",
    "5438792c-ff2a-4554-9f85-af795f870741",
    "5438792c-ff2a-4554-9f85-af795f870741",
    "164ef5c9-bec5-43c7-b258-80798cb0d57b",

    # for jba25
    "a260b3b4eadda85af9fd8df4100c10a9",
    "a8247117-3349-49b7-a2f4-aa62b5fdd115",
    "1c9fad3c-d235-4074-b505-74cd1c6fa3db",
    "b5aed403-291b-4dfd-8a67-b4419b644fdc",
    "018b51f0-c2e1-410b-b82b-9c14ac7d47b4",
    "784eb7bd-1368-4248-8f47-ca667694c463",
    "4938b81c-e040-4ceb-a751-41b7384058c7",
    "0bea7e03-bfb2-4907-9d45-db9071ce627d",
    "677c84c3-56b2-4671-bc63-15d3dda730a2",
    "cf4b3fad-2d45-458b-8bc7-1095983ed1dd",

)
REGION_TYPE = "minds/core/parcellationregion/v1.0.0"
DUPLICATED_ID_FILE = ".duplicated_id.log"

def process_region(region, url_to_filename):

    # ebrain references
    ebrains_references = [area.get("fullId", {}) for area in region.pop("relatedAreas", [])]
    if "fullId" in region and region["fullId"] is not None:
        full_id = region.pop("fullId", {})
        if "kg" in full_id:
            ebrains_references.append(full_id)
        if REGION_TYPE in full_id:
            kg_ids = list(full_id[REGION_TYPE].keys())
            ebrains_references.append({
                "kg": {
                    "kgSchema": REGION_TYPE,
                    "kgId": kg_ids[0]
                }
            })
    if len(ebrains_references) > 0:
        assert all([
            'kg' in ref and 'kgSchema' in ref['kg'] and 'kgId' in ref['kg']
            for ref in ebrains_references
        ])
        seen_schema = set()
        all_schema = defaultdict(set)
        duplicated_schema = set()
        for ref in ebrains_references:
            schema = ref['kg']['kgSchema']
            _id = ref['kg']['kgId']
            
            if schema in seen_schema:
                if _id not in all_schema[schema]:
                    duplicated_schema.add(schema)

            all_schema[schema].add(_id)
            seen_schema.add(schema)

        ebrains_references = [ref
            for ref in ebrains_references
            if ref['kg']['kgSchema'] not in duplicated_schema]

        if len(ebrains_references) > 0:
            region['ebrains'] = {
                ref['kg']['kgSchema']: ref['kg']['kgId']
                for ref in ebrains_references
            }

        if len(duplicated_schema) > 0:
            with open(DUPLICATED_ID_FILE, "a") as fp:
                fp.write(f"region {region.get('name')} has duplicated schemas: {[(key,  all_schema[key]) for key in all_schema if key in duplicated_schema]}")
                fp.write("\n")

    keep_keys = (
        "name",
        "children",
        "rgb",
        "ebrains"
    )

    # ensure we are not removing datasets that we did not yet account for
    
    assert all([
        ds.get("@type") in ("fzj/tmp/volume_type/v0.0.1", "minds/core/dataset/v1.0.0")
        or "kgSchema" in ds
        for ds in region.get('datasets', [])
    ])
    ebrains_dss = [ds for ds in region.get("datasets", []) if ds.get("@type") == "minds/core/dataset/v1.0.0" or "kgSchema" in ds]
    urls_to_patch = [ds.get("url") for ds in region.get("datasets", []) if ds.get("@type") == "fzj/tmp/volume_type/v0.0.1"]
    append_ebrains_ref(url_to_filename, urls_to_patch, ebrains_dss)

    for key in list(region.keys()):
        if key not in keep_keys:
            region.pop(key, None)

    children = region.get("children", [])
    for child in children:
        process_region(child, url_to_filename)
    

def main():
    # first, create a map of all urls to filenames
    url_to_filename = {}
    for dirpath, dirname, filenames in os.walk("./maps"):
        if dirpath != "./maps":
            continue
        for filename in filenames:
            if not filename.endswith(".json"):
                continue
            full_path = f"{dirpath}/{filename}"
            with open(full_path, "r") as fp:
                map_json = json.load(fp)
            for volume in map_json.get("volumes", []):
                for _key, url in volume.get("urls").items():
                    url_to_filename[url] = [*url_to_filename.get(url, []), full_path]

    for dirpath, dirname, filenames in os.walk("./parcellations"):
        if dirpath != "./parcellations":
            continue
        for filename in filenames:
            if not filename.endswith(".json"):
                continue
            full_path = f"{dirpath}/{filename}"
            with open(full_path, "r") as fp:
                parc_json = json.load(fp=fp)
            
            # remove key
            unused_key = (
                "groupName",
                "properties"
            )
            for key in unused_key:
                parc_json.pop(key, None)

            # fix datasets
            datasets = parc_json.pop("datasets", [])
            # volumes already sorted
            datasets = [ds for ds in datasets if ds.get("@type") != "fzj/tmp/volume_type/v0.0.1"]
            # ebrains
            ebrains_dss = [ds for ds in datasets if ds.get("@type") == "minds/core/dataset/v1.0.0"]
            if len(ebrains_dss) > 0:
                parc_json['ebrains'] = {
                    ebrains_ds.get("kgSchema"): ebrains_ds.get("kgId")
                    for ebrains_ds in ebrains_dss
                }
            
            datasets = [ds for ds in datasets if ds.get("@type") != "minds/core/dataset/v1.0.0"]

            # simple origin info
            so_dss = [ds for ds in datasets if ds.get("@type") == "fzj/tmp/simpleOriginInfo/v0.0.1"]
            if len(so_dss) > 0:
                assert len(so_dss) == 1
                so_ds = so_dss[0]
                description = so_ds.pop("description", None)
                if description:
                    parc_json['description'] = description
            

            # fix publications
            publications = [
                *parc_json.pop("publications", []),
                *[url for ds in so_dss for url in ds.get("urls", [])],
            ]
            if len(publications) > 0:
                parc_json['publications'] = [sanitize_publication(pub) for pub in publications]
            
            # fix version
            parc_json.pop("version", None)
            version = parc_json.get("@version", None)
            if version:
                remove_keys = ("collectionName", "@next",)
                for key in remove_keys:
                    version.pop(key, None)

            # fix regions
            regions = parc_json.get("regions", [])
            if len(regions) == 0:
                raise RuntimeError(f"parcellation should always have regions")
            for region in regions:
                process_region(region, url_to_filename)

            parc_json['@type'] = "siibra/parcellation/v0.0.1"
            with open(full_path, 'w') as fp:
                json.dump(parc_json, fp=fp, indent="\t")
                fp.write("\n")

if __name__ == "__main__":
    main()