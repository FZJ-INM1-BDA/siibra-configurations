import requests
import json

base_url = 'https://1um.brainatlas.eu'
search_url = "/registered_sections/search"

def main():
    resp = requests.get(f"{base_url}{search_url}")
    resp.raise_for_status()
    slices = resp.json()
    for slice in slices:
        urls = slice.get("urls")
        assert urls
        precomputed = urls.get("precomputed")
        assert precomputed
        _filename = slice.get("fileName")
        assert _filename
        section_id = slice.get("sectionId")
        assert section_id

        with open(f"features/volumes/bigbrain_1um_{_filename}.json", "w") as fp:
            json.dump({
                "@type": "siibra/feature/voi/v0.1",
                "name": f"Big Brain 1um slices section: {section_id}",
                "modality": "Histology",
                "space": {
                    "@id": "minds/core/referencespace/v1.0.0/a1655b99-82f1-420f-a3c2-fe80fd4c8588"
                },
                "providers": {
                    "neuroglancer/precomputed": precomputed
                }
            }, indent="\t", fp=fp)
            fp.write("\n")


if __name__ == "__main__":
    main()