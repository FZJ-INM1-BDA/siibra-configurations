import json
import pathlib
import requests
import pytest

neuroglancer_urls = []


def add_ng_url(providers: dict):
    for provider, details in providers.items():
        if not provider.startswith("neuroglancer"):
            continue
        if isinstance(details, str):
            neuroglancer_urls.append(details.split(" ")[0])
        else:
            neuroglancer_urls.extend([url.split(" ")[0] for url in details.values()])


for fpath in pathlib.Path("./").rglob("*.json"):
    with open(fpath, "r", encoding="utf-8") as fp:
        config = json.load(fp)

    if "providers" in config:
        add_ng_url(config["providers"])
    if "volumes" in config:
        for vol in config["volumes"]:
            add_ng_url(vol["providers"])


@pytest.mark.parametrize("url", neuroglancer_urls)
def test_info_response(url):
    req = requests.get(f"{url}/info")
    req.raise_for_status()
