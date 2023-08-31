import json
import os
from siibra.retrieval import ZipfileRequest

LOCAL_CONFIG_FOLDER = "./"
DIFUMO_URLS = {
    "64": "https://osf.io/pqu9r/download",
    "128": "https://osf.io/wjvd5/download",
    "256": "https://osf.io/3vrct/download",
    "512": "https://osf.io/9b76y/download",
    "1024": "https://osf.io/34792/download"
}


def create_regionname(component: int, difumo_name: str):
    if difumo_name.endswith(" LH"):
        difumo_name = difumo_name.replace(" LH", " - left hemisphere")
    elif difumo_name.endswith(" RH"):
        difumo_name = difumo_name.replace(" RH", " - right hemisphere")

    return {"name": f"Component {component}: {difumo_name}"}


def create_labelledmap_index(component: int, difumo_name: str):
    regionname = create_regionname(component, difumo_name)
    key = regionname["name"]
    return (key, [{"label": component, "volume": 0}])


def create_statisticalmap_index(component: int, difumo_name: str):
    regionname = create_regionname(component, difumo_name)
    key = regionname["name"]
    return (key, [{"z": component-1, "volume": 0}])


def main():
    maps_folder = f"{LOCAL_CONFIG_FOLDER}/maps"
    parc_folder = f"{LOCAL_CONFIG_FOLDER}/parcellations"

    for difumokey, url in DIFUMO_URLS.items():
        # get the original data
        atlas_df = ZipfileRequest(
            url=url, filename=f'{difumokey}/labels_{difumokey}_dictionary.csv'
        ).data

        # remake region names of the parcellation
        parc_file_path = os.path.join(parc_folder, f'difumo{difumokey}.json')
        with open(parc_file_path, "r") as fp:
            parc_config = json.load(fp)

        parc_config['regions'] = [
            create_regionname(comp, name)
            for comp, name in zip(atlas_df.Component, atlas_df.Difumo_names)
        ]

        with open(parc_file_path, "w") as fp:
            json.dump(parc_config, fp=fp, indent="\t")

        # remake indices of the labelled map
        labelled_map_file_path = os.path.join(
            maps_folder, f'mni152-difumo{difumokey}-labelled.json'
        )
        with open(labelled_map_file_path, "r") as fp:
            labelled_map_config = json.load(fp)

        labelled_map_config['indices'] = {}
        for comp, name in zip(atlas_df.Component, atlas_df.Difumo_names):
            key, index = create_labelledmap_index(comp, name)
            labelled_map_config['indices'][key] = index

        with open(labelled_map_file_path, "w") as fp:
            json.dump(labelled_map_config, fp=fp, indent="\t")

        # remake indices of the labelled map
        stat_map_file_path = os.path.join(
            maps_folder, f'mni152-difumo{difumokey}-continuous.json'
        )
        with open(stat_map_file_path, "r") as fp:
            stat_map_config = json.load(fp)

        stat_map_config['indices'] = {}
        for comp, name in zip(atlas_df.Component, atlas_df.Difumo_names):
            key, index = create_statisticalmap_index(comp, name)
            stat_map_config['indices'][key] = index

        with open(stat_map_file_path, "w") as fp:
            json.dump(stat_map_config, fp=fp, indent="\t")


if __name__ == "__main__":
    main()
