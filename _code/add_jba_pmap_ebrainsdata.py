import siibra
import json
import os
from siibra.retrieval.requests import EbrainsRequest, DECODERS
from typing import Tuple, List, Dict

LOCAL_CONFIG_FOLDER = "./"
BASE_URL = "https://core.kg.ebrains.eu/v3-beta/queries/"

JBAPROJECT_QUERYID = 'cc81c899-9ca1-488e-adc5-5034c1b78092'
TERMINOLOGY_QUERYID = 'ef662398-8016-42a2-a4ed-e10b2535b3f6'

# spaces and jba parcellations in siibra
SUPPORTED_SPACES = ['Colin27', 'MNI152']
SUPPORTED_PARCELATIONS = ['julich 1.18', 'julich 2.9', 'julich 3.0.2']


def get_UUID(kg_id: str):
    return kg_id.removeprefix('https://kg.ebrains.eu/api/instances/')


def search_datasets(
        query_id: str,
        param: Tuple[str, str] = None,
        in_progress=False,
):
    """
    Search for datasets hosted in EBRAINS.

    Parameters
    ----------
    query_id: str
    param: Tuple[str, str]
        0: for the parameter name, and 1: paramter value
    in_progress: bool, default=False,

    Returns
    -------
    List[Dict]: List dataset metadata matching the query parameters.

    Note
    ----
    Requires ebrains token: `siibra.fetch_ebrains_token()`
    """
    stage = "IN_PROGRESS" if in_progress else "RELEASED"
    url = f"{BASE_URL}{query_id}/instances?stage={stage}"
    if param is not None:
        url += f"&{param[0]}={param[1]}"

    response = EbrainsRequest(url, DECODERS[".json"]).get()
    return response.get('data', [])


def get_jba_terminology():
    terminology_query_result = search_datasets(query_id=TERMINOLOGY_QUERYID)
    map_terminology_versions = terminology_query_result[0]["hasVersion"]
    print("The following versions have been found in KG:")
    for ver in map_terminology_versions:
        print(
            ver.get("versionIdentifier"),
            '- Number of regions: ', len(ver["hasTerminology"][0]["hasEntity"])
        )
    return terminology_query_result, map_terminology_versions


def get_entity_dataset_table(blacklist: List[str]):
    julichbrain_all_datasets = search_datasets(query_id=JBAPROJECT_QUERYID)
    global_entity_dataset_table = {}
    for ds in julichbrain_all_datasets:
        if any(word.lower() in ds['fullName'].lower() for word in blacklist):
            continue
        ds_versions = ds['hasVersion']
        ds_entities = set()
        for ds_v in ds_versions:
            study_targets = ds_v['studyTarget']
            if len(study_targets) == 0:
                print('No study targets found for:', ds_v)
                continue
            for target in study_targets:
                name = target['name']
                lookuplabel = target['lookupLabel']
                if lookuplabel is None:
                    lookuplabel = name
                ds_entities.add(lookuplabel)
            if len(ds_entities) > 1:
                print("Dataset with multiple enitites found. Entities:")
                print(ds_entities)

            global_entity_dataset_table[
                list(ds_entities)[0].removeprefix("JBA_")
            ] = {
                'name': name,
                'lookuplabel': lookuplabel,
                'entity_id': target['id'],
                'DatasetVersionID': {ds_v['versionIdentifier']: ds_v['id']},
                'DatasetName': ds['fullName']
            }
    return global_entity_dataset_table


def connect_terminology_version_and_datasets(
        map_versionIdentifier: str,
        map_terminology_version: List,
        global_entity_dataset_table: Dict
):
    mapversion_entities = \
        map_terminology_version["hasTerminology"][0]["hasEntity"]
    connection_json = {}
    for entity_in_map in mapversion_entities:
        lookup_label = clean_version_lookup_label(
            entity_in_map.get('lookupLabel'), map_versionIdentifier
        )
        entity_dataset_version = global_entity_dataset_table.get(lookup_label)
        if entity_dataset_version is not None:
            pmap_version = clean_pmap_version(
                entity_in_map.get("versionIdentifier")
            )
            # get dataset version UUID
            try:
                dataset_UUID = get_UUID(
                    entity_dataset_version['DatasetVersionID'][pmap_version]
                )
                versionMatchesKey = True
            except Exception:
                DatasetVersionID = \
                    entity_dataset_version.get('DatasetVersionID')
                if DatasetVersionID is None:
                    print(
                        f"Cannot find dataset version id for: {entity_in_map}"
                    )
                    continue
                versionid = DatasetVersionID.get('v1')
                if versionid is None:
                    print(f"Dataset version key issue: {DatasetVersionID}")
                    continue
                dataset_UUID = get_UUID(versionid)
                versionMatchesKey = False

            connection_json[entity_in_map.get('name')] = {
                "EntityID": get_UUID(entity_in_map.get('id')),
                "DatasetName": entity_dataset_version['DatasetName'],
                "DatasetVersion": dataset_UUID,
                "PMapVersion": pmap_version,
                "versionMatchesKey": versionMatchesKey
            }
        else:
            print(f"Could not find: {lookup_label} in {map_versionIdentifier}")

    return connection_json


def clean_version_lookup_label(lookuplabel: str, map_versionIdentifier: str):
    lookuplabel = lookuplabel.removeprefix(
        "JBA_" + map_versionIdentifier.replace(', ', '-')
    ).replace('_', " ")
    try:
        lookuplabel = lookuplabel[:lookuplabel.index("PM")].strip()
    except Exception:
        return None
    return lookuplabel


def clean_pmap_version(versionIdentifier: str):
    return versionIdentifier[versionIdentifier.index("PM-")+3:]


def create_terminology_master(terminology_query_result):
    terminology_entities = terminology_query_result[0].get(
        "hasTerminology"
    )[0]["hasEntity"]

    for entity in terminology_entities:
        entity['entity_id'] = get_UUID(entity.pop('id'))
        for ep in entity.get('hasParent'):
            ep['entity_id'] = get_UUID(ep.pop('id'))
        for ev in entity.get('hasVersion'):
            ev['entity_id'] = get_UUID(ev.pop('id'))

    with open(f"{LOCAL_CONFIG_FOLDER}julich_brain_terminolgy.json", "w") as fp:
        json.dump(terminology_entities, fp, indent="\t")


def get_map_key(mp: siibra._parcellationmap.Map):
    if mp.is_labelled:  # accept only sparsemaps
        return None
    # only supported spaces and parcellations
    space_i = [
        i for i, s in enumerate(SUPPORTED_SPACES) if mp.space.matches(s)
    ]
    if len(space_i) == 0:
        return None
    parcellation_i = [
        i for i, p in enumerate(SUPPORTED_PARCELATIONS)
        if mp.parcellation.matches(p)
    ]
    if len(parcellation_i) == 0:
        return None

    # go through each region listed in KG queried map data
    return f"v{mp.parcellation.version}, {SUPPORTED_SPACES[space_i[0]]}"


def update_volume_ebrains_field(
        mp: siibra._parcellationmap.Map,
        volumes: Dict,
        connection_data
):
    for region_name, vals in connection_data.items():
        # find the index of the KG defined region in siibra preconfig
        indices = mp.find_indices(region_name)  # should find L and R
        if len(indices) == 0:
            continue
        if len(indices) > 2:
            print(f'Cannot pinpoint index for {region_name}:\n', indices)
            continue
        # for each hemisphere
        for index in indices:
            # add ebrains info
            if volumes[index.volume].get('ebrains') is None:
                volumes[index.volume]['ebrains'] = {}
            volumes[index.volume]['ebrains']["openminds/DatasetVersion"] \
                = vals['DatasetVersion']
            # volumes[index.volume]['ebrains']["PMapVersion"] = vals['PMapVersion']
            # volumes[index.volume]['ebrains']["DatasetName"] = vals['DatasetName']
            # volumes[index.volume]['ebrains']["EntityID"] = vals['EntityID']

            # check if the version in the url matches the pmap datasetVer
            url = volumes[index.volume]["providers"]["nii"]
            i = url.index('_public')
            url_version = f"v{url[i-url[:i][::-1].index('_'):i]}"
            if url_version != str(vals['PMapVersion']):
                print(f"Flagged: {mp.name} - {region_name} - {index}")
                # volumes[index.volume]['ebrains']["VersionUrlMismatch"] = True
    return volumes


def main():
    siibra.use_configuration(LOCAL_CONFIG_FOLDER)
    siibra.fetch_ebrains_token()

    terminology_query_result, map_terminology_versions = get_jba_terminology()
    create_terminology_master(terminology_query_result)

    blacklist = [
        'Ultrahigh',
        'BigBrain',
        'Connectivity',
        'Whole-brain parcellation of the Julich-Brain Cytoarchitectonic Atlas'
    ]
    global_entity_dataset_table = get_entity_dataset_table(blacklist)

    connection_data = {}
    for map_term_ver in map_terminology_versions:
        map_key = map_term_ver.get("versionIdentifier")
        connection_data[map_key] = connect_terminology_version_and_datasets(
            map_key, map_term_ver, global_entity_dataset_table
        )

    # go through each map json
    maps_folder = f"{LOCAL_CONFIG_FOLDER}/maps"
    for file in os.listdir(maps_folder):
        file_path = os.path.join(maps_folder, file)
        with open(file_path, "r") as fp:
            config = json.load(fp)

        config['filename'] = file
        mp = siibra.from_json(config)
        # filename is required to build a map but should not be in
        # the configuration
        config.pop('filename')

        # get existing volume information in the siibra map preconfig
        map_key = get_map_key(mp)
        if map_key is None:
            continue
        siibra.commons.logger.info(map_key)
        config["volumes"] = update_volume_ebrains_field(
            mp, config["volumes"], connection_data[map_key]
        )

        # write the data over existing local config
        with open(file_path, "w") as fp:
            json.dump(config, fp, indent="\t")


if __name__ == "__main__":
    main()
