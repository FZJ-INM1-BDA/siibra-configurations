import json, os, re
from os.path import isfile

version_collection_key='collectionName'
version_key='@version'
version_prev_key='@prev'
version_next_key='@next'

def main():
    parc_dict={}

    is_json=re.compile(r'\.json$')
    path_to_parc='./parcellations/'
    files = [f for f in os.listdir(path_to_parc)
        if isfile(f'{path_to_parc}{f}') and is_json.search(f) ]
    for f in files:
        with open(f'{path_to_parc}{f}') as fp:
            parc_json=json.load(fp)
            assert parc_json.get('@id') is not None, 'parcellation.@id must be populated'
            parc_dict[parc_json.get('@id')] = parc_json

    name_re=re.compile(r'^[0-9.]+$')
    for parc_id in parc_dict.keys():
        version=parc_dict[parc_id].get(version_key)
        parc_name=parc_dict[parc_id].get('name', 'Unnamed parcellation')
        if version is not None:

            # regex test name
            assert version.get('name') is not None, f'for {parc_name}, parcellation.{version_key}.name must be populated'
            assert name_re.search(version.get('name')) is not None, f'for {parc_name}, parcellation.{version_key}.name must be in the format of ^[0-9.]+$'

            # collectionName
            assert version.get(version_collection_key) is not None, f'for {parc_name}, parcellation.{version_key}.{version_collection_key} needs to be defined for {parc_name}'

            # test next
            next_id=version.get(version_next_key)
            if next_id is not None:
                assert parc_dict.get(next_id) is not None, f'for {parc_name}, parcellation.{version_key}.{version_next_key} defined as {next_id}, but cannot be found.'
                next_parc=parc_dict.get(next_id)
                next_parc_version=next_parc.get(version_key)
                assert next_parc_version is not None, f'for {parc_name}, parcellation.{version_key}.{version_next_key} defined as {next_id}, but the parcellation referenced does not have {version_key} defined'
                next_parc_version_prev_id=next_parc_version.get(version_prev_key)
                assert next_parc_version_prev_id == parc_id, f'for {parc_name}, parcellation.{version_key}.{version_next_key} defined as {next_id}, but the parcellation referenced has {version_key}.{version_prev_key} defined as {next_parc_version_prev_id}, but expecting {parc_id}'

                # sanity check on collectionName
                this_collection_name=version.get(version_collection_key)
                next_parc_collection_name=next_parc_version.get(version_collection_key)
                assert this_collection_name == next_parc_collection_name, f'for {parc_name}, parcellation.{version_key}.{version_collection_key} defined as {this_collection_name}, but the parcellation referenced from {version_key}.{version_next_key} has {version_key}.collectionName defined as {next_parc_collection_name}'

            # test prev
            prev_id=version.get(version_prev_key)
            if prev_id is not None:
                assert parc_dict.get(prev_id) is not None, f'for {parc_name}, parcellation.{version_key}.{version_prev_key} defined as {prev_id}, but cannot be found.'
                prev_parc=parc_dict.get(prev_id)
                prev_parc_version=prev_parc.get(version_key)
                assert prev_parc_version is not None, f'for {parc_name}, parcellation.{version_key}.{version_prev_key} defined as {prev_id}, but the parcellation referenced does not have {version_key} defined'
                prev_parc_version_prev_id=prev_parc_version.get(version_next_key)
                assert prev_parc_version_prev_id == parc_id, f'for {parc_name}, parcellation.{version_key}.{version_prev_key} defined as {prev_id}, but the parcellation referenced has {version_key}.{version_prev_key} defined as {prev_parc_version_prev_id}, but expecting {parc_id}'

                # sanity check on collectionName
                this_collection_name=version.get(version_collection_key)
                prev_parc_collection_name=prev_parc_version.get(version_collection_key)
                assert this_collection_name == prev_parc_collection_name, f'for {parc_name}, parcellation.{version_key}.{version_collection_key} defined as {this_collection_name}, but the parcellation referenced from {version_key}.{version_prev_key} has {version_key}.collectionName defined as {prev_parc_collection_name}'

if __name__ == '__main__':
    main()