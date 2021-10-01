from genericpath import isfile
import os
import re
import json
directories = ['spaces', 'parcellations', 'atlases']

def validate_json(obj):
    datasets=obj.get('datasets', [])
    obj_name=obj.get("name")

    simple_origin_type='fzj/tmp/simpleOriginInfo/v0.0.1'

    for ds in datasets:
        ds_type = ds.get('@type')
        if ds_type == simple_origin_type:
            # seems name is needed
            # https://github.com/FZJ-INM1-BDA/siibra-python/blob/5cace7a/siibra/core/datasets.py#L94-L101
            assert ds.get('name') is not None, f'for [{obj_name}] datasets, expecting all dataset with type [{simple_origin_type}] to have a name attribute. But one of them does not'

def main():
    for dir in directories:
        for f in [file for file in os.listdir(dir) if os.path.isfile(f'{dir}/{file}') and re.search(r'\.json$', file) ]:
            with open(f'{dir}/{f}', 'r') as fp:
                validate_json(json.load(fp))

if __name__ == '__main__':
    main()