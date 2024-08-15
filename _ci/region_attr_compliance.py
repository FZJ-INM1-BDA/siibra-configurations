import json, os, re
from os.path import isfile

def main():

    is_json=re.compile(r'\.json$')
    path_to_parc='./parcellationschemes/'
    files = [f for f in os.listdir(path_to_parc)
        if isfile(f'{path_to_parc}{f}') and is_json.search(f) ]
    for f in files:
        with open(f'{path_to_parc}{f}') as fp:
            parc_json=json.load(fp)
            assert parc_json.get('regions') is not None
            assert type(parc_json.get('regions')) is list

if __name__ == '__main__':
    main()