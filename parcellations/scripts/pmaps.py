import brainscapes as bs
from sys import argv
import json
from os import environ,path
import numpy as np
from pprint import pprint
environ['HBP_AUTH_TOKEN'] = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImJicC1vaWRjIn0.eyJleHAiOjE2MTA1NzAzNjMsInN1YiI6IjI1NTIzMCIsImF1ZCI6WyIzMjMxNDU3My1hMjQ1LTRiNWEtYjM3MS0yZjE1YWNjNzkxYmEiXSwiaXNzIjoiaHR0cHM6XC9cL3NlcnZpY2VzLmh1bWFuYnJhaW5wcm9qZWN0LmV1XC9vaWRjXC8iLCJqdGkiOiJjZGE1MTIxMi0yOTA1LTRiZDctODJiMS1jNWE4MmExN2EyMTAiLCJpYXQiOjE2MTA1NTU5NjMsImhicF9rZXkiOiJlNjI2M2IzN2I4ZGI0ZGM1ZjdjNzliYmJmMTk3ZDU3ODJjZDAyN2IxIn0.HR56tnAcnRUWi46o_b1qmCB8ZBX5KWJdCaUctTHcMPgaBchPvRF22qaYb1XCI0dVMFC0VH5E0t8taF1DglUa2OMG1kCnR_PjvSd5UojNJpanOOSOIv7Z5k6z4VUIHZT4PwTmsiFPPwCHHTFOXdVISGXoSb_O3DDwFNg0vj9SS8g"

def check_region(regiondef,parcellation):

    regiondef['maps'] = {}
    # test any children
    if 'children' in regiondef.keys():
        for subregion in regiondef['children']:
            check_region(subregion,parcellation)

    # test this region
    space_words_to_ignore = ['mni','nonlinear','asymmetric','icbm']
    hemisphere_keyword_mappings = {'left':'_l','right':'_r'}

    region = bs.region.Region(regiondef,parcellation)
    require_keywords = [ kw 
            for w,kw in hemisphere_keyword_mappings.items()
            if w in region.name ]
    files = region._related_ebrains_files()
    for fn in files:
        if ".nii" not in fn['name']:
            continue
        for space in bs.spaces:
            if any([ w.lower() not in fn['name'].lower() 
                for w in space.name.split(' ') 
                if w.lower() not in space_words_to_ignore]):
                continue
            if any([ kw.lower() not in fn['name'].lower()
                for kw in require_keywords]):
                continue
            print(region,space," -> ",fn['name'])
            if space.id in regiondef['maps'].keys(): 
                print("    - overwrites ",regiondef['maps'][space.id])
            regiondef['maps'][space.id] = fn['path']

if __name__=="__main__":

    configfile = argv[1]
    print("Trying to initialize a parcellation from file",configfile)

    with open(configfile,'r') as f:
        object_hook = bs.parcellation.Parcellation.from_json 
        parcellation = json.load( f, object_hook=object_hook)
    #    regiontree = bs.region.construct_tree(parcellation)
    #for region in regiontree.descendants:

    with open(configfile,'r') as f:
        config = json.load(f)
    for regiondef in config['regions']:
        check_region(regiondef,parcellation)

    if len(argv)>2:
        print("Writing updated json to",argv[2])
        with open(argv[2],'w') as f:
            json.dump(config,f,indent="\t")
    else:
        pprint(config)

