import os
import json

PATH_TO_MAP = "./maps"

def main():
    for dirpath, dirnames, filenames in os.walk(PATH_TO_MAP):
        for filename in filenames:
            full_file_name = f"{dirpath}/{filename}"
            with open(full_file_name, "r") as fp:
                map_json = json.load(fp)
            indices = map_json.pop("regions")
            for key, list_of_labels in indices.items():
                for labels in list_of_labels:
                    volume = labels.pop("map")
                    label = labels.pop("index")
                    labels['volume'] = volume
                    labels['label'] = label
            map_json['indices'] = indices
            with open(full_file_name, "w") as fp:
                json.dump(map_json, indent="\t", fp=fp)
                fp.write("\n")
        


if __name__ == "__main__":
    main()