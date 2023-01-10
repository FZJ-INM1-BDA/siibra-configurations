import json

FILENAME = "maps/hcp32k-jba29-labelled.json"

def main():

    with open(FILENAME, "r") as fp:
        hcp = json.load(fp)
    

    hcp['volumes'] = [{
        "@type": "siibra/volume/v0.0.1",
        "providers": {
            "gii-label": {
                "left hemisphere": "https://neuroglancer.humanbrainproject.eu/precomputed/data-repo-ng-bot/20210628_julichBrainV290_freesurfer_update2/gii/2021_06_28/GapMapPublicMPMAtlas_l_N10_nlin2StdColin27_29_hcp32k_cleaned.gii",
                "right hemisphere": "https://neuroglancer.humanbrainproject.eu/precomputed/data-repo-ng-bot/20210628_julichBrainV290_freesurfer_update2/gii/2021_06_28/GapMapPublicMPMAtlas_r_N10_nlin2StdColin27_29_hcp32k_cleaned.gii"
            }
        }
    }]
    hcp['indices'] = {
        key: [{
            "label": index.get("label"),
            "fragment": "left hemisphere" if "left hemisphere" in key else "right hemisphere"
        } for index in indices]
        for key, indices in hcp['indices'].items()
    }

    with open(FILENAME, 'w') as fp:
        json.dump(hcp, fp=fp, indent="\t")
        fp.write("\n")

if __name__ == "__main__":
    main()