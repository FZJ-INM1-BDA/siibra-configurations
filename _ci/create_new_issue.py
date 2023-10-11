import requests
import os

TITLE = "[release checklist] v???"
LABELS = ["release-checklist"]
BODY_TMPL = """see diff: https://jugit.fz-juelich.de/t.dickscheid/brainscapes-configurations/-/compare/{latest_tag}...master?from_project_id=3484&straight=false

see viewer link: https://siibra-explorer-rc.apps.jsc.hbp.eu/

Generated checklist

| name | validator | validation | 
| --- | --- | --- |
| | | |
"""

def main():
    token = os.getenv("RELEASE_CHECKLIST_CI_TOKEN")
    latest_tag = os.getenv("NEWEST_TAG")
    print("NEWEST_TAG:", latest_tag)

    v4_api = os.getenv("CI_API_V4_URL")
    ci_project_id = os.getenv("CI_PROJECT_ID")

    url = f"{v4_api}/projects/{ci_project_id}/issues"
    print("url: {url!r}")
    requests.post(url, headers={
        "PRIVATE-TOKEN": token
    }, params={
        "title": TITLE,
        "labels": ",".join(LABELS),
        "description": BODY_TMPL.format(latest_tag=latest_tag)
    })

if __name__ == "__main__":
    main()
