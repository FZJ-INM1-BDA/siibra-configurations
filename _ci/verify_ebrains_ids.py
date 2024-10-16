from dataclasses import dataclass
from urllib.parse import quote
import requests
import os
import json
from typing import Union, Dict, List

try:
    import tqdm

    tqdm_imported = True
except ImportError:
    tqdm_imported = False

PER_PAGE = 50
FAIL_FAST = True


@dataclass
class EbrainsIdValidationResult:
    valid: bool
    key: str

    value: str
    filename: str = None


@dataclass
class GitlabConnector:
    host: str
    project: int
    ref: str = "master"

    def file(self, path: str):
        quoted_path = quote(path, safe="")
        resp = requests.get(
            f"{self.host}/api/v4/projects/{self.project}/respository/files/{quoted_path}/raw?ref={self.ref}"
        )
        resp.raise_for_status()
        return resp.text()

    def ls(self):
        page = 1
        progress = None
        while True:
            resp = requests.get(
                f"{self.host}/api/v4/projects/{self.project}/repository/tree?ref={self.ref}&per_page={PER_PAGE}&page={page}&recursive=true"
            )
            resp.raise_for_status()
            page = page + 1
            arr = resp.json()

            if tqdm_imported:
                if not progress:
                    progress = tqdm.tqdm(
                        total=int(resp.headers.get("x-total-pages")),
                        leave=True,
                        unit="files",
                    )
                progress.update(1)
            if len(arr) == 0:
                break
            for item in arr:
                if item.get("type") == "blob":
                    yield item
        return

    def files(self):
        """
        Get all files as a list.
        For larger repositories, could lead to memory issues.
        """
        return [f for f in self.ls()]


SNAPSHOT_GITLABS = [
    GitlabConnector(host="https://jugit.fz-juelich.de", project=7846),
    GitlabConnector(host="https://gitlab.ebrains.eu", project=421),
]

all_files: List[str] = []


def validate_ebrains_dict(obj: Dict) -> List[EbrainsIdValidationResult]:
    assert isinstance(obj, dict)
    return_result: List[EbrainsIdValidationResult] = []
    for key in obj:
        valid_flag = None
        if key == "openminds/Dataset":
            valid_flag = f"ebrainsquery/v3/Dataset/{obj[key]}.json" in all_files
        if key == "openminds/DatasetVersion":
            valid_flag = f"ebrainsquery/v3/DatasetVersion/{obj[key]}.json" in all_files
        if key == "minds/core/dataset/v1.0.0":
            valid_flag = f"ebrainsquery/v1/dataset/{obj[key]}.json" in all_files
        if FAIL_FAST and valid_flag is False:
            raise Exception(f"key={key},value={obj[key]}")
        if valid_flag is not None:
            return_result.append(
                EbrainsIdValidationResult(valid=valid_flag, key=key, value=obj[key])
            )
    return return_result


def traverse(obj: Union[Dict, List, str]) -> List[EbrainsIdValidationResult]:

    if isinstance(obj, dict):
        if "ebrains" in obj:
            return validate_ebrains_dict(obj["ebrains"])

        return [err for value in obj.values() for err in traverse(value)]
    if isinstance(obj, list):
        return [err for item in obj for err in traverse(item)]

    if isinstance(obj, (str, int, float, type(None))):
        return []

    raise Exception(f"Cannot traverse {obj.__class__.__name__}")


def main():
    global all_files

    print("Retrieving all files...")
    for gitlab in SNAPSHOT_GITLABS:
        try:
            all_files = [f.get("path") for f in gitlab.files()]
            break
        except Exception:
            pass

    print("Traversing files...")
    all_results: List[EbrainsIdValidationResult] = []
    for dirpath, dirnames, filenames in os.walk("."):
        for filename in filenames:
            if not filename.endswith(".json"):
                continue
            full_filename = f"{dirpath}/{filename}"
            with open(full_filename, "r") as fp:
                validation_results = traverse(json.load(fp))

                for result in validation_results:
                    result.filename = full_filename
                all_results.extend(validation_results)

    failed = [r for r in all_results if not r.valid]
    succeeded = [r for r in all_results if r.valid]
    print(
        f"Scanned {len(all_results)}, Failed: {len(failed)}, Succeeded: {len(succeeded)}"
    )
    if len(failed) > 0:
        raise Exception(
            "\n".join(
                [f"filename={f.filename}, key={f.key}, value={f.value}" for f in failed]
            )
        )


if __name__ == "__main__":
    main()
