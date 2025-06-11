import json
import os

# duplicated keys results in overwriting of key values
# even though they are permitted by json spec, they should be avoided, as omitted key values can lead to unstable behaviour
# also checks that json are valid


class DuplicatedKeyError(Exception):
    pass


expected_exceptions = (
    json.JSONDecodeError,
    DuplicatedKeyError,
)


def check_for_duplicated_hook(tuplets):
    key_count = {
        tuplet[0]: len([True for _tuplet in tuplets if _tuplet[0] == tuplet[0]])
        for tuplet in tuplets
    }

    duplicated_key_count = {key: count for key, count in key_count.items() if count > 1}
    if len(duplicated_key_count) > 0:
        raise DuplicatedKeyError(
            f"Duplicated keys found! {', '.join(f'key: {key}: count {count}' for key, count in duplicated_key_count.items())}"
        )
    return dict


def main():
    errors = []
    for dirpath, dirnames, filenames in os.walk("."):
        for filename in filenames:
            if not filename.endswith(".json"):
                continue
            full_filename = f"{dirpath}/{filename}"
            with open(full_filename, "r") as fp:
                try:
                    json.load(fp, object_pairs_hook=check_for_duplicated_hook)
                except json.JSONDecodeError as e:
                    errors.append(
                        f"error decoding {full_filename}: JSONDecodeError: {str(e)}"
                    )
                except DuplicatedKeyError as e:
                    errors.append(
                        f"error decoding {full_filename}: DuplicatedKeyError: {str(e)}"
                    )
                except Exception as e:
                    errors.append(
                        f"error decoding {full_filename}: Exception: {str(e)}"
                    )
    if len(errors):
        raise ValueError("\n".join(errors))


if __name__ == "__main__":
    main()
