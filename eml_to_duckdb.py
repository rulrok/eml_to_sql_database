from pathlib import Path
import dlt
import logging
from typing import Iterable
from parser import parse_eml, EmailRecord

@dlt.source(name="eml")
def eml_source(eml_dir: str):
    eml_dir_path = Path(eml_dir)

    @dlt.resource(name="messages", write_disposition="merge", primary_key="message-id")
    def messages() -> Iterable[EmailRecord]:
        for path in eml_dir_path.rglob("*.eml"):
            result = parse_eml(path)
            if result:
                yield result
            else:
                logging.warning(f"Failed to parse EML file: {path}")

    return (messages,)

def main() -> None:

    pipeline = dlt.pipeline(
        pipeline_name="emails",
        destination="duckdb",
        dataset_name="emails"
    )

    source = eml_source("eml_files")

    info = pipeline.run(source, destination='duckdb')
    print("Load completed:")
    print(info)


if __name__ == "__main__":
    main()
