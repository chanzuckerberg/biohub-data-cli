# set an env var that takes in mocked collections
import json
import os
from pathlib import Path

FIXTURES = Path("/content/fixtures")
FIXTURES.mkdir(exist_ok=True)

(FIXTURES / "3b32c693-a8b6-46d2-8f42-d4511a07f2f7.json").write_text(
    json.dumps(
        {
            "id": "3b32c693-a8b6-46d2-8f42-d4511a07f2f7",
            "slug": "aconcagua",
            "title": "aconcagua",
            "datasets": [
                {
                    "id": "3b32c693-a8b6-46d2-8f42-d4511a07f2f7",
                    "slug": "aconcagua_dataset",
                    "title": "aconcagua",
                    "file_format": "zarr_v3",
                    "urls": ["s3://ops-explorer-public/aconcagua/aconcagua"],
                }
            ],
        }
    )
)

(FIXTURES / "6a3f8b91-1c5e-4d3a-9b4c-f7e0a2d8b6f3.json").write_text(
    json.dumps(
        {
            "id": "6a3f8b91-1c5e-4d3a-9b4c-f7e0a2d8b6f3",
            "slug": "leonetti",
            "title": "Leonetti — OPS atlas",
            "datasets": [
                {
                    "id": "f4a2e5c8-9b1d-4e3f-a7c6-3d8e9f1a2b5c",
                    "slug": "ops-atlas",
                    "title": "Leonetti OPS atlas — CellProfiler features",
                    "file_format": "zarr_v3",
                    "urls": [
                        "s3://ops-explorer-public/leonetti_ops/ops_data_portal_submission/atlas_reformatted"
                    ],
                },
                {
                    "id": "7006c6c0-6487-43c5-b6ca-735acebb4375",
                    "slug": "cropseq-pseudobulk",
                    "title": "CROP-seq Transcriptome Pseudobulk",
                    "file_format": "h5ad",
                    "urls": [
                        "s3://cellxstate-data-dev/sources/CropSeq_June2025_perturbation_lvl_ops_2026_05_07.h5ad"
                    ],
                },
            ],
        }
    )
)

os.environ["DATA_CLI_FIXTURES_DIR"] = str(FIXTURES)
