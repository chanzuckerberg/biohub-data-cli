#!/usr/bin/env python3
"""Write _build_config.py from $OPS_SERVICE_URL. Run in CI before `python -m build`."""

import os
import sys
from pathlib import Path

OUT = Path("src/biohub_data_cli/_build_config.py")


def main() -> None:
    url = os.environ.get("OPS_SERVICE_URL", "").strip()
    if not url:
        sys.exit("OPS_SERVICE_URL is not set")
    if not url.startswith(("http://", "https://")):
        sys.exit(f"OPS_SERVICE_URL must start with http:// or https://: {url!r}")
    OUT.write_text(f"OPS_SERVICE_URL = {url!r}\n")
    print(f"Wrote {OUT} with OPS_SERVICE_URL={url}")


if __name__ == "__main__":
    main()
