#!/usr/bin/env python3
"""Write _build_config.py from build-time env vars. Run in CI before `python -m build`.

ALL_DATA_API_URL is required. OIDC_ISSUER / OIDC_CLIENT_ID are optional — set them
to bake in `biohub-data login` for an Okta-gated deployment; omit them for a public
one (login stays unconfigured and downloads run anonymously).
"""

import os
import sys
from pathlib import Path

OUT = Path("src/biohub_data_cli/_build_config.py")


def main() -> None:
    url = os.environ.get("ALL_DATA_API_URL", "").strip()
    if not url:
        sys.exit("ALL_DATA_API_URL is not set")
    if not url.startswith(("http://", "https://")):
        sys.exit(f"ALL_DATA_API_URL must start with http:// or https://: {url!r}")

    lines = [f"ALL_DATA_API_URL = {url!r}\n"]

    issuer = os.environ.get("OIDC_ISSUER", "").strip()
    client_id = os.environ.get("OIDC_CLIENT_ID", "").strip()
    if issuer and not issuer.startswith(("http://", "https://")):
        sys.exit(f"OIDC_ISSUER must start with http:// or https://: {issuer!r}")
    # Issuer and client id are only useful together; require both or neither.
    if bool(issuer) != bool(client_id):
        sys.exit("Set both OIDC_ISSUER and OIDC_CLIENT_ID, or neither")
    if issuer:
        lines.append(f"OIDC_ISSUER = {issuer!r}\n")
        lines.append(f"OIDC_CLIENT_ID = {client_id!r}\n")

    OUT.write_text("".join(lines))
    print(
        f"Wrote {OUT} with ALL_DATA_API_URL={url}"
        + (f", OIDC_ISSUER={issuer}" if issuer else "")
    )


if __name__ == "__main__":
    main()
