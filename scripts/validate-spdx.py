"""Validate the entire SPDX document using the official pinned toolchain."""
import sys
from spdx_tools.spdx.parser.parse_anything import parse_file
from spdx_tools.spdx.validation.document_validator import validate_full_spdx_document

document = parse_file(sys.argv[1])
messages = validate_full_spdx_document(document)
for message in messages:
    print(message, file=sys.stderr)
if messages:
    raise SystemExit(1)
print("official SPDX 2.3 validation passed")
