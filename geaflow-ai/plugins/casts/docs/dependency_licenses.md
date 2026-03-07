# Third-Party Licenses (CASTS)

This document lists direct dependencies from `pyproject.toml` and their
license information, based on locally available package metadata. Review and
verify before release.

## Runtime Dependencies

| Package | Version Spec | License (from metadata) | Notes |
| --- | --- | --- | --- |
| openai | >=1.86.0 | Apache Software License | Verify SPDX: Apache-2.0 |
| numpy | >=2.0.0 | BSD License | Includes bundled components (see package license) |
| matplotlib | >=3.8.0 | Python Software Foundation License | Verify full license text |
| networkx | >=3.2.0 | UNKNOWN | Verify from project license |
| python-dotenv | >=0.21.0 | UNKNOWN | Verify from project license |
| pytest | >=8.4.0 | MIT License | Runtime dependency |
| mypy | >=1.19.1 | MIT License | Runtime dependency |
| types-networkx | >=3.6.1.20251220 | UNKNOWN | Verify from project license |
| ruff | >=0.14.9 | MIT License | Runtime dependency |

## Optional Dependencies

### dev

| Package | Version Spec | License (from metadata) | Notes |
| --- | --- | --- | --- |
| pytest | >=8.4.0 | MIT License | |
| ruff | >=0.11.13 | MIT License | |
| mypy | >=1.18.1 | MIT License | |

### service

| Package | Version Spec | License (from metadata) | Notes |
| --- | --- | --- | --- |
| flask | ==3.1.1 | UNKNOWN | Verify from project license |
| flask-sqlalchemy | ==3.1.1 | BSD License | |
| flask-cors | ==6.0.1 | UNKNOWN | Verify from project license |

### test

| Package | Version Spec | License (from metadata) | Notes |
| --- | --- | --- | --- |
| pytest | ==8.4.0 | MIT License | |
| pytest-cov | ==6.2.1 | NOT INSTALLED | Verify from project license |
| pytest-mock | >=3.14.1 | NOT INSTALLED | Verify from project license |
| pytest-asyncio | >=0.24.0 | UNKNOWN | Verify from project license |

## Notes

- License values above are derived from local package metadata when available.
- For UNKNOWN/NOT INSTALLED entries, confirm license information before release.
