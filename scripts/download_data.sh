#!/usr/bin/env bash
# Fetch the BGL log dataset from LogHub on Zenodo and verify it before use.
# The dataset is not committed to this repository.
set -euo pipefail

DATA_DIR="${DATA_DIR:-data}"
ARCHIVE_URL="https://zenodo.org/records/8196385/files/BGL.zip?download=1"
ARCHIVE_NAME="BGL.zip"
LOG_NAME="BGL.log"

# Pinned checksums. The MD5 is the value Zenodo publishes for this file; the
# SHA-256 was computed from the verified download.
EXPECTED_SHA256="d67fd82a711aea0157a9b83175892c6ee60e384a2ddf5bc51f39118453816da8"
EXPECTED_BYTES="57489019"

archive_path="${DATA_DIR}/${ARCHIVE_NAME}"
log_path="${DATA_DIR}/${LOG_NAME}"

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | cut -d' ' -f1
  else
    shasum -a 256 "$1" | cut -d' ' -f1
  fi
}

mkdir -p "${DATA_DIR}"

if [[ -f "${archive_path}" ]] && [[ "$(sha256_of "${archive_path}")" == "${EXPECTED_SHA256}" ]]; then
  echo "Archive already present and verified: ${archive_path}"
else
  echo "Downloading ${ARCHIVE_NAME} (${EXPECTED_BYTES} bytes) from Zenodo..."
  curl --fail --location --progress-bar --output "${archive_path}.part" "${ARCHIVE_URL}"
  mv "${archive_path}.part" "${archive_path}"

  actual_sha256="$(sha256_of "${archive_path}")"
  if [[ "${actual_sha256}" != "${EXPECTED_SHA256}" ]]; then
    echo "Checksum mismatch for ${archive_path}" >&2
    echo "  expected ${EXPECTED_SHA256}" >&2
    echo "  actual   ${actual_sha256}" >&2
    echo "Refusing to continue. The file has been left in place for inspection." >&2
    exit 1
  fi
  echo "Checksum verified: ${actual_sha256}"
fi

if [[ -f "${log_path}" ]]; then
  echo "Already extracted: ${log_path}"
else
  echo "Extracting ${LOG_NAME}..."
  unzip -o -q "${archive_path}" -d "${DATA_DIR}"
fi

echo
echo "Ready: ${log_path}"
wc -l < "${log_path}" | xargs printf '%s lines\n'
