#!/usr/bin/env bash

set -euo pipefail

CYCLONEDX_VERSION="2.8.1"
OUTPUT_DIR="$(pwd)/sbom-output"
MERGED_SBOM="${OUTPUT_DIR}/merged-bom.json"

mkdir -p "${OUTPUT_DIR}"

echo "Starting SBOM generation..."
echo "Output directory: ${OUTPUT_DIR}"
echo

SBOM_FILES=()

for module in */ ; do
  if [[ -f "${module}/pom.xml" ]]; then
    echo "Processing module: ${module}"

    pushd "${module}" > /dev/null

    mvn -q \
      org.cyclonedx:cyclonedx-maven-plugin:${CYCLONEDX_VERSION}:makeBom \
      -DskipTests

    if [[ -f "target/bom.json" ]]; then
      MODULE_NAME=$(basename "${module}")
      DEST="${OUTPUT_DIR}/${MODULE_NAME}-bom.json"
      cp target/bom.json "${DEST}"
      SBOM_FILES+=("${DEST}")
      echo "  SBOM generated: ${DEST}"
    else
      echo "  WARNING: No SBOM generated for ${module}"
    fi

    popd > /dev/null
    echo
  fi
done

if [[ ${#SBOM_FILES[@]} -eq 0 ]]; then
  echo "ERROR: No SBOMs were generated."
  exit 1
fi

echo "Merging SBOMs..."
cyclonedx merge \
  --input-files "${SBOM_FILES[@]}" \
  --output-file "${MERGED_SBOM}" \
  --output-format json

echo
echo "Merged SBOM created at:"
echo "  ${MERGED_SBOM}"

