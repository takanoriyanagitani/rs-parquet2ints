#!/bin/bash

set -u

bin="./target/release/rs-parquet2ints"

ischema="./sample.d/psch.txt"
icsv="./sample.d/input.csv"
iparquet="./sample.d/input.parquet"

export ENV_PARQUET_NAME="${iparquet}"
export ENV_COL_INDEX=0
export ENV_BATCH_SIZE=8192
export ENV_ENDIAN=little

geninput(){
  echo generating the input file...

  mkdir -p "./sample.d"
  parquet-fromcsv \
    --has-header \
    --schema "${ischema}" \
    --input-file "${icsv}" \
    --output-file "${iparquet}"
}

run_native(){
  "${bin}"
}

test -f "${iparquet}" || geninput

run_native |
  od \
    -t d4 \
    -An
