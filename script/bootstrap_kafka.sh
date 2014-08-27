#!/bin/bash

SCALA_VERSION=2.8.0
DOWNLOAD_URL_BASE="https://archive.apache.org/dist/kafka/"
OFFICIAL_RELEASES="0.8.0 0.8.1 0.8.1.1"

pushd .
  cd ..
  mkdir -p servers
popd

pushd .
  cd ../servers
  for kfka_version in $OFFICIAL_RELEASES; do
    if [ ! -d "$kfka_version/kafka-bin" ]; then
      curl -O https://archive.apache.org/dist/kafka/$kfka_version/kafka_${SCALA_VERSION}-${kfka_version}.tgz
    fi
  done
popd

