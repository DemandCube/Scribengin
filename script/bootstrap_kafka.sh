#!/bin/bash

SCALA_VERSION=2.8.0
DOWNLOAD_URL_BASE="https://archive.apache.org/dist/kafka/"
OFFICIAL_RELEASES="0.8.0 0.8.1 0.8.1.1"
TARGET_DIR=$1

mkdir -p $PWD/$TARGET_DIR

for kfka_version in $OFFICIAL_RELEASES; do
  if [ ! -d "$PWD/$TARGET_DIR/$kfka_version/kafka-bin" ]; then
    echo $kfka_version
    scala_kafka_version=kafka_${SCALA_VERSION}-${kfka_version}
    if [ $kfka_version = "0.8.0" ]; then
      kafka_download_file=$scala_kafka_version.tar.gz
    else
      kafka_download_file=$scala_kafka_version.tgz
    fi

    echo "curl -o $TARGET_DIR/$kafka_download_file https://archive.apache.org/dist/kafka/$kfka_version/$kafka_download_file"
    curl -o $PWD/$TARGET_DIR/$kafka_download_file https://archive.apache.org/dist/kafka/$kfka_version/$kafka_download_file
    tar xvzf $PWD/$TARGET_DIR/$kafka_download_file -C $PWD/$TARGET_DIR/$kfka_version
    echo "mv $PWD/$TARGET_DIR/$kfka_version/$scala_kafka_version $PWD/$TARGET_DIR/$kfka_version/kafka-bin"
  else
    echo "skip $kafka_version"
  fi
done


