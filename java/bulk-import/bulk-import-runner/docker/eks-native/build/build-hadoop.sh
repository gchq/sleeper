#!/bin/bash -e
# Copyright 2022-2024 Crown Copyright
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

apt-get -qq update
apt-get -qq install -y \
            curl \
            automake \
            build-essential \
            cmake \
            g++ \
            git \
            libbz2-dev \
            libsnappy-dev \
            libsasl2-dev \
            libssl-dev \
            libtool \
            libzstd-dev \
            pkg-config \
            nasm \
            zlib1g-dev

PROTOBUF_VERSION=2.5.0

curl -Ls https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOBUF_VERSION}/protobuf-${PROTOBUF_VERSION}.tar.gz | tar -xz
cd protobuf-${PROTOBUF_VERSION}
./configure --prefix=/opt/protobuf/
make
make install

cd ..
rm -rf protobuf-${PROTOBUF_VERSION}*
export PROTOBUF_HOME=/opt/protobuf
export PATH=${PROTOBUF_HOME}/bin:${PATH}

git clone https://github.com/01org/isa-l.git
cd isa-l
./autogen.sh
./configure
make
make install
cd ..
rm -rf isa-l  

git clone https://github.com/apache/hadoop.git
cd hadoop
git checkout rel/release-${HADOOP_VERSION}

mvn install \
    -P dist,native \
    -Dtar \
    -Drequire.bzip2 \
    -Drequire.snappy \
    -Drequire.zstd \
    -Drequire.openssl \
    -Drequire.isal \
    -Disal.prefix=/usr \
    -Disal.lib=/usr/lib \
    -Dbundle.isal=true \
    -DskipTests
   
