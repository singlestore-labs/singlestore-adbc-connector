#!/usr/bin/env bash
# Copyright (c) 2026 SingleStore, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eu

# this script must be run from the top-level of the repo
cd "$(git rev-parse --show-toplevel)"


DEFAULT_SINGLESTORE_VERSION=""
VERSION="${SINGLESTORE_VERSION:-$DEFAULT_SINGLESTORE_VERSION}"
IMAGE_NAME="ghcr.io/singlestore-labs/singlestoredb-dev:latest"
CONTAINER_NAME="singlestore-integration"
SSL_DIR="${PWD}/ssl"

rm -rf "${SSL_DIR}"
mkdir -p "${SSL_DIR}"

echo "Create a Certificate Authority (CA)"

openssl genpkey -algorithm RSA -out "${SSL_DIR}"/ca-key.pem
openssl req -new -x509 -key  "${SSL_DIR}"/ca-key.pem -out "${SSL_DIR}"/ca-cert.pem -days 365 -subj "/CN=SingleStoreDBCA"

echo "Generate the Server Certificate"

openssl genpkey -algorithm RSA -out "${SSL_DIR}"/server-key.pem
openssl req -new -key "${SSL_DIR}"/server-key.pem -out "${SSL_DIR}"/server-req.csr -subj "/CN=singlestore-server"
openssl x509 -req -in "${SSL_DIR}"/server-req.csr -CA "${SSL_DIR}"/ca-cert.pem -CAkey "${SSL_DIR}"/ca-key.pem -CAcreateserial -out "${SSL_DIR}"/server-cert.pem -days 365

echo "Create truststore"
keytool -import -trustcacerts -file "${SSL_DIR}"/ca-cert.pem -keystore "${SSL_DIR}"/truststore.jks -storepass password -alias singlestore-ca -noprompt

echo "Generate the Client Certificate"

openssl genpkey -algorithm RSA -out "${SSL_DIR}"/client-key.pem
openssl req -new -key "${SSL_DIR}"/client-key.pem -out "${SSL_DIR}"/client-req.csr -subj "/CN=singlestore-client"
openssl x509 -req -in "${SSL_DIR}"/client-req.csr -CA "${SSL_DIR}"/ca-cert.pem -CAkey "${SSL_DIR}"/ca-key.pem -CAcreateserial -out "${SSL_DIR}"/client-cert.pem -days 365

echo "Create keystore"
openssl pkcs12 -export -inkey "${SSL_DIR}"/client-key.pem -in "${SSL_DIR}"/client-cert.pem -out "${SSL_DIR}"/client-keystore.p12 -name client-cert -CAfile "${SSL_DIR}"/ca-cert.pem -caname root -passout pass:password

chmod -R 777 "${SSL_DIR}"

EXISTS=$(docker inspect ${CONTAINER_NAME} >/dev/null 2>&1 && echo 1 || echo 0)

if [[ "${EXISTS}" -eq 1 ]]; then
  echo "Removing existing container ${CONTAINER_NAME}"
  docker rm -f ${CONTAINER_NAME}
fi

docker run -d \
    --name ${CONTAINER_NAME} \
    -v ${PWD}/ssl:/test-ssl \
    -e ROOT_PASSWORD=${SINGLESTORE_PASSWORD} \
    -e SINGLESTORE_VERSION=${SINGLESTORE_VERSION} \
    -p ${SINGLESTORE_PORT}:3306 \
    ${IMAGE_NAME}

singlestore-wait-start() {
  echo -n "Waiting for SingleStore to start..."
  while true; do
      if mysql -u root -h ${SINGLESTORE_HOST} -P ${SINGLESTORE_PORT} -p"${SINGLESTORE_PASSWORD}" -e "select 1" >/dev/null 2>/dev/null; then
          break
      fi
      echo -n "."
      sleep 0.2
  done
  echo ". Success!"
}

singlestore-wait-start

echo
echo "Setting up SSL"
docker exec ${CONTAINER_NAME} memsqlctl update-config --yes --all --key ssl_ca --value /test-ssl/ca-cert.pem
docker exec ${CONTAINER_NAME} memsqlctl update-config --yes --all --key ssl_cert --value /test-ssl/server-cert.pem
docker exec ${CONTAINER_NAME} memsqlctl update-config --yes --all --key ssl_key --value /test-ssl/server-key.pem
if dpkg --compare-versions "$VERSION" ge "9.0"; then
  docker exec ${CONTAINER_NAME} memsqlctl update-config --yes --all --key ssl_ca_for_client_cert --value /test-ssl/ca-cert.pem
fi
echo "Restarting cluster"
docker restart ${CONTAINER_NAME}
singlestore-wait-start

echo "Setting up SSL user"
mysql -u root -h ${SINGLESTORE_HOST} -P ${SINGLESTORE_PORT} -p"${SINGLESTORE_PASSWORD}" -e "create user \"${SINGLESTORE_USERNAME_SSL}\"@\"%\" require ssl"
mysql -u root -h ${SINGLESTORE_HOST} -P ${SINGLESTORE_PORT} -p"${SINGLESTORE_PASSWORD}" -e "grant all privileges on *.* to \"${SINGLESTORE_USERNAME_SSL}\"@\"%\" require ssl with grant option"
echo "Done!"

sleep 0.5
echo

# create the database used in tests
mysql -u root -h ${SINGLESTORE_HOST} -P ${SINGLESTORE_PORT} -p"${SINGLESTORE_PASSWORD}" -e "create database if not exists db"
mysql -u root -h ${SINGLESTORE_HOST} -P ${SINGLESTORE_PORT} -p"${SINGLESTORE_PASSWORD}" -e "create database if not exists db2"

echo "Done!"