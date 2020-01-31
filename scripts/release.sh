#!/usr/bin/env bash
#
# Copyright (C) 2020 by eHealth Africa : http://www.eHealthAfrica.org
#
# See the NOTICE file distributed with this work for additional information
# regarding copyright ownership.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
set -Eeuo pipefail

# Try to create the common aether network if it doesn't exist.
{
    docker network create aether_internal
} || { # catch
    echo "aether_internal is ready."
}

# Build docker images
IMAGE_REPO='ehealthafrica'
CORE_APPS=( ckan-consumer )
CORE_COMPOSE='docker-compose.yml'
VERSION=$TRAVIS_TAG

release_app () {
  APP_NAME=$1
  COMPOSE_PATH=$2
  AETHER_APP="aether-${1}"
  echo "$AETHER_APP"
  echo "version: $VERSION"
  echo "Building Docker image ${IMAGE_REPO}/${AETHER_APP}:${VERSION}"
  docker-compose -f $COMPOSE_PATH build --build-arg VERSION=$VERSION $APP_NAME

  docker tag ${AETHER_APP} "${IMAGE_REPO}/${AETHER_APP}:${VERSION}"
  echo "Pushing Docker image ${IMAGE_REPO}/${AETHER_APP}:${VERSION}"
  docker push "${IMAGE_REPO}/${AETHER_APP}:${VERSION}"

}

for APP in "${CORE_APPS[@]}"
do
  release_app $APP $CORE_COMPOSE
done
