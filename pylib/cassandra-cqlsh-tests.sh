#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

################################
#
# Prep
#
################################

WORKSPACE=$1

[ "x${WORKSPACE}" != "x" ] || WORKSPACE="$(readlink -f $(dirname "$0")/..)"

PYTHON_VERSION=python3

export PYTHONIOENCODING="utf-8"
export PYTHONUNBUFFERED=true
export CASS_DRIVER_NO_EXTENSIONS=true
export CASS_DRIVER_NO_CYTHON=true
export CCM_MAX_HEAP_SIZE="2048M"
export CCM_HEAP_NEWSIZE="200M"
export CCM_CONFIG_DIR=${WORKSPACE}/.ccm
export NUM_TOKENS="16"
export CASSANDRA_DIR=${WORKSPACE}
[ "x${BUILD_DIR}" != "x" ] || BUILD_DIR="${CASSANDRA_DIR}/build"

java_version=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | awk -F. '{print $1}')
version=$(grep 'property\s*name=\"base.version\"' ${CASSANDRA_DIR}/build.xml |sed -ne 's/.*value=\"\([^"]*\)\".*/\1/p')
export TESTSUITE_NAME="cqlshlib.${PYTHON_VERSION}.jdk${java_version}"

pushd ${CASSANDRA_DIR} >/dev/null

# check project is already built. no cleaning is done, so jenkins unstash works, beware.
[[ -f "${DIST_DIR}/apache-cassandra-${version}.jar" ]] || [[ -f "${DIST_DIR}/apache-cassandra-${version}-SNAPSHOT.jar" ]] || { echo "Project must be built first. Use \`ant jar\`. Build directory is ${DIST_DIR} with: $(ls ${DIST_DIR})"; exit 1; }

# Set up venv with dtest dependencies
set -e # enable immediate exit if venv setup fails
[ -d ${CASSANDRA_DIR}/venv ] || mkdir -p build/venv
virtualenv --python=$PYTHON_VERSION build/venv
source build/venv/bin/activate
# 3.11 needs the newest pip
curl -sS https://bootstrap.pypa.io/get-pip.py | $PYTHON_VERSION

pip install -r ${CASSANDRA_DIR}/pylib/requirements.txt
pip freeze

if [ "$cython" = "yes" ]; then
    TESTSUITE_NAME="${TESTSUITE_NAME}.cython"
    pip install "Cython>=0.29.15,<3.0"
    pushd pylib >/dev/null
    python setup.py build_ext --inplace
    popd >/dev/null
else
    TESTSUITE_NAME="${TESTSUITE_NAME}.no_cython"
fi

################################
#
# Main
#
################################

ccm remove test || true # in case an old ccm cluster is left behind
ccm create test -n 1 --install-dir=${CASSANDRA_DIR}
ccm updateconf "user_defined_functions_enabled: true"

version_from_build=$(ccm node1 versionfrombuild)
export pre_or_post_cdc=$(python -c """from distutils.version import LooseVersion
print (\"postcdc\" if LooseVersion(\"${version_from_build}\") >= \"3.8\" else \"precdc\")
""")
case "${pre_or_post_cdc}" in
    postcdc)
        ccm updateconf "cdc_enabled: true"
        ;;
    precdc)
        :
        ;;
    *)
        echo "${pre_or_post_cdc}" is an invalid value.
        exit 1
        ;;
esac

ccm start --wait-for-binary-proto

pushd ${CASSANDRA_DIR}/pylib/cqlshlib/ >/dev/null

set +e # disable immediate exit from this point
pytest --junitxml=${BUILD_DIR}/test/output/cqlshlib.xml
RETURN="$?"

# remove <testsuites> wrapping elements. `ant generate-unified-test-report` doesn't like it`
sed -r "s/<[\/]?testsuites>//g" ${BUILD_DIR}/test/output/cqlshlib.xml > /tmp/cqlshlib.xml
cat /tmp/cqlshlib.xml > ${BUILD_DIR}/test/output/cqlshlib.xml

# don't do inline sed for linux+mac compat
sed "s/testsuite errors=\(\".*\"\) failures=\(\".*\"\) hostname=\(\".*\"\) name=\"pytest\"/testsuite errors=\1 failures=\2 hostname=\3 name=\"${TESTSUITE_NAME}\"/g" ${BUILD_DIR}/test/output/cqlshlib.xml > /tmp/cqlshlib.xml
cat /tmp/cqlshlib.xml > ${BUILD_DIR}/test/output/cqlshlib.xml
sed "s/testcase classname=\"cqlshlib./testcase classname=\"${TESTSUITE_NAME}./g" ${BUILD_DIR}/test/output/cqlshlib.xml > /tmp/cqlshlib.xml
cat /tmp/cqlshlib.xml > ${BUILD_DIR}/test/output/cqlshlib.xml

ccm remove

################################
#
# Clean
#
################################

# /virtualenv
deactivate
popd >/dev/null
popd >/dev/null

# circleci needs non-zero exit on failures, jenkins need zero exit to process the test failures
if ! command -v circleci >/dev/null 2>&1
then
    exit 0
else
    exit ${RETURN}
fi
