#!/bin/bash
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
#
# A wrapper script to run-tests.sh (or dtest-python.sh) in docker.
#  Can split (or grep) the test list into multiple docker runs, collecting results.
#
# Each split chunk may be further parallelised over docker containers based on the host's available cpu and memory.
#  TODO: figure out if we still want this inner splitting, as it contributes to most of the complexity in this script.
#
#

# help
if [ "$#" -lt 1 ] || [ "$#" -gt 3 ] || [ "$1" == "-h" ]; then
    echo ""
    echo "Usage: run-tests.sh target [split_chunk] [java_version]"
    echo ""
    echo "        default split_chunk is 1/1"
    echo "        default java_version is what 'java.default' specifies in build.xml"
    exit 1
fi

# variables, with defaults
[ "x${cassandra_dir}" != "x" ] || cassandra_dir="$(readlink -f $(dirname "$0")/../..)"
[ "x${cassandra_dtest_dir}" != "x" ] || cassandra_dtest_dir="${cassandra_dir}/../cassandra-dtest"
[ "x${build_dir}" != "x" ] || build_dir="${cassandra_dir}/build"
[ -d "${build_dir}" ] || { mkdir -p "${build_dir}" ; }

# pre-conditions
command -v docker >/dev/null 2>&1 || { echo >&2 "docker needs to be installed"; exit 1; }
command -v bc >/dev/null 2>&1 || { echo >&2 "bc needs to be installed"; exit 1; }  # FIXME or remove docker_runs calc section
(docker info >/dev/null 2>&1) || { echo >&2 "docker needs to running"; exit 1; }
[ -f "${cassandra_dir}/build.xml" ] || { echo >&2 "${cassandra_dir}/build.xml must exist"; exit 1; }
[ -f "${cassandra_dir}/.build/run-tests.sh" ] || { echo >&2 "${cassandra_dir}/.build/run-tests.sh must exist"; exit 1; }

# arguments
target=$1
split_chunk="1/1"
[ "$#" -gt 1 ] && split_chunk=$2
java_version=$3

test_script="run-tests.sh"
java_version_default=`grep 'property\s*name="java.default"' ${cassandra_dir}/build.xml |sed -ne 's/.*value="\([^"]*\)".*/\1/p'`
java_version_supported=`grep 'property\s*name="java.supported"' ${cassandra_dir}/build.xml |sed -ne 's/.*value="\([^"]*\)".*/\1/p'`

if [ "x${java_version}" == "x" ] ; then
    echo "Defaulting to java ${java_version_default}"
    java_version="${java_version_default}"
fi

regx_java_version="(${java_version_supported//,/|})"
if [[ ! "${java_version}" =~ $regx_java_version ]]; then
    echo "Error: Java version is not in ${java_version_supported}, it is set to ${java_version}"
    exit 1
fi

python_version="3.6"
command -v python >/dev/null 2>&1 && python_version="$(python -V | awk '{print $2}' | awk -F'.' '{print $1"."$2}')"

# print debug information on versions
docker --version


pushd ${cassandra_dir}/.build >/dev/null

# build test image
dockerfile="ubuntu2004_test.docker"
image_tag="$(md5sum docker/${dockerfile} | cut -d' ' -f1)"
image_name="apache/cassandra-${dockerfile/.docker/}:${image_tag}"
docker_mounts="-v ${cassandra_dir}:/home/cassandra/cassandra -v "${build_dir}":/dist -v ${HOME}/.m2/repository:/home/cassandra/.m2/repository"

# Look for existing docker image, otherwise build
timeout -k 5 5 docker login >/dev/null 2>/dev/null
if ! ( [[ "$(docker images -q ${image_name} 2>/dev/null)" != "" ]] || docker pull -q ${image_name} ) >/dev/null 2>/dev/null ; then
    # Create build images containing the build tool-chain, Java and an Apache Cassandra git working directory, with retry
    until docker build -t ${image_name} -f docker/${dockerfile} .  ; do
        echo "docker build failed… trying again in 10s… "
        sleep 10
    done
fi

pushd ${cassandra_dir} >/dev/null

# Jenkins agents run multiple executors per machine. `jenkins_executors=1` is used for anything non-jenkins.
jenkins_executors=1
if [[ ! -z ${JENKINS_URL+x} ]] && [[ ! -z ${NODE_NAME+x} ]] ; then
    fetched_jenkins_executors=$(curl -s --retry 9 --retry-connrefused --retry-delay 1 "${JENKINS_URL}/computer/${NODE_NAME}/api/json?pretty=true" | grep 'numExecutors' | awk -F' : ' '{print $2}' | cut -d',' -f1)
    # use it if we got a valid number (despite retry settings the curl above can still fail
    [[ ${fetched_jenkins_executors} =~ '^[0-9]+$' ]] && jenkins_executors=${fetched_jenkins_executors}
fi

# find host's available cores and mem
cores=1
command -v nproc >/dev/null 2>&1 && cores=$(nproc --all)
mem=1
# linux
command -v free >/dev/null 2>&1 && mem=$(free -b | grep Mem: | awk '{print $2}')
# macos
sysctl -n hw.memsize >/dev/null 2>&1 && mem=$(sysctl -n hw.memsize)

# for relevant test targets calculate how many docker containers we should split the test list over
case ${target} in
    # test-burn doesn't have enough tests in it to split beyond 8, and burn and long we want a bit more resources anyway
    "stress-test" | "fqltool-test" | "microbench" | "test-burn" | "long-test" | "cqlsh-test" )
        [[ ${mem} -gt $((5 * 1024 * 1024 * 1024 * ${jenkins_executors})) ]] || { echo >&2 "tests require minimum docker memory 6g (per jenkins executor (${jenkins_executors})), found ${mem}"; exit 1; }
        docker_runs=1
    ;;
    "dtest" | "dtest-novnode" | "dtest-offheap" | "dtest-large" | "dtest-large-novnode" | "dtest-upgrade" )
        [ -f "${cassandra_dtest_dir}/dtest.py" ] || { echo >&2 "${cassandra_dtest_dir}/dtest.py must exist"; exit 1; }
        [[ ${mem} -gt $((15 * 1024 * 1024 * 1024 * ${jenkins_executors})) ]] || { echo >&2 "dtests require minimum docker memory 16g (per jenkins executor (${jenkins_executors})), found ${mem}"; exit 1; }
        docker_runs=1
        test_script="run-python-dtests.sh"
        docker_mounts="${docker_mounts} -v ${cassandra_dtest_dir}:/home/cassandra/cassandra-dtest"
        # check that exists ${cassandra_dtest_dir}
        [ -f "${cassandra_dtest_dir}/dtest.py" ] || { echo >&2 "${cassandra_dtest_dir}/dtest.py not found. please specify 'cassandra_dtest_dir' to point to the local cassandra-dtest source"; exit 1; }
    ;;
    "test"| "test-cdc" | "test-compression" | "jvm-dtest" | "jvm-dtest-upgrade")
        [[ ${mem} -gt $((5 * 1024 * 1024 * 1024 * ${jenkins_executors})) ]] || { echo >&2 "tests require minimum docker memory 6g (per jenkins executor (${jenkins_executors})), found ${mem}"; exit 1; }
        max_docker_runs_by_cores=$( echo "sqrt( ${cores} / ${jenkins_executors} )" | bc )
        max_docker_runs_by_mem=$(( ${mem} / ( 5 * 1024 * 1024 * 1024 * ${jenkins_executors} ) ))
        docker_runs=$(( ${max_docker_runs_by_cores} < ${max_docker_runs_by_mem} ? ${max_docker_runs_by_cores} : ${max_docker_runs_by_mem} ))
        docker_runs=$(( ${docker_runs} < 1 ? 1 : ${docker_runs} ))
    ;;
    *)
    echo "unrecognized \"${target}\""
    exit 1
    ;;
esac

docker_runs=1 # tmp FIXME # TODO this also clashes with the test regexp approach (instead of split chunks)

# Break up the requested split chunk into a number of concurrent docker runs, as calculated above
# This will typically be between one to four splits. Five splits would require >25 cores and >25GB ram
if [[ "${split_chunk}" =~ ^[0-9]+/[0-9]+$ ]]; then
    inner_splits=$(( $(echo $split_chunk | cut -d"/" -f2 ) * ${docker_runs} ))
    inner_split_first=$(( ( $(echo $split_chunk | cut -d"/" -f1 ) * ${docker_runs} ) - ( ${docker_runs} - 1 ) ))
fi
docker_cpus=$(echo "scale=2; ${cores} / ( ${jenkins_executors} * ${docker_runs} )" | bc)

# hack: long-test does not handle limited CPUs
if [ "${target}" == "long-test" ] ; then
    docker_flags="-m 5g --memory-swap 5g"
elif [[ "${target}" =~ dtest* ]] ; then
    docker_flags="--cpus=${docker_cpus} -m 15g --memory-swap 15g"
else
    docker_flags="--cpus=${docker_cpus} -m 5g --memory-swap 5g"
fi
docker_flags="${docker_flags} --env-file build/env.list -d --rm"

# make sure build_dir is good
mkdir -p ${cassandra_dir}/build/tmp || true
mkdir -p ${cassandra_dir}/build/test/logs || true
mkdir -p ${cassandra_dir}/build/test/output || true
chmod -R ag+rwx ${cassandra_dir}/build

# FIXME cython
cython="no"

# the docker container's env
touch build/env.list
cat > build/env.list <<EOF
TEST_SCRIPT=${test_script}
JAVA_VERSION=${java_version}
PYTHON_VERSION=${python_version}
CYTHON_ENABLED=${cython}
ANT_OPTS="-Dtesttag.extra=.arch=$(arch).python${python_version}.cython=${cython}"
EOF

declare -a docker_ids
declare -a process_ids
declare -a statuses

# TODO – decide if we want to support inner_splits, and if so remove all these loops and waiting on multiple containers logic
for i in `seq 1 ${docker_runs}` ; do
    inner_split=$(( ${inner_split_first} + ( $i - 1 ) ))

    random_string="$(LC_ALL=C tr -dc A-Za-z0-9 </dev/urandom | head -c 6 ; echo '')"
    container_name="cassandra_${dockerfile/.docker/}_${target}_jdk${java_version/./-}_arch-$(arch)_python${python_version/./-}_cython-${cython}_${inner_split}_${inner_splits}__${random_string}"

    # Docker commands:
    #  set java to java_version
    #  execute the run_script

    split_arg="${split_chunk}"
    if [[ "${split_arg}" =~ ^[0-9]+/[0-9]+$ ]]; then
        split_arg="${inner_split}/${inner_splits}"
    fi

    docker_command="source \${CASSANDRA_DIR}/.build/docker/_set_java.sh ${java_version} ; \
                \${CASSANDRA_DIR}/.build/docker/_docker_init_tests.sh ${target} ${split_arg} ; exit \$?"

    # start the container
    docker_id=$(docker run --name ${container_name} ${docker_flags} ${docker_mounts} ${image_name} sleep inf)

    echo "Running container ${container_name} ${docker_id}"

    docker exec --user root ${container_name} bash -c "\${CASSANDRA_DIR}/.build/docker/_create_user.sh cassandra $(id -u) $(id -g)"
    docker exec --user root ${container_name} update-alternatives --set python /usr/bin/python${python_version}

    # capture logs and pid for container
    docker exec --user cassandra ${container_name} bash -c "${docker_command}" > ${cassandra_dir}/build/test/logs/docker_attach_${container_name}_${i}.log &
    process_ids+=( $! )
    docker_ids+=( ${docker_id} )
done

exit_result=0
i=0
for process_id in "${process_ids[@]}" ; do
    # wait for each container to complete
    docker_id=${docker_ids[$i]}
    inner_split=$(( $inner_split_first + $i ))
    #cat ${cassandra_dir}/build/test/logs/docker_attach_${container_name}_$(( $i + 1 )).log
    ( tail -F ${cassandra_dir}/build/test/logs/docker_attach_${container_name}_$(( $i + 1 )).log 2>/dev/null ) &
    tail_pid=$!
    wait ${process_id}
    status=$?
    process_ids+=( ${status} )
    kill $tail_pid
    echo

    if [ "$status" -ne 0 ] ; then
        echo "${docker_id} failed (${status}), debug…"
        docker inspect ${docker_id}
        echo "–––"
        docker logs ${docker_id}
        echo "–––"
        docker ps -a
        echo "–––"
        docker info
        echo "–––"
        exit_result=$status
        echo "Failure."
    else
        # pull from the container all logs and results we might need. this is optimistic, not all files exist in all test types
        echo "${docker_id} done (status=${status}), copying files…"
        docker cp ${docker_id}:${build_dir}/test/output/. ${cassandra_dir}/build/test/output/ 2>/dev/null
        docker cp ${docker_id}:${build_dir}/test/logs/. ${cassandra_dir}/build/test/logs/ 2>/dev/null
        # pytests
        docker cp ${docker_id}:/home/cassandra/cassandra-dtest/nosetests.xml ${cassandra_dir}/build/test/output/  2>/dev/null
        docker cp ${docker_id}:${build_dir}/test_stdout.txt.xz ${cassandra_dir}/build/  2>/dev/null
        docker cp ${docker_id}:/home/cassandra/cassandra-dtest/ccm_logs.tar.xz ${cassandra_dir}/build/  2>/dev/null

        echo "Completed."
    fi
    [ "x${docker_id}" == "x" ] || docker stop ${docker_id} >/dev/null
    ((i++))
done

xz -f ${cassandra_dir}/build/test/logs/docker_attach_${container_name}_*.log 2>/dev/null

popd >/dev/null
popd >/dev/null
exit $exit_result
