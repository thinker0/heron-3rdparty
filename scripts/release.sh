#!/usr/bin/env bash

if [[ -e /usr/libexec/java_home ]]; then
    export JAVA_HOME="$(/usr/libexec/java_home -v '1.8')"
fi

${JAVA_HOME}/bin/java -version

mvn -B gitflow:release-start gitflow:release-finish
