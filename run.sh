#!/bin/bash

basedir=.
jarfile=${basedir}/target/chaos.jar

if [ ! -f "$jarfile" ]; then
    ./mvnw clean install
fi

java -jar $jarfile $*