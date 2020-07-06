#!/bin/bash

export _JAVA_OPTIONS="-Xmx30g"
export MAVEN_OPTS="-Xmx30g -XX:+HeapDumpOnOutOfMemoryError"

mvn compile && mvn exec:java -Dexec.mainClass="Main"  -Dexec.args="/Users/wajih/Downloads/logs/short ./src/tmp/"

#mvn compile && mvn exec:java -Dexec.mainClass="Main"  -Dexec.args="/Users/wajih/Downloads/logs/short"
