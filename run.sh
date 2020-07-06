#!/bin/bash

export _JAVA_OPTIONS="-Xmx30g"
export MAVEN_OPTS="-Xmx30g -XX:+HeapDumpOnOutOfMemoryError"

#mvn compile && mvn exec:java -Dexec.mainClass="Main"  -Dexec.args="/Users/wajih/Downloads/logs/short ./src/tmp/"

mvn compile && mvn exec:java -Dexec.mainClass="Main"  -Dexec.args="/data/disk1/optc-dataset/ecar/evaluation/25Sept /data/disk2/backup-dataset/tmp/"
