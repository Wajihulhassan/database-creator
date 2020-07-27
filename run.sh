#!/bin/bash

export _JAVA_OPTIONS="-Xmx50g"
export MAVEN_OPTS="-Xmx50g"

#mvn compile && mvn exec:java -Dexec.mainClass="Main"  -Dexec.args="/Users/wajih/Downloads/logs/short ./src/tmp/"

# Day 3
#mvn compile && mvn exec:java -Dexec.mainClass="Main"  -Dexec.args="/data/disk1/optc-dataset/ecar/evaluation/25Sept/ /data/disk2/backup-dataset/tmp/ /data/disk1/optc-dataset/ecar-bro/evaluation/25Sept/ 2019-09-25"

# Day 1
# mvn compile && mvn exec:java -Dexec.mainClass="Main"  -Dexec.args="/data/disk1/optc-dataset/ecar/evaluation/23Sep19-red/ /data/disk2/backup-dataset/tmp/ /data/disk1/optc-dataset/ecar-bro/evaluation/23Sep19-red/ 2019-09-23"

# Day 2
mvn compile && mvn exec:java -Dexec.mainClass="Main"  -Dexec.args="/data/disk1/optc-dataset/ecar/evaluation/24Sep19/ /data/disk2/backup-dataset/tmp/ /data/disk1/optc-dataset/ecar-bro/evaluation/24Sep19/ 2019-09-24"
