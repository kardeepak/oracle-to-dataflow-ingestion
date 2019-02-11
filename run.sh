#!/bin/bash

RUNNER=DirectRunner
CONFIG_KEYNAME=AMDS
CONFIG_KIND=config

mvn compile
mvn exec:java -Dexec.mainClass=com.searce.app.App \
-Dexec.args="--runner=$RUNNER \
--configKeyName=$CONFIG_KEYNAME \
--configKind=$CONFIG_KIND \
--tempLocation=gs://my-airflow-bucket/tmp/"

