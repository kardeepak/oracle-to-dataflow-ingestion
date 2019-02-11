#!/bin/bash

RUNNER=DirectRunner
CONFIG_KEYNAME=testConfig
CONFIG_KIND=test

mvn exec:java -Dexec.mainClass=com.searce.app.App \
-Dexec.args="--runner=$RUNNER \
--configKeyName=$CONFIG_KEYNAME \
--configKind=$CONFIG_KIND \
--tempLocation=gs://my-airflow-bucket/tmp/"

