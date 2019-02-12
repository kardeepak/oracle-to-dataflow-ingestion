#!/bin/bash

RUNNER=DirectRunner
CONFIG_KIND=config
CONFIG_KEYNAME=AMDS.CMSDBA.T_AI_CONFIG

mvn compile
mvn exec:java -Dexec.mainClass=com.searce.app.App \
-Dexec.args="--runner=$RUNNER \
--configKeyName=$CONFIG_KEYNAME \
--configKind=$CONFIG_KIND \
--tempLocation=/tmp/"

