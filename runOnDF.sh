#!/bin/bash

RUNNER=DataflowRunner
CONFIG_KEYNAME=testConfig
CONFIG_KIND=test
NETWROK=vpc-tsl
SUBNET=subnet-1

mvn exec:java -Dexec.mainClass=com.searce.app.App \
-Dexec.args="--runner=$RUNNER \
--configKeyName=$CONFIG_KEYNAME \
--configKind=$CONFIG_KIND \
--network=$NETWORK \
--subnetwork=$SUBNET"

