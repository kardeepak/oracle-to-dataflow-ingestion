#!/bin/bash

RUNNER=DataflowRunner
REGION=asia-east1
ZONE=asia-south1-c
PROJECT=tsl-datalake
CONFIG_KIND=config
NETWORK=vpc-tsl
SUBNET=regions/asia-south1/subnetworks/subnet-1
TEMP_LOCATION="gs://tsl_datalake/tmp/"
STAGING_LOCATION="gs://tsl_datalake/tmp/"

mvn compile
mvn exec:java -Dexec.mainClass=com.searce.app.App \
-Dexec.args="--runner=$RUNNER \
--project=$PROJECT \
--region=$REGION \
--zone=$ZONE \
--configKind=$CONFIG_KIND \
--configKeyName=$CONFIG_KEYNAME \
--network=$NETWORK \
--subnetwork=$SUBNET \
--gcpTempLocation=$TEMP_LOCATION \
--stagingLocation=$STAGING_LOCATION"
