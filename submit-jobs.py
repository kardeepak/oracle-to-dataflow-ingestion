#!env python3

from google.cloud import datastore
from os import system

CONFIG_KIND = "config"

client = datastore.Client()

query = client.query(kind = CONFIG_KIND)

for ent in query.fetch():
	if ent.key.name.strip().startswith("DSSDATA"):
		cmd = "CONFIG_KEYNAME={} ./runOnDF.sh".format(ent.key.name.strip())
		system(cmd)
