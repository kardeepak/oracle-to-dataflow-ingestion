#!env python3

from google.cloud import datastore, storage
from os import system, path
from time import sleep

CONFIG_KIND = "config"

client = datastore.Client()
st_cl = storage.Client()
bucket = st_cl.get_bucket("tsl-datalake")

query = client.query(kind = CONFIG_KIND)

for ent in query.fetch():
	if ent.key.name.strip().startswith("MES_FP") and ent["counter"] == 0:
		cmd = "CONFIG_KEYNAME={} ./runOnDF.sh".format(ent.key.name.strip())
		# print(cmd)
		system(cmd)
