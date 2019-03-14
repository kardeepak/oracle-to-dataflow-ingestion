#!env python3

from google.cloud import datastore, storage
from os import system, path
from time import sleep

CONFIG_KIND = "config"

client = datastore.Client()
st_cl = storage.Client()
bucket = st_cl.get_bucket("tsl-datalake")

query = client.query(kind = CONFIG_KIND)
query.add_filter("counter", "=", 1)

total = int(input("Enter total jobs : "))

for ent in query.fetch():
	if ent.key.name.strip().startswith("DSS"):
		print(ent["tableName"])
		cmd = "CONFIG_KEYNAME={} ./runOnDF.sh &".format(ent.key.name.strip())
		system(cmd)
		total -= 1
		if total == 0:
			break
