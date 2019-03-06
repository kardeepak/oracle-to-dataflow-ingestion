#!env python3

from google.cloud import datastore, storage
from os import system, path
from time import sleep

CONFIG_KIND = "config"

client = datastore.Client()
st_cl = storage.Client()
bucket = st_cl.get_bucket("tsl-datalake")

query = client.query(kind = CONFIG_KIND)
query.add_filter("counter", "=", 0)

cnt = 0
total = int(input("Enter total jobs : "))

for ent in query.fetch():
	if ent.key.name.strip().startswith("DSS") and ent["tableName"] != "T_PI_DSS_KBF":
		print(ent)
		ent["counter"] = 0
		client.put(ent)
		cmd = "CONFIG_KEYNAME={} ./runOnDF.sh &".format(ent.key.name.strip())
		system(cmd)
		with open("started.csv", "a") as f:
			f.write(ent.key.name + "\n")
		cnt += 1
		if cnt == total:
			break
