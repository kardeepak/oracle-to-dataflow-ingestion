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
	with open("started.csv", "r") as f:
		lines = [x.strip() for x in f.readlines()]
	if ent.key.name.strip().startswith("MES_FP") and ent.key.name not in lines:
		cmd = "CONFIG_KEYNAME={} ./runOnDF.sh &".format(ent.key.name.strip())
		system(cmd)
		with open("started.csv", "a") as f:
			f.write(ent.key.name + "\n")
		cnt += 1
		if cnt == total:
			break
