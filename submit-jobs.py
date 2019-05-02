#!env python3

from google.cloud import datastore, storage
from os import system, path
from time import sleep

CONFIG_KIND = "config"

total = int(input("Enter total jobs : "))

client = datastore.Client()
query = client.query(kind=CONFIG_KIND)
query.add_filter("outputFolder", "=", "AMDS/")
query.add_filter("counter", "=", 1)
	
for ent in query.fetch():
	if ent.get("url", False):
		print(ent.key.name)	
		cmd = "CONFIG_KEYNAME={} ./runOnDF.sh &".format(ent.key.name.strip())
		system(cmd)
		ent["running"] = True
		client.put(ent)
		total -= 1
		if total == 0:
			break
