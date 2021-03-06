#!/usr/bin/env python3

from google.cloud import datastore
from os import system
from flask import Flask

app = Flask(__name__)

@app.route("/")
def submit_jobs():
	CONFIG_KIND = "config"
	client = datastore.Client()
	query = client.query(kind=CONFIG_KIND)
	query.add_filter("outputFolder", "=", "AMDS/")
	entities = list(query.fetch())

	counter = min([ent["counter"] for ent in entities if ent["counter"] > 1])
	running = len([ent for ent in entities if ent["running"] and ent["counter"] > 1])

	query = client.query(kind=CONFIG_KIND)
	query.add_filter("counter", "=", counter)
	query.add_filter("outputFolder", "=", "AMDS/")
	entities = list(query.fetch())

	while running < 10 and len(entities) > 0:
		ent = entities.pop(0)
		cmd = "CONFIG_KEYNAME={} ./runOnDF.sh &".format(ent.key.name.strip())
		system(cmd)
		ent["running"] = True
		client.put(ent)
		running += 1

	return "Running..."

if __name__ == "__main__":
	app.run(host="0.0.0.0", port=8000)
