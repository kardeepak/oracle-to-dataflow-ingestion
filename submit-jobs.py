#!env python3

from google.cloud import datastore, storage
from os import system, path
from time import sleep

CONFIG_KIND = "config"

total = int(input("Enter total jobs : "))

keys = open("start.csv", "r").read().split("\n")

for key in keys:
	if True:
		print(key)
		cmd = "CONFIG_KEYNAME={} ./runOnDF.sh &".format(ent.key.name.strip())
		system(cmd)
		total -= 1
		if total == 0:
			break
