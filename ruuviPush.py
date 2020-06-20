#!/usr/bin/python3

import sys
import json
import time
from datetime import datetime

import paho.mqtt.client as mqtt
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
from influxdb import InfluxDBClient

from ruuvi import Ruuvi, RuuviScanner

def broadcastMqtt(client, server, port, prefix, postfix, data):
  # Publishing the results to MQTT
  mqttc = mqtt.Client(client)
  mqttc.connect(server, port)

  topic = prefix + "/" + postfix

  #print "MQTT Publish", topic, data
  mqttc.publish(topic, data)

  mqttc.loop(2)

def main(argv):

  print("Starting")

  configuration = json.load(open('configuration.json'))

  if "mqtt" in configuration:
    try:
      if "client" not in configuration["mqtt"]:
        configuration["mqtt"]["client"] = "Ruuvi-Mqtt"

      if "server" not in configuration["mqtt"]:
        configuration["mqtt"]["server"] = "127.0.0.1"

      if "port" not in configuration["mqtt"]:
        configuration["mqtt"]["port"] = 1883

      if "prefix" not in configuration["mqtt"]:
        configuration["mqtt"]["prefix"] = "power"

      if "enabled" not in configuration["mqtt"]:
        configuration["mqtt"]["enabled"] = True

      print ("MQTT Configuration:")
      print ("MQTT Client:    ", configuration["mqtt"]["client"])
      print ("MQTT Server:    ", configuration["mqtt"]["server"])
      print ("MQTT Port:      ", configuration["mqtt"]["port"])
      print ("MQTT Prefix:    ", configuration["mqtt"]["prefix"])
      print ("MQTT Enabled:   ", configuration["mqtt"]["enabled"])

    except Exception as ex:
      print ("Error parsing mqtt configuration", ex)
      configuration["mqtt"]["enabled"] = False
  else:
    configuration["mqtt"] = {}
    configuration["mqtt"]["enabled"] = False

  if "prometheuspush" in configuration:
    try:
      if "server" not in configuration["prometheuspush"]:
        configuration["prometheuspush"]["server"] = "127.0.0.1"

      if "port" not in configuration["prometheuspush"]:
        configuration["prometheuspush"]["port"] = 9091

      if "client" not in configuration["prometheuspush"]:
        configuration["prometheuspush"]["client"] = "Ruuvi-Prometheus"

      if "prefix" not in configuration["prometheuspush"]:
        configuration["prometheuspush"]["prefix"] = "weather"

      if "enabled" not in configuration["prometheuspush"]:
        configuration["prometheuspush"]["enabled"] = True

      print("Prometheus Push Configuration:")
      print("Prometheus Push Client:   ", configuration["prometheuspush"]["client"])
      print("Prometheus Push Server:   ", configuration["prometheuspush"]["server"])
      print("Prometheus Push Port:     ", configuration["prometheuspush"]["port"])
      print("Prometheus Push Prefix:   ", configuration["prometheuspush"]["prefix"])
      print("Prometheus Push Enabled:  ", configuration["prometheuspush"]["enabled"])

    except Exception as ex:
      print("Error parsing prometheuspush configuration", ex)
      configuration["prometheuspush"]["enabled"] = False
  else:
    configuration["prometheuspush"] = {}
    configuration["prometheuspush"]["enabled"] = False

  if "influxdb" in configuration:
    try:
      if "client" not in configuration["influxdb"]:
        configuration["influxdb"]["client"] = "Ruuvi-Influxdb"

      if "server" not in configuration["influxdb"]:
        configuration["influxdb"]["server"] = "127.0.0.1"

      if "username" not in configuration["influxdb"]:
        configuration["influxdb"]["username"] = "influxdb"

      if "password" not in configuration["influxdb"]:
        configuration["influxdb"]["password"] = "influxdb"

      if "port" not in configuration["influxdb"]:
        configuration["influxdb"]["port"] = 8086

      if "database" not in configuration["influxdb"]:
        configuration["influxdb"]["database"] = "measurements"

      if "policy" not in configuration["influxdb"]:
        configuration["influxdb"]["policy"] = "sensor"

      if "prefix" not in configuration["influxdb"]:
        configuration["influxdb"]["prefix"] = "power"

      if "enabled" not in configuration["influxdb"]:
        configuration["influxdb"]["enabled"] = True

      print ("Influxdb Configuration:")
      print ("Influxdb Client:     ", configuration["influxdb"]["client"])
      print ("Influxdb Username:   ", configuration["influxdb"]["username"])
      print ("Influxdb Password:   ", configuration["influxdb"]["password"])
      print ("Influxdb Server:     ", configuration["influxdb"]["server"])
      print ("Influxdb Port:       ", configuration["influxdb"]["port"])
      print ("Influxdb Database:   ", configuration["influxdb"]["database"])
      print ("Influxdb Policy:     ", configuration["influxdb"]["policy"])
      print ("Influxdb Prefix:     ", configuration["influxdb"]["prefix"])
      print ("Influxdb Enabled:    ", configuration["influxdb"]["enabled"])

    except Exception as ex:
      print("Error parsing influxdb configuration", ex)
      configuration["influxdb"]["enabled"] = False
  else:
    configuration["influxdb"] = {}
    configuration["influxdb"]["enabled"] = False

  scanner = RuuviScanner()
  devices = scanner.discoverAll()

  if configuration["influxdb"]["enabled"]:
    influxDbClient = InfluxDBClient(configuration["influxdb"]["server"], configuration["influxdb"]["port"], 
      configuration["influxdb"]["username"], configuration["influxdb"]["password"], configuration["influxdb"]["database"])

    try:
      influxDbClient.create_database(configuration["influxdb"]["database"])

    except InfluxDBClientError as ex:
      print("InfluxDBClientError", ex)

    influxDbClient.create_retention_policy(configuration["influxdb"]["policy"], 'INF', 3, default=True)

  for device in devices:
    print(device)

    realtimeData = device.getRealtimeData()
    sensorId = device.mac.lower()

    tag = {}
    sensorId = str(device.mac.lower().replace(":", "")[-4:])
    tag["air_temperature"] = ("Temperature", realtimeData.temperature)
    tag["air_humidity"] = ("Humidity", realtimeData.humidity)
    tag["air_pressure"] = ("Pressure", realtimeData.pressure)
    tag["battery"] = ("Battery", realtimeData.battery)

    now = datetime.utcnow()
    lastUtc = ("Updated", now.strftime("%Y-%m-%dT%H:%M:%SZ")) #2017-11-13T17:44:11Z

    if configuration["mqtt"]["enabled"]:
      print("Pushing Mqtt", sensorId, ":", configuration["mqtt"]["prefix"], tag)
      try:
        broadcastMqtt(
          configuration["mqtt"]["client"], 
          configuration["mqtt"]["server"], 
          configuration["mqtt"]["port"], 
          configuration["mqtt"]["prefix"], 
          sensorId + "/update",
          json.dumps(tag))
      except Exception as ex:
        print("Error on mqtt broadcast", ex)

    if configuration["prometheuspush"]["enabled"]:
      prometheusRegistry = CollectorRegistry()
      for key in tag.keys():

        g = Gauge(configuration["prometheuspush"]["prefix"]  + '_' + key + '_total', tag[key][0], ['sensorid'], registry=prometheusRegistry)
        g.labels(sensorid=sensorId).set(tag[key][1])

      print("Pushing Prometheus", sensorId, ":", configuration["prometheuspush"]["prefix"] + '_' + key + '_total', "=", tag[key])
      try:
        push_to_gateway(configuration["prometheuspush"]["server"] + ":" + configuration["prometheuspush"]["port"], 
          job=configuration["prometheuspush"]["client"] + "_" + sensorId, 
          registry=prometheusRegistry)
      except Exception as ex:
        print("Error on prometheus push", ex)

    if configuration["influxdb"]["enabled"]:
      influxDbJson = [
      {
        "measurement": configuration["influxdb"]["prefix"],
        "tags": {
            "sensor": sensorId,
        },
        "time": lastUtc[1],
        "fields": {
        }
      }]

      for key in tag.keys():
        influxDbJson[0]["fields"][key] = tag[key][1]

      print("Pushing InfluxDb", influxDbJson)
      try:
        influxDbClient.write_points(influxDbJson, retention_policy=configuration["influxdb"]["policy"])
      except Exception as ex:
        print("Error on influxdb write_points", ex)

    # The sleep makes sure that everything is sent
    time.sleep(1)

if __name__ == "__main__":
  main(sys.argv)
