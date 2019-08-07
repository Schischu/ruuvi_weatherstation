import sys
import json
import time
from datetime import datetime

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
from influxdb import InfluxDBClient

from ruuvi import Ruuvi, RuuviScanner

def main(argv):

  print "Starting"

  configuration = json.load(open('configuration.json'))
  if configuration.has_key("prometheuspush-client") is False:
    configuration["prometheuspush-client"] = "Ruuvi-Prometheus"

  if configuration.has_key("prometheuspush-server") is False:
    configuration["prometheuspush-server"] = "127.0.0.1"

  if configuration.has_key("prometheuspush-port") is False:
    configuration["prometheuspush-port"] = 9091

  if configuration.has_key("prometheuspush-prefix") is False:
    configuration["prometheuspush-prefix"] = "weather"

  if configuration.has_key("influxdb-client") is False:
    configuration["influxdb-client"] = "Ruuvi-Influxdb"

  if configuration.has_key("influxdb-server") is False:
    configuration["influxdb-server"] = "127.0.0.1"

  if configuration.has_key("influxdb-username") is False:
    configuration["influxdb-username"] = "influxdb"

  if configuration.has_key("influxdb-password") is False:
    configuration["influxdb-password"] = "influxdb"

  if configuration.has_key("influxdb-port") is False:
    configuration["influxdb-port"] = 8086

  if configuration.has_key("influxdb-database") is False:
    configuration["influxdb-database"] = "measurements"

  if configuration.has_key("influxdb-prefix") is False:
    configuration["influxdb-prefix"] = "sensor.weather"

  print "Configuration:"
  print "Prometheus Push Client:   ", configuration["prometheuspush-client"]
  print "Prometheus Push Server:   ", configuration["prometheuspush-server"]
  print "Prometheus Push Port:     ", configuration["prometheuspush-port"]
  print "Prometheus Push Prefix    ", configuration["prometheuspush-prefix"]

  print "Influxdb Push Client:     ", configuration["influxdb-client"]
  print "Influxdb Push Username:   ", configuration["influxdb-username"]
  print "Influxdb Push Password:   ", configuration["influxdb-password"]
  print "Influxdb Push Server:     ", configuration["influxdb-server"]
  print "Influxdb Push Port:       ", configuration["influxdb-port"]
  print "Influxdb Push Database    ", configuration["influxdb-database"]
  print "Influxdb Push Prefix      ", configuration["influxdb-prefix"]

  scanner = RuuviScanner()
  devices = scanner.discoverAll()

  influxDbClient = InfluxDBClient(configuration["influxdb-server"], configuration["influxdb-port"], 
    configuration["influxdb-username"], configuration["influxdb-password"], configuration["influxdb-database"])

  influxDbClient.create_database(configuration["influxdb-database"])

  for device in devices:
    print device

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

    prometheusRegistry = CollectorRegistry()
    for key in tag.keys():

      g = Gauge(configuration["prometheuspush-prefix"]  + '_' + key + '_total', tag[key][0], ['sensorid'], registry=prometheusRegistry)
      g.labels(sensorid=sensorId).set(tag[key][1])

      print "Pushing", sensorId, ":", configuration["prometheuspush-prefix"] + '_' + key + '_total', "=", tag[key]

    push_to_gateway(configuration["prometheuspush-server"] + ":" + configuration["prometheuspush-port"], 
      job=configuration["prometheuspush-client"] + "_" + sensorId, 
      registry=prometheusRegistry)

    influxDbJson = [
    {
      "measurement": configuration["influxdb-prefix"],
      "tags": {
          "sensor": sensorId,
      },
      "time": lastUtc[1],
      "fields": {
      }
    }]

    for key in tag.keys():
      influxDbJson[0]["fields"][key] = tag[key][1]

    print "Pushing", influxDbJson
    influxDbClient.write_points(influxDbJson)

    time.sleep(1)

if __name__ == "__main__":
  main(sys.argv)
