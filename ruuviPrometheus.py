import sys
import json
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
import time
from datetime import datetime

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

  print "Configuration:"
  print "Prometheus Push Client:   ", configuration["prometheuspush-client"]
  print "Prometheus Push Server:   ", configuration["prometheuspush-server"]
  print "Prometheus Push Port:     ", configuration["prometheuspush-port"]
  print "Prometheus Push Prefix   :", configuration["prometheuspush-prefix"]

  scanner = RuuviScanner()
  devices = scanner.discoverAll()


  for device in devices:
    print device

    realtimeData = device.getRealtimeData()
    sensorId = device.mac.lower()

    tag = {}
    #tag["sensor_id"] = device.mac.lower()
    tag["air_temperature"] = ("Temperature", realtimeData.temperature)
    tag["air_humidity"] = ("Humidity", realtimeData.humidity)
    tag["air_pressure"] = ("Pressure", realtimeData.pressure)
    tag["battery"] = ("Battery", realtimeData.battery)

    #now = datetime.utcnow()
    #tag["last_utc"] = ("Updated", now.strftime("%Y-%m-%dT%H:%M:%SZ")) #2017-11-13T17:44:11Z

    registry = CollectorRegistry()
    for key in tag.keys():

      g = Gauge(configuration["prometheuspush-prefix"]  + '_' + key + '_total', tag[key][0], ['sensorid'], registry=registry)
      g.labels(sensorid=sensorId).set(tag[key][1])

      print "Pushing", sensorId, ":", configuration["prometheuspush-prefix"] + '_' + key + '_total', "=", tag[key]

    push_to_gateway(configuration["prometheuspush-server"] + ":" + configuration["prometheuspush-port"], 
      job=configuration["prometheuspush-client"] + "_" + sensorId, 
      registry=registry)

    time.sleep(1)

if __name__ == "__main__":
  main(sys.argv)
