import sys
import json
import paho.mqtt.client as mqtt
import time
from datetime import datetime

from ruuvi import Ruuvi, RuuviScanner

def broadcastMqtt(client, server, port, prefix, postfix, data):
  # Publishing the results to MQTT
  mqttc = mqtt.Client(client)
  mqttc.connect(server, port)

  topic = prefix + "/" + postfix

  print "MQTT Publish", topic, data
  mqttc.publish(topic, data)

  mqttc.loop(2)

def main(argv):

  print "Starting"

  configuration = json.load(open('configuration.json'))
  if configuration.has_key("mqtt-client") is False:
    configuration["mqtt-client"] = "Ruuvi-Mqtt"

  if configuration.has_key("mqtt-server") is False:
    configuration["mqtt-server"] = "127.0.0.1"

  if configuration.has_key("mqtt-port") is False:
    configuration["mqtt-port"] = 1883

  if configuration.has_key("mqtt-prefix") is False:
    configuration["mqtt-prefix"] = "weather"

  print "Configuration:"
  print "MQTT Client:   ", configuration["mqtt-client"]
  print "MQTT Server:   ", configuration["mqtt-server"]
  print "MQTT Port:     ", configuration["mqtt-port"]
  print "MQTT Prefix   :", configuration["mqtt-prefix"]

  scanner = RuuviScanner()
  devices = scanner.discoverAll()

  for device in devices:
    print device

    realtimeData = device.getRealtimeData()

    tag = {}
    tag["sensor_name"] = device.name
    sensorId = device.id.lower()

    tag["air_temperature"] = realtimeData.temperature
    tag["air_humidity"] = realtimeData.humidity
    tag["air_pressure"] = realtimeData.pressure

    now = datetime.utcnow()
    tag["last_utc"] = now.strftime("%Y-%m-%dT%H:%M:%SZ") #2017-11-13T17:44:11Z

    broadcastMqtt(
      configuration["mqtt-client"], 
      configuration["mqtt-server"], 
      configuration["mqtt-port"], 
      configuration["mqtt-prefix"], 
      sensorId + "/update",
      json.dumps(tag))

    time.sleep(1)

if __name__ == "__main__":
  main(sys.argv)