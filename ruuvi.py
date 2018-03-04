import binascii
import struct
import time
import math
import os
import sys
import base64

from bluepy.btle import UUID, Peripheral, Scanner, DefaultDelegate, BTLEException


STRUCT_UInt8LE = 'B'
STRUCT_UInt16LE = 'H'
STRUCT_UInt32LE = 'I'
STRUCT_Float = 'f'
STRUCT_Bytes = 'B'
STRUCT_String = 's'

class DeviceInformation:
  localName = ""
  flags = 0
  adData = ""
  maData = ""
  addr = ""
  rssi = 0.0
  uuid = None
  id = ""

  def __init__(self):
    self.localName = ""
    self.flags = 0
    self.adData = ""
    self.maData = ""
    self.addr = ""
    self.rssi = 0.0
    self.uuid = None
    self.id = ""

class RuuviScanner:

  def __init__(self):
    self.scanner = Scanner()

  def _isEddystoneAdvertismentData(self, adData):
    if adData is None or len(adData) < 15:
      return False

    if adData[0] != 0xAA or adData[1] != 0xFE:
      #print "Not Eddystone:", adData[0], adData[1]
      return False

    return True

  def _isRuuviRawData(self, maData):
    if maData is None or len(maData) < 15:
      return False

    if maData[0] != 0x99 or maData[1] != 0x04:
      return False

    return True

  def _parseEddystoneUrl(self, adData):
    if adData[2] != 0x10:
      return None

    if adData[4] != 0x03:
      return None

    url = adData[5:]

    if url.startswith("ruu.vi/") is False:
      return None
    
    return url

  def _parseEddystoneUrlHash(self, url):
    if url is None:
      return None
    
    base64Hash = url[8:-1]
    hashStr = base64.b64decode(base64Hash)
    hash = bytearray (hashStr)
    return hash

  def _discover(self, duration=1):
    devices = self.scanner.scan(duration)
    if devices is None:
      return None

    ruuviDevices = []
    for device in devices:
      #print device.getScanData()

      deviceInformation = DeviceInformation()
      deviceInformation.addr         = device.addr
      deviceInformation.rssi         = device.rssi

      #deviceInformation.localName    = device.getValueText(0x09) #Local Name
      #deviceInformation.localName    = device.getValueText(0x09) #Local Name

      flags        = device.getValueText(0x01) #Flags
      if flags is not None:
        deviceInformation.flags        = int(flags, 16)

      deviceInformation.adData = device.getValueText(0x16) # Service Data - 16-bit UUID
      deviceInformation.maData = device.getValueText(0xFF) # Manufacturer Specific Data - 16-bit UUID

      foundRuuvi = False

      if deviceInformation.adData is not None:
        hexAdData =  bytearray.fromhex(deviceInformation.adData)
        if self._isEddystoneAdvertismentData(hexAdData):

          deviceInformation.localName = "Ruu.vi"

          eddystoneUrl = self._parseEddystoneUrl(hexAdData)
          eddystoneHash = self._parseEddystoneUrlHash(eddystoneUrl)

          if eddystoneHash is not None:
            deviceInformation.adData = eddystoneHash
            deviceInformation.id = "{}{}".format(deviceInformation.addr[-5:-3], deviceInformation.addr[-2:]).upper()

          foundRuuvi = True

        #else:
        #  print "Ignored:",  deviceInformation.addr, "(Cause: Not Eddystone", hexAdData[0], hexAdData[1], ")"
      elif deviceInformation.maData is not None:
        hexMaData =  bytearray.fromhex(deviceInformation.maData)
        print "deviceInformation.maData:", hexMaData
        if self._isRuuviRawData(hexMaData):

          deviceInformation.adData = hexMaData[2:]

          foundRuuvi = True

          #9904034e1a2eb3dbffec000c03e80c1900000000

      #else:
      #  print "Ignored:",  deviceInformation.addr, "(Cause: No adData and no maData)"

      if foundRuuvi:
        print "-"*60
        print "deviceInformation.flags:", deviceInformation.flags
        print "deviceInformation.addr:", deviceInformation.addr
        print "deviceInformation.rssi:", deviceInformation.rssi
        print "deviceInformation.id:", deviceInformation.id
        print "deviceInformation.localName:", deviceInformation.localName
        print "deviceInformation.adData:", deviceInformation.adData
        print "deviceInformation.maData:", deviceInformation.maData
        print "deviceInformation.uuid:", deviceInformation.uuid

        tag = Ruuvi(deviceInformation)

        print "Found Ruu.vi:", tag
        ruuviDevices.append(tag)

    if len(ruuviDevices) == 0:
      ruuviDevices = None

    return ruuviDevices

  def discover(self):
    devices = self._discover(1)
    if devices is None:
      return None

    return devices[0]

  def discoverAll(self):
    devices = self._discover(2)
    if devices is None:
      return None

    return devices

class Ruuvi:
  def __init__(self, deviceInformation):
    self._deviceInformation = deviceInformation
    self.eddystoneHash = deviceInformation.adData
    self.id = deviceInformation.id
    self.name = deviceInformation.localName
    self.mac = deviceInformation.addr

  def __str__(self):
    str = '{{name: "{}" addr: "{}"}}'.format(self.name, self._deviceInformation.addr)
    return str

  class RealtimeData:
    temperature = 0
    humidity = 0
    pressure = 0

    def __init__(self):
      self.temperature = 0
      self.humidity = 0
      self.airPressure = 0

    def __str__(self):
      str = '{{temperature: "{}" humidity: "{}" pressure: "{}" battery: "{}"}}'.format(self.temperature, self.humidity, self.pressure, self.battery)
      return str

  def getRealtimeData(self):
    #print "getRealtimeData", self.eddystoneHash
    print "getRealtimeData", " ".join("{:02x}".format(c) for c in self.eddystoneHash)
    humidity = self.eddystoneHash[1] * 1.0 / 2.0

    temperatureSign = (self.eddystoneHash[2] >> 7) & 1
    temperature =  ((((self.eddystoneHash[2] & 0x7F) << 8) | self.eddystoneHash[3]) * 1.0) / 256.0

    if temperatureSign == 1:
      temperature = temperature * -1.0

    pressure = (self.eddystoneHash[4] * 256.0) + 50000.0 + self.eddystoneHash[5]
    pressure = pressure / 100.0

    battery = 100.0
    if len(self.eddystoneHash) >= 14:
      battery = (self.eddystoneHash[12] * 256.0) + self.eddystoneHash[13]

    realtimeData = Ruuvi.RealtimeData()
    realtimeData.humidity = humidity
    realtimeData.temperature = float("{:.2f}".format(temperature))
    realtimeData.pressure = pressure
    realtimeData.battery = battery

    return realtimeData

def main(argv):
  print "Starting"
  scanner = RuuviScanner()
  devices = scanner.discoverAll()

  for device in devices:
    print device
    print "RealtimeData", device.getRealtimeData()

if __name__ == "__main__":
  main(sys.argv)
