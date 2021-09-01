import influxdb
import paho.mqtt.client as mqtt
import json
from datetime import datetime
import time
import queue
import threading


DB='victron'

class MqttToInflux:
   def __init__(self):
    self._points = queue.Queue()
    self._msg_count = 0
    self._msg_ignored = 0

    t = threading.Thread(target=self.write)
    t.daemon = True
    t.start()

    self._influx = influxdb.InfluxDBClient(host='localhost', port=8086)
    self._influx.create_database(DB)
    self._influx.switch_database(DB)

    self._mqtt = mqtt.Client()
    self._mqtt.on_connect = self.on_connect
    self._mqtt.on_message = self.on_message
    self._mqtt.connect('192.168.2.10', 1883, 60)
    self._mqtt.loop_forever()

   def on_connect(self, client, userdata, flags, rc):
    print('connected')
    client.subscribe('N/#')

   def on_message(self, client, userdata, msg):
    self._msg_count += 1
    t = msg.topic
    p = t.split('/')
    m = '.'.join(p[4:])
    v = json.loads(msg.payload)['value']
    # print(t, m, v, type(v))
    if type(v) not in [float, int]:
        self._msg_ignored += 1
        return
    v = float(v)  # automatic conversion sometimes makes it an int
    point = {
        "measurement": m,
        "tags": {
            "path": p[2],
            "instanceNumber": p[3],
            "portalId": p[1]
        },
        "time": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        "fields": {
            "value": v
        }
        }
    self._points.put(point)

   def write(self):
      points = {}
      lastwrite = time.time()
      deduped = 0
      while True:
        now = time.time()
        try:
            p = self._points.get(timeout=1)
            # deduplicate frequent measurements
            if p['measurement'] in points:
                deduped += 1
            points[p['measurement']] = p
        except queue.Empty:
            pass
        if now - lastwrite > 10:
            if points:
                print('Write points', len(points), time.time() - now)
                self._influx.write_points(points.values())
                points = {}
            print(self._msg_count, self._msg_ignored, deduped)
            lastwrite = now

def main():
    MqttToInflux()

main()
