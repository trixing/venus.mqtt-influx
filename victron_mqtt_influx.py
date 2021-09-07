import influxdb
import paho.mqtt.client as mqtt
import json
from datetime import datetime
import time
import queue
import threading
import sys
from collections import defaultdict
#import defaultdict


DB='victron'

class MqttToInflux:
   def __init__(self):
    self._points = queue.Queue(maxsize=10000)
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
    # print(m, v)
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
    try:
        self._points.put(point, block=False)
    except queue.Full:
        print('Queue full')
        sys.exit(2)

   def write(self):
      lastwrite = time.time()
      deduped = 0
      points = defaultdict(list)
      agg = defaultdict(dict)
      while True:
        now = time.time()
        try:
            p = self._points.get(timeout=1)
            k = p['measurement'] + '.' + p['tags']['path'] + '.' + p['tags']['portalId'] + '.' + p['tags']['instanceNumber']
            parts = p['measurement'].split('.')
            i = None
            if 'L1' in parts:
                i = parts.index('L1')
            if 'L2' in parts:
                i = parts.index('L2')
            if 'L3' in parts:
                i = parts.index('L3')
            if i is not None:
                what = parts[i+1]
                ks = k.replace('L1', 'Lx').replace('L2', 'Lx').replace('L3', 'Lx')
                # print(ks, what)
                if what in ('Power', 'Current', 'Voltage', 'Energy', 'I', 'P', 'V'):
                    agg[ks][parts[i]] = p
                #else:
                #    print('ignored', what, ks)
                if len(agg[ks]) == 3:
                    ps = p.copy()
                    ps['measurement'] = ps['measurement'].replace('L1', 'Lx').replace('L2', 'Lx').replace('L3', 'Lx')
                    ps['fields']['value'] = sum(v['fields']['value'] for v in agg[ks].values())
                    if what == 'Voltage' or what == 'V':
                        ps['fields']['value'] /= 3
                    # print('new sum', ks, what, ps['fields']['value'])
                    points[ks].append(ps)
                    del agg[ks]

                
            points[k].append(p)
        except queue.Empty:
            pass
        if now - lastwrite > 10:
            if points:
                tbw = []
                duped = 0
                for k, ms in points.items():
                    if '.Power.' in k:
                        tbw += ms
                        continue
                    value = sum(v['fields']['value'] for v in ms) / len(ms)
                    ms[0]['fields']['value'] = value
                    duped += len(ms) - 1
                    tbw.append(ms[0])
                print('Write points', len(tbw), len(points), duped, time.time() - now)
                self._influx.write_points(tbw)
                points = defaultdict(list)
                # agg = defaultdict(dict) ??
            print(self._msg_count, self._msg_ignored)
            lastwrite = now

def main():
    MqttToInflux()

main()
