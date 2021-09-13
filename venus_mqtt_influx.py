import influxdb
import paho.mqtt.client as mqtt
from datetime import datetime
import json
import logging
import queue
import sys
import threading
import time
from collections import defaultdict
#import defaultdict

log = logging.getLogger('mqtt_to_influx')

DB='victron'
INFLUX_HOST='localhost'
MQTT_HOST='192.168.2.10'


class MqttToInflux:
   def __init__(self, dryrun=False):
    self._points = queue.Queue(maxsize=10000)
    self._msg_count = 0
    self._msg_ignored = 0
    self._msg_seen = set()
    self._dryrun = dryrun

    t = threading.Thread(target=self.write)
    t.daemon = True
    t.start()

    self._influx = influxdb.InfluxDBClient(host=INFLUX_HOST, port=8086)
    if not self._dryrun:
        self._influx.create_database(DB)
        self._influx.switch_database(DB)

    self._mqtt = mqtt.Client()
    self._mqtt.on_connect = self.on_connect
    self._mqtt.on_message = self.on_message
    self._mqtt.on_subscribe = self.on_subscribe
    self._mqtt.connect(MQTT_HOST, 1883, 60)
    self._mqtt.loop_forever()

   def on_connect(self, client, userdata, flags, rc):
    log.info('Connected to mqtt')
    client.subscribe('N/#')

   def on_subscribe(self, client, userdata, flags, rc):
    log.info('MQTT subscription successful.')

   def on_message(self, client, userdata, msg):
    self._msg_count += 1
    t = msg.topic
    p = t.split('/')
    m = '.'.join(p[4:])
    v = json.loads(msg.payload)['value']
    # print(t, m, v, type(v))
    if type(v) not in [float, int]:
        self._msg_ignored += 1
        if t not in self._msg_seen:
            log.info('Ignoring %s of type %s' % (t, type(v)))
            self._msg_seen.add(t)
        elif type(v) != type(None):
            log.debug('Ignoring %s of type %s' % (t, type(v)))
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
        log.error('Queue full')
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
            interval = now - lastwrite
            lastwrite = now
            if points:
                tbw = []
                duped = 0
                for k, ms in points.items():
                    if '.Power.' in k or 'Dc.0.Current' in k or 'Dc.0.Voltage' in k:
                        tbw += ms
                        continue
                    value = sum(v['fields']['value'] for v in ms) / len(ms)
                    ms[0]['fields']['value'] = value
                    duped += len(ms) - 1
                    tbw.append(ms[0])
                log.info('Write %d points (across %d unique measurements), Deduped %d, Interval %.3fs' % (len(tbw), len(points), duped, interval))
                # print(points.keys())
                if not self._dryrun:
                    latency = time.time()
                    self._influx.write_points(tbw)
                    latency = time.time() - latency
                    log.info('Latency %dms' % (latency*1000))
                else:
                    log.debug('  Skip write due to dryrun.')
                points = defaultdict(list)
                # agg = defaultdict(dict) ??
            log.info('Messages handled: %d, Messages ignored %d' % (self._msg_count, self._msg_ignored))

def main():
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)

    log.info('Starting up')

    import argparse
    parser = argparse.ArgumentParser(description='Bridge MQTT messages from Venus GX to Influx DB with some smart sampling..')
    parser.add_argument('--dryrun', action='store_true',
                        help='do not publish to influx')
    args = parser.parse_args()
    if args.dryrun:
        log.warning('Running in dryrun mode')
    MqttToInflux(dryrun=args.dryrun)

main()