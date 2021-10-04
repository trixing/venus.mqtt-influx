"""
Module to read Venus GX messages from the dbus MQTT broker
and write them to Influx in a format which is easy to
process for Grafana monitoring.
"""
import influxdb
import paho.mqtt.client as mqtt
from datetime import datetime
import json
import logging
import queue
import requests
import sys
import threading
import traceback
import time
from collections import defaultdict

log = logging.getLogger('mqtt_to_influx')


class MqttToInflux:
   def __init__(self, mqtt_host='127.0.0.1', influx_host='127.0.0.1',
                influx_db='venus', dryrun=False):
    self._points = queue.Queue(maxsize=100)
    self._msg_count = 0
    self._msg_ignored = 0
    self._msg_dropped = 0
    self._msg_fail = 0
    self._msg_seen = set()
    self._dryrun = dryrun
    self._keepalive = set()
    self._active = True

    t = threading.Thread(target=self.safe_write)
    t.daemon = True
    t.start()

    t = threading.Thread(target=self.safe_keepalive)
    t.daemon = True
    t.start()

    self._influx = influxdb.InfluxDBClient(
            host=influx_host, port=8086,
            timeout=5, retries=1)
    if not self._dryrun:
        self._influx.create_database(influx_db)
        self._influx.switch_database(influx_db)

    self._mqtt = mqtt.Client()
    self._mqtt.on_connect = self.on_connect
    self._mqtt.on_message = self.on_message
    self._mqtt.on_subscribe = self.on_subscribe
    self._mqtt.connect(mqtt_host, 1883, 60)
    try:
        self._mqtt.loop_forever()
    except Exception as e:
        log.error('Exception: %s' % type(e))
        traceback.print_exc()
    finally:
        self._active = False

   def quit(self):
       self._active = False
       self._mqtt.disconnect()

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
    if t.endswith('system/0/Serial'):
        self._keepalive.add(t)
    # print(t, m, v, type(v))
    if type(v) not in [float, int]:
        self._msg_ignored += 1
        if type(v) == type(None):
            pass
        elif t not in self._msg_seen:
            log.info('Ignoring %s of type %s' % (t, type(v)))
            self._msg_seen.add(t)
        else:
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
        log.error('Queue full, overload? - dropping all')
        self._msg_dropped += self._points.qsize()
        self._points.clear()

   def safe_keepalive(self):
       try:
           self.keepalive()
       except Exception as e:
           log.error('Keepalive Exception %s' % e)
           traceback.print_exc()
           self.quit()

   def keepalive(self):
       # Wait for the first host to appear, to prevent
       # startup delay.
       while not self._keepalive:
           time.sleep(.1)
       while self._active:
           for t in self._keepalive:
               log.info('Send keepalive to %s' % t)
               self._mqtt.publish(t)
           time.sleep(25)

   def safe_write(self):
       try:
           self.write()
       except Exception as e:
           log.error('Write Exception %s' % type(e))
           traceback.print_exc()
           self.quit()

   def write(self):
      lastwrite = time.time()
      deduped = 0
      points = defaultdict(list)
      agg = defaultdict(dict)
      while self._active:
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
                    try:
                        self._influx.write_points(tbw)
                    except requests.exceptions.ConnectionError:
                        log.error('Write failure, dropping: %d' % len(tbw))
                        self._msg_fail += len(tbw)
                    latency = time.time() - latency
                    log.info('Latency %dms' % (latency*1000))
                else:
                    log.debug('  Skip write due to dryrun.')
                points = defaultdict(list)
            log.info('Messages handled: %d, ignored %d, dropped %d, failed %d' % (self._msg_count, self._msg_ignored, self._msg_dropped, self._msg_fail))

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
    parser = argparse.ArgumentParser(
            description='Bridge MQTT messages from Venus GX to Influx DB with some smart sampling..')
    parser.add_argument('--dryrun', action='store_true',
                        help='do not publish to influx')
    parser.add_argument('--mqtt_host', help='MQTT host to connect to', default='127.0.0.1')
    parser.add_argument('--influx_host', help='Influx host to connect to', default='127.0.0.1')
    parser.add_argument('--influx_db', help='Influx db to connect to', default='venus')

    args = parser.parse_args()
    if args.dryrun:
        log.warning('Running in dryrun mode')
    MqttToInflux(mqtt_host=args.mqtt_host, influx_host=args.influx_host,
                 influx_db=args.influx_db, dryrun=args.dryrun)

main()
