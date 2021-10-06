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
from http.server import HTTPServer, BaseHTTPRequestHandler


log = logging.getLogger('mqtt_to_influx')


class Stats(BaseHTTPRequestHandler):

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        if hasattr(self.server, 'data'):
            self.wfile.write(json.dumps(self.server.data).encode())
        else:
            self.wfile.write(json.dumps({"error": "No data defined"}))

    def log_message(self, format, *args):
        log.info("%s - %s" % (
            self.address_string(), format%args))


class MqttToInflux:
   def __init__(self, mqtt_host='127.0.0.1', influx_host='127.0.0.1',
                influx_db='venus', dryrun=False, stats_port=None):
    self._points = queue.Queue(maxsize=100)
    self._msg_seen = set()
    self._stats = {
            'msg': {
                'count': 0,
                'ignored': 0,
                'dropped': 0,
                'failed': 0,
                },
            'influx': {
                'latency': 0,
                'writes': 0,
                'failed': 0,
                },
    }
    self._dryrun = dryrun
    self._keepalive = set()
    self._active = True

    t = threading.Thread(target=self.safe_keepalive)
    t.daemon = True
    t.start()

    t = threading.Thread(target=self.safe_write)
    t.daemon = True
    t.start()

    self._httpd = None
    if stats_port:
        server_address = ('', stats_port)
        self._httpd = HTTPServer(server_address, Stats)
        self._httpd.data = self._stats
        t = threading.Thread(target=self._httpd.serve_forever)
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
    self._mqtt.on_disconnect = self.on_disconnect
    self._mqtt.on_message = self.on_message
    self._mqtt.on_subscribe = self.on_subscribe

    while self._active:
        try:
            self._mqtt.connect(mqtt_host, 1883, 60)
            self._mqtt.loop_forever()
        except Exception as e:
            log.error('MQTT Exception: %s' % type(e))
            traceback.print_exc()
            time.sleep(1)

    self.quit()

   def quit(self):
       self._active = False
       if self._httpd:
           self._httpd.shutdown()
       self._mqtt.disconnect()

   def on_connect(self, client, userdata, flags, rc):
    log.info('Connected to mqtt')
    client.subscribe('N/#')

   def on_disconnect(self, client, userdata, rc):
    log.info('Disconnected from mqtt')

   def on_subscribe(self, client, userdata, flags, rc):
    log.info('MQTT subscription successful.')

   def on_message(self, client, userdata, msg):
    self._stats['msg']['count'] += 1
    t = msg.topic
    p = t.split('/')
    m = '.'.join(p[4:])
    v = json.loads(msg.payload)['value']
    if t.endswith('system/0/Serial'):
        self._keepalive.add(t)
    # print(t, m, v, type(v))
    if type(v) in [float, int, bool]:
        v = float(v)
    else:
        self._stats['msg']['ignored'] += 1
        if type(v) == type(None):
            pass
        elif t not in self._msg_seen:
            log.info('Ignoring %s of type %s' % (t, type(v)))
            self._msg_seen.add(t)
        else:
            log.debug('Ignoring %s of type %s' % (t, type(v)))
        return
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
            # value
            # text
        }
    }
    if type(v) == float:
        v = float(v)  # automatic conversion sometimes makes it an int
        point['fields']['value'] = v
    elif type(v) == str:
        point['fields']['text'] = v

    try:
        self._points.put(point, block=False)
    except queue.Full:
        log.error('Queue full, overload? - dropping all')
        self._stats['msg']['dropped'] += self._points.qsize()
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
       n = 0
       interval = 30
       while self._active:
           for t in self._keepalive:
               log.info('Send keepalive to %s' % t)
               self._mqtt.publish(t)
           n += 1
           if n >= 300/interval:
               n = 0
               # Disconnect a reconnect, which forces
               # a publish of all values every 5 minutes.
               self._mqtt.disconnect()
           time.sleep(interval)

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
                    # These are sampled on every datapoint
                    if '.Power.' in k or 'Dc.0.Current' in k or 'Dc.0.Voltage' in k:
                        tbw += ms
                        continue
                    # Everything else is aggregated to a mean value
                    if ms[0]['fields'].get('value', None) is not None:
                        value = sum(v['fields']['value'] for v in ms) / len(ms)
                        ms[0]['fields']['value'] = value
                    elif ms[0]['fields'].get('text', None) is not None:
                        # Don't need to do anything, just take the first value
                        # TODO(jdi): Should be mode probably.
                        pass
                    duped += len(ms) - 1
                    tbw.append(ms[0])
                log.info('Write %d points (across %d unique measurements), Deduped %d, Interval %.3fs' % (
                    len(tbw), len(points), duped, interval))
                # print(points.keys())
                if not self._dryrun:
                    latency = time.time()
                    try:
                        self._influx.write_points(tbw)
                        self._stats['influx']['writes'] += 1
                    except requests.exceptions.RequestException as e:
                        log.error('Write failure %s, dropping: %d' % (type(e), len(tbw)))
                        self._stats['msg']['failed'] += len(tbw)
                        self._stats['influx']['failed'] += 1
                    latency = time.time() - latency
                    self._stats['influx']['latency'] = (latency + 9*self._stats['influx']['latency'])/10
                    log.info('Latency %dms' % (latency*1000))
                else:
                    log.debug('  Skip write due to dryrun.')
                points = defaultdict(list)
            log.info('Messages handled: %s' % (self._stats['msg']))

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
    parser.add_argument('--port', help='Status report port', default=8071)

    args = parser.parse_args()
    if args.dryrun:
        log.warning('Running in dryrun mode')

    MqttToInflux(mqtt_host=args.mqtt_host, influx_host=args.influx_host,
                 influx_db=args.influx_db, dryrun=args.dryrun,
                 stats_port=args.port)

main()
