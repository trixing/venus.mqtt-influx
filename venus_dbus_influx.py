"""
Module to read Venus GX messages from the dbus
and write them to a server to process.
"""

from datetime import datetime, timedelta
import json
import logging
import os
import queue
import requests
import socket
import sys
import threading
import traceback
import time
from collections import defaultdict
from http.server import HTTPServer, BaseHTTPRequestHandler

import dbus

try:
  import gobject
except ImportError:
  from gi.repository import GLib as gobject

sys.path.insert(
    1,
    os.path.join(
        os.path.dirname(__file__),
        "/opt/victronenergy/dbus-systemcalc-py/ext/velib_python",
    ),
)
# from vedbus import VeDbusService
import dbusmonitor
from vedbus import VeDbusItemImport


INTERVAL=30

log = logging.getLogger('dbus_to_influx')


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


TOPICS = (
        '/Ac/Energy/Forward',
        '/Ac/Energy/Reverse',
        '/Ac/L1/Energy/Forward',
        '/Ac/L1/Energy/Reverse',
        '/Ac/L2/Energy/Forward',
        '/Ac/L2/Energy/Reverse',
        '/Ac/L3/Energy/Forward',
        '/Ac/L3/Energy/Reverse',
        #'/Ac/L1/Power',
        #'/Ac/L2/Power',
        #'/Ac/L3/Power',
        '/Ac/Power',
        '/CustomName',
        '/Dc/0/Power',
        '/Dc/0/Temperature',
        '/Dc/Battery/Soc',
        '/Energy/Forward',
        '/Energy/Reverse',
        '/Energy/AcIn1ToAcOut',
        '/Energy/AcIn1ToInverter',
        '/Energy/AcIn2ToAcOut',
        '/Energy/AcIn2ToInverter',
        '/Energy/AcOutToAcIn1',
        '/Energy/AcOutToAcIn2',
        '/Energy/InverterToAcIn1',
        '/Energy/InverterToAcIn2',
        '/Energy/InverterToAcOut',
        '/Energy/OutToInverter',
        '/ErrorCode',
        '/Frequency',
        '/Info/BatteryLowVoltage',
        '/Info/MaxChargeCurrent',
        '/Info/MaxChargeVoltage',
        '/Info/MaxDischargeCurrent',
        '/MaxCurrent',
        '/Power',
        '/Pv/I',
        '/Pv/V',
        '/ProductName',
        '/SetCurrent',
        '/Soc',
        '/Status',
        '/System/MaxCellVoltage',
        '/System/MinCellVoltage',
        '/System/MaxVoltageCellId',
        '/System/MinVoltageCellId',
        '/Temperature',
        '/Temperature1',
        '/Temperature2',
        '/Yield/Power',
        '/Yield/User',
        '/Yield/System',

)


class AllDbusMonitor(dbusmonitor.DbusMonitor):

  def service_wanted(self, serviceName):
    print('service_wanted', serviceName)
    return True


class DbusToIngest:
   def write_points(self, tbw):
     requests.post(self._url, json=tbw, headers={'Token': self._token}, timeout=5)

   def allowed(self, topic):
     return (
        topic and any(
        topic.endswith(t) for t in TOPICS
     ))

   def value_changed_on_dbus(self, dbusServiceName, dbusPath, dict, changes, deviceInstance):
      self._stats['msg']['count'] += 1
      p = dbusServiceName.split('.')
      strInstance = p[2] 
      path = '.'.join(p[0:3])

      m = str(dbusPath) 
      a = self.allowed(m)
      if not a:
        self._stats['msg']['ignored'] += 1
        return

      m = m[1:].replace("/", ".")
      v = changes['Value']
      log.debug('%s %s %s %s %s' % (path, deviceInstance, m, type(v), v))

      point = {
        "measurement": m,
        "tags": {
            "path": path,
            "instanceNumber": str(deviceInstance),
            "portalId": self._portal_id,
        },
        "time": datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ'),
        "fields": {
            # value
            # text
        }
      }
      if type(v) in (float, int):
        v = float(v)  # automatic conversion sometimes makes it an int
        point['fields']['value'] = v
      elif type(v) == str:
        point['fields']['text'] = v
      try:
        self._points.put(point, block=False)
      except queue.Full:
        log.error('Queue full, overload? - dropping all')
        self._stats['msg']['dropped'] += self._points.qsize()

   def device_added(self, a, b):
      print('device added', a, b)
      pass

   def device_removed(self, a, b):
      print('device removed', a, b)        
      pass
     
   def __init__(self, portal_id, ingest_host='127.0.0.1',
                token='unset', dryrun=False, stats_port=None):
    self._portal_id = portal_id
    self._points = queue.Queue(maxsize=10000)
    self._msg_seen = set()
    self._stats = {
            'msg': {
                'count': 0,
                'ignored': 0,
                'dropped': 0,
                'failed': 0,
                },
            'ingest': {
                'latency': 0,
                'writes': 0,
                'failed': 0,
                },
            'report': datetime.utcnow()
    }
    self._dryrun = dryrun
    self._keepalive = set()
    self._active = True

    self._httpd = None
    if stats_port:
        server_address = ('', stats_port)
        self._httpd = HTTPServer(server_address, Stats)
        self._httpd.data = self._stats
        t = threading.Thread(target=self._httpd.serve_forever)
        t.daemon = True
        t.start()

    self._url = 'https://%s/ingest' % ingest_host
    self._token = token

    dummy = {'code': None, 'whenToLog': 'onIntervalAlways', 'accessLevel': None}
    monitor_target='com.victronenergy.battery'
    monitorlist = {                                        
                        monitor_target: {                                                    
                            '/Dc/0/Temperature': dummy,          
                        },                                      
                  }                                                              
    self._v = {}                                                    
    self._dm = AllDbusMonitor({},                                            
           self.value_changed_on_dbus,                     
           deviceAddedCallback=self.device_added,                                        
           deviceRemovedCallback=self.device_removed)

    serviceNames = self._dm.dbusConn.list_names()
    serviceNames = set(['.'.join(s.split('.')[0:3]) for s in serviceNames if s.startswith('com.victronenergy.')])
    self.monitorlist = {}
    for s in serviceNames:
      self.monitorlist[s] = dict([(t, dummy) for t in TOPICS])
    self._dm = AllDbusMonitor(self.monitorlist,                                            
           self.value_changed_on_dbus,                     
           deviceAddedCallback=self.device_added,
           deviceRemovedCallback=self.device_removed)

    for service_name, instance in self._dm.get_service_list().items():
        short_name = '.'.join(service_name.split('.')[:3])
        if short_name not in self.monitorlist:
            continue
        for k in self.monitorlist[short_name]:
            value = self._dm.get_value(service_name, k, None)
            if value is not None:
                log.info('Initial value for %s(%s) %s: %s', service_name, instance, k, value)
                changes = {'Value': value, 'Text': str(value)}
                self.value_changed_on_dbus(service_name, k, {}, changes, instance)
    
    self.timer = datetime.utcnow()
    log.info("Startup finished")
    gobject.timeout_add(INTERVAL*1000, self.safe_write)

   def quit(self):
       self._active = False
       if self._httpd:
           self._httpd.shutdown()

   def on_message(self, client, userdata, msg):
    self._stats['msg']['count'] += 1

    if type(v) in [float, int, bool] and self.allowed(t):
        v = float(v)
    elif type(v) in [str] and self.allowed(t):
        pass
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
            "instanceNumber": p[3] if len(p) > 3 else "",
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

   def safe_write(self):
       try:
           self.write()
       except Exception as e:
           log.error('Write Exception %s' % type(e))
           traceback.print_exc()
#       self.quit()
       return True

   def write(self):
      deduped = 0
      unchanged = 0
      points = defaultdict(list)
      agg = defaultdict(dict)
      changed = dict()
      timer = datetime.utcnow()
      interval = timer - self.timer
      self.timer = timer
      self.unchanged_timer = timer + timedelta(hours=1)
      while True:
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
            break

      if True:

        if True:
            # this is slightly wrong and should be corrected by INTERVAL/2
            # also it would be nice to run this on a full 10s interval
            dt = timer.strftime('%Y-%m-%dT%H:%M:%SZ')
            if points:
                tbw = []
                duped = 0
                unchanged = 0
                for k, ms in points.items():
                    # These are sampled on every datapoint
                    #if '.Power.' in k or 'Dc.0.Current' in k or 'Dc.0.Voltage' in k:
                    #   tbw += ms
                    #    continue
                    # Everything else is aggregated to a mean value
                    if ms[0]['fields'].get('value', None) is not None:
                        value = sum(v['fields']['value'] for v in ms) / len(ms)
                        ms[0]['fields']['value'] = value
                    elif ms[0]['fields'].get('text', None) is not None:
                        # Don't need to do anything, just take the first value
                        # TODO(jdi): Should be mode probably.
                        value = ms[0]['fields'].get('text', None)
                        pass
                    else:
                        value = None
                    duped += len(ms) - 1
                    ms[0]['time'] = dt
                    if k in changed and changed[k] == value:
                      unchanged += 1
                    else:
                      tbw.append(ms[0])
                      changed[k] = value
                log.info('Write %d points (across %d unique measurements), Deduped %d, Unchanged %d, Interval %.3fs' % (
                    len(tbw), len(points), duped, unchanged, interval.total_seconds()))
                # print(points.keys())
                if not self._dryrun:
                    latency = time.time()
                    try:
                        self.write_points(tbw)
                        self._stats['ingest']['writes'] += 1
                    except requests.exceptions.RequestException as e:
                        log.error('Write failure %s, dropping: %d' % (type(e), len(tbw)))
                        self._stats['msg']['failed'] += len(tbw)
                        self._stats['ingest']['failed'] += 1
                    latency = time.time() - latency
                    self._stats['ingest']['latency'] = (latency + 9*self._stats['ingest']['latency'])/10
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
            description='Bridge MQTT messages from Venus GX to a compatible HTTP server with some smart sampling..')
    parser.add_argument('--dryrun', action='store_true',
                        help='do not publish values')
    parser.add_argument('--portal_id', help='Venus Portal ID for logging')
    parser.add_argument('--ingest_host', help='Ingestion host to connect to', default='127.0.0.1')
    parser.add_argument('--port', help='Status report port', default=8071)
    parser.add_argument('--token', help='Token to authorize ingestion', default=os.getenv('TOKEN', socket.gethostname()))

    args = parser.parse_args()
    if args.dryrun:
        log.warning('Running in dryrun mode')

    from dbus.mainloop.glib import DBusGMainLoop
    # Have a mainloop, so we can send/receive asynchronous calls to and from dbus
    DBusGMainLoop(set_as_default=True)

    DbusToIngest(portal_id=args.portal_id, ingest_host=args.ingest_host,
                 token=args.token,
                 dryrun=args.dryrun, stats_port=int(args.port))

    logging.info('Connected to dbus, and switching over to gobject.MainLoop() (= event based)')
    mainloop = gobject.MainLoop()
    mainloop.run()

if __name__ == "__main__":
    main()
