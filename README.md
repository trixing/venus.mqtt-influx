# venus.mqtt-influx - A Smart MQTT to Influx Bridge

This program takes the messages as produced by the dbus-mqtt broker
in the Venus GX system and transforms them into a format which is
easy to use for graphing in Grafana.

It has some additional features

- By default new messages are averaged over 10 seconds to reduce
  the load on Influx
- Some messages (like Power in particular) are logged every second
  to allow high fidelity debugging
- For the phased measurements, L1, L2, L3 it will create another Lx
  datapoint as the sum (for Current and Power) or mean (for Voltage)
  of the values. This again is facilitate easier graphing.


## Downsampling

The default 1 second interval produces quite a lot of data. To
configure your influxdb with a reasonable retention policy and
aggressive downsampling use the included [Example](./influx_example.sql).

## Installation (Systemd)

On systemd systems, copy the supplied [Unit File](./venus-mqtt-influx.service.example)
to /etc/systemd/system/ . Optionally edit the file to add command line
arguments and adapt the installation path.

To start the service, issue
```
systemctl daemon-reload
systemctl start venus-mqtt-influx 
```

Log output is available with
```
journalctl -u venus-mqtt-influx
```

## Installation (Supervise)

If you want to run the script on the GX device, create a 
`/service/venus-mqtt-influx/` directory and add the following file
named `run`:
```
#!/bin/sh
python3 /data/venus-mqtt-influx/venus-mqtt-influx.py --influx_host=example.host
```

If you are on Venus OS < 2.80 you need to `opkg install python3` and a bunch
of dependencies (paho-mqtt, influx packages).
