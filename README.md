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
