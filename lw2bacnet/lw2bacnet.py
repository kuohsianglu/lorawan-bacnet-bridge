import os
import sys
import time
import flatdict
import yaml
import logging
import json
import base64
import binascii
import math
import quickjs
import socket
import shutil
import hashlib
import re
import fcntl
import struct
import csv
from pathlib import Path
from .bacnetdb import *

from paho.mqtt.client import Client
import BAC0
from BAC0.core.devices.local.models import ObjectFactory
from bacpypes.object import BinaryValueObject, BinaryInputObject, BinaryOutputObject, AnalogInputObject, AnalogOutputObject, AnalogValueObject

# -----------------------------------------------------------------------------
# Globals
# -----------------------------------------------------------------------------

APP_NAME = 'LoRaWAN to BACnet Bridge'
APP_VERSION = 'v1.0.0'
CFG_ROOT = '/etc/lw2bacnet'
DP_CSV = '/tmp/dp.csv'

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

class Config():

    _data = flatdict.FlatDict({})
    _file = f"{CFG_ROOT}/config/config.yml"
    _dirty = False

    def __init__(self):
        try:
            with open(self._file, "r") as f:
                _data =  yaml.load(f, Loader=yaml.loader.SafeLoader)
        except FileNotFoundError:
            _data = {'logging': {'level' : logging.INFO}}
        self._data =  flatdict.FlatDict(_data, delimiter='.')

    def get(self, name, default=None):

        # Environment variables have precedence over `config.yml` but do not get persisted
        env_name = name.upper().replace('.', '_').replace('-', '_')
        value = os.environ.get(env_name)

        # Get the value from `config.yml` or the default, and persist
        if not value:
            value = self._data.get(name, default)
            self.set(name, value)

        return value

    def set(self, name, value):
        if self._data.get(name) != value:
            self._data[name] = value
            self._dirty = True

    def save(self):
        if self._dirty:
            try:
                with open(self._file, "w") as f:
                    yaml.dump(self._data.as_dict(), f, default_flow_style=False)
                self._dirty = False
            except FileNotFoundError:
                None

    def unflat(self):
        return self._data.as_dict()

    def dump(self):
        print(json.dumps(self._data.as_dict(), sort_keys=True, indent=4))


# -----------------------------------------------------------------------------
# MQTT
# -----------------------------------------------------------------------------

class MQTTClient(Client):

    MQTTv31 = 3
    MQTTv311 = 4
    MQTTv5 = 5

    def __init__(self, broker="localhost", port=1883, username=None, password=None, userdata=None):

        def connect_callback_default(client, userdata, flags, rc):
            if rc == 0:
                logging.debug("[MQTT] Connected to MQTT Broker!")
                self.subscribe(config.get('mqtt.topic', 'v3/+/devices/+/up'))
            else:
                logging.error("[MQTT] Failed to connect, return code %d", rc)

        def message_callback_default(client, userdata, msg):
            logging.debug("[MQTT] Received `%s` from `%s` topic", msg.payload, msg.topic)

        def subscribe_callback_default(client, userdata, mid, granted_qos):
            logging.debug("[MQTT] Subscribed")

        def disconnect_callback_default(client, userdata, rc):
            logging.debug("[MQTT] Disconnected from MQTT Broker!")

        Client.__init__(self,
            client_id = "",
            clean_session = None,
            userdata = userdata,
            protocol = self.MQTTv311,
            transport = "tcp",
            reconnect_on_failure = True
        )

        self.on_connect = connect_callback_default
        self.on_disconnect = disconnect_callback_default
        self.on_message = message_callback_default
        self.on_subscribe = subscribe_callback_default
        if username and password:
            self.username_pw_set(username, password)
        self.connect(broker, port)

    def run(self):
        self.loop_start()

# -----------------------------------------------------------------------------
# BACnet
# -----------------------------------------------------------------------------

class BACnetApp():

    id = 0

    def __init__(self):
        self.objects = None
        self.device = None
        self.mqtt = None

    def create_device(self, ip=None, port=None, mask=24, **params):
        self.device = BAC0.lite(ip=ip, port=port, mask=mask, **params)
        self.device.this_application.wp_callback_init(wp_complete)

    def setLoggingLevel(self, level):
        self.device._log.setLevel(level)
        self.device._update_local_cov_task.task._log.setLevel(level)

    def set_mqtt_client(self, client):
        self.mqtt = client

    def add_object(self, type, name, description, value, units, bid):
        prop = {"units": units}
        bin_obj = re.compile("binary*")
        if bin_obj.match(type.objectType):
            prop = None

        self.objects = ObjectFactory(
            type, int(bid), name,
            properties = prop,
            description = description,
            presentValue = value
        )
        self.id = int(bid)

    def clear_objects(self):
        if self.objects:
            self.objects.clear_objects()
        self.id = 0

    def list(self):
        for obj in self.device.this_application.iter_objects():
            print(obj.objectName)

    def unload(self):
        if self.objects:
            for k, v in self.objects.objects.items():
                self.device.this_application.delete_object(v)

    def load(self):
        if self.objects:
            self.objects.add_objects_to_application(self.device)

    def run(self):
        self.setLoggingLevel(logging.INFO)
        while True:
            time.sleep(0.1)

# -----------------------------------------------------------------------------
# Utils
# -----------------------------------------------------------------------------

def get_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(0)
    try:
        # doesn't even have to be reachable
        s.connect(('8.8.8.8', 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = '127.0.0.1'
    finally:
        s.close()
    return IP


def get_mask():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        iface = "eth0"
        subnet_mask = socket.inet_ntoa(fcntl.ioctl(s.fileno(), 0x891b, struct.pack('256s', bytes(iface, 'utf-8')))[20:24])
    except Exception:
        subnet_mask = '255.255.255.0'

    return(sum([ bin(int(bits)).count("1") for bits in subnet_mask.split(".") ]))

def get_netmask(ifname):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    return socket.inet_ntoa(fcntl.ioctl(s.fileno(), 0x891b, struct.pack('256s',ifname))[20:24])

def get_data(msg, force_decode=False, decoder='cayenne.js'):

    # Get payload in JSON
    payload = json.loads(msg.payload)

    # Find out what LNS we have
    if "uplink_message" in payload:
        payload_raw = base64.b64decode(payload['uplink_message']['frm_payload'])
        payload_decoded = payload['uplink_message'].get('decoded_payload', False)
        port = payload['uplink_message']['f_port']
        gateways = payload['uplink_message']['rx_metadata']
    else:
        try:
            payload_raw = base64.b64decode(payload['data'])
        except Exception:
            logging.error(f"[Decode] payload without data field")
            payload_raw = []
        payload_decoded = payload.get('object', False)
        port = payload['fPort']
        gateways = payload['rxInfo']

    if (force_decode) or (payload_decoded == False):

        # Decode raw payload
        decoder_file = f'{CFG_ROOT}/config/decoders/{decoder}'
        with open(decoder_file) as f:
            decoder = f.readlines()
        context = quickjs.Context()
        context.eval(''.join(decoder))
        context.eval("""
            function f(bytes, port) {
                input = {'bytes': bytes, 'fPort': port};
                return decodeUplink(input);
            }
        """)
        command = "f([{}], {})".format(",".join([str(b) for b in payload_raw]), port)

        try:
            response = json.loads(context.eval(command).json())
        except Exception:
            logging.error(f"[Decode] {decoder_file} parsing error!")
            response = {'data':[]}

        response = response['data']

    else:

        # get the pre-decoded payload
        response = payload_decoded

    return response

def get_device_id(msg):
    return msg.topic.split('/')[3]

def load_bacnet_devices():
    dump_dp_to_csv()

    # Unload them all first
    bacnet_app.unload()
    bacnet_app.clear_objects()

    if os.path.exists(f"{DP_CSV}"):
        logging.debug(f"[CSV] file exist {DP_CSV}")
        with open(f"{DP_CSV}", newline='') as f:
            reader = csv.reader(f)
            prev_eui = ""
            for row in reader:
                dev_eui = row[1].removeprefix('\\x')
                dp_id = row[0]
                dp_name = row[2]

                if dev_eui != prev_eui:
                    devname = get_dev_name(dev_eui)

                dpname = get_dp_name(dp_id)
                oname = f"{devname}:{dpname}"

                obj_type = row[3]
                obj_name = oname
                obj_desc = dp_name
                obj_units = row[4]
                obj_id = row[8]
                if len(row[4]) == 0:
                    obj_units = "noUnits"
                bacnet_app.add_object(
                    type = globals()[obj_type],
                    name = obj_name,
                    description = obj_desc,
                    value = 0,
                    units = obj_units,
                    bid = obj_id
                )
                prev_eui = dev_eui

    bacnet_app.load()

def update_object(device, device_id, element):

    save = False
    datatype = element.get('type', 0)
    value = element.get('value', 0)
    ch = element.get('channel', 0)

    object_id = f"{device_id}-{ch}"
    devname = get_dev_name(device_id)
    dpname = get_dp_name(object_id)
    oname = f"{devname}:{dpname}"
    bid = get_bacnet_id(object_id)

    try:

        # Update the BACnet object value
        device[oname].presentValue = value
        bacnetdb_update_datapoint(object_id, value)

        logging.debug(f"[DB] Update Obj {bid}-{oname}: {value}")

    except:

        logging.debug(f"[BACNET] Object {oname} not found, created it")

        obj_type = get_dp_type(object_id)
        obj_units = get_dp_units(object_id)
        obj_id = get_bacnet_id(object_id)

        if obj_units == None:
            obj_units = "noUnits"

        bacnet_app.add_object(
            type = globals()[obj_type],
            name = oname,
            description = dpname,
            value = value,
            units = obj_units,
            bid = obj_id
        )

        save = True

    return save

def update_objects(device, msg):

    logging.debug(f"[MQTT] Message received for {msg.topic}")

    device_id = get_device_id(msg)
    decode = True
    decoder = get_decoder(device_id)
    codec_file = f'{CFG_ROOT}/config/decoders/{decoder}'

    if not os.path.exists(codec_file):
        dev_codec = load_dev_codec(device_id)
        if dev_codec == None:
            decoder = "cayenne.js"
            logging.debug(f"[Codec] No Codec. Use default decoder.")
        else:
            with open(codec_file, 'a') as file:
                file.write(dev_codec.strip())

    data = get_data(msg, decode, decoder)
    logging.debug(f"[MQTT] Message from {device_id}: {data}")
    save = False

    for element in data:
        save |= update_object(device, device_id, element)

    if save:
        bacnet_app.load()

def load_dev_codec(device_id):
    try:
        with open(f'{CFG_ROOT}/config/devices/{device_id}.yml', "r") as f:
            data =  yaml.load(f, Loader=yaml.loader.SafeLoader)
        return data['codec']
    except FileNotFoundError:
        logging.error(f"[MAIN] Could not load {device_id}.yml file")

    return None

def copy_recursive(source_base_path, target_base_path):

    for item in os.listdir(source_base_path):

        # Directory
        if os.path.isdir(os.path.join(source_base_path, item)):

            # Create destination directory if needed
            new_target_dir = os.path.join(target_base_path, item)
            try:
                os.mkdir(new_target_dir)
            except OSError:
                None

            # Recurse
            new_source_dir = os.path.join(source_base_path, item)
            copy_recursive(new_source_dir, new_target_dir)

        # File
        else:
            # Copy file over (not overwriting)
            source_name = os.path.join(source_base_path, item)
            target_name = os.path.join(target_base_path, item)
            if not Path(target_name).is_file():
                shutil.copy(source_name, target_name)

def get_app_id(dev_eui):
    db_cmd = f"sudo -u postgres /usr/bin/psql -h localhost --no-align --quiet --tuples-only -c"
    query_cmd = f"\"SELECT application_id FROM device WHERE dev_eui=bytea '\\x{dev_eui}'\" chirpstack"
    psql_cmd = f"{db_cmd} {query_cmd}"
    appid = os.popen(psql_cmd).read().strip('\n')

    return appid

def get_dev_name(dev_eui):
    db_cmd = f"sudo -u postgres /usr/bin/psql -h localhost --no-align --quiet --tuples-only -c"
    query_cmd = f"\"SELECT name FROM device WHERE dev_eui=bytea '\\x{dev_eui}'\" chirpstack"
    psql_cmd = f"{db_cmd} {query_cmd}"
    devname = os.popen(psql_cmd).read().strip('\n')

    return devname

def get_dev_eui_by_name(dev_name):
    db_cmd = f"sudo -u postgres /usr/bin/psql -h localhost --no-align --quiet --tuples-only -c"
    query_cmd = f"\"SELECT dev_eui FROM device WHERE name='{dev_name}'\" chirpstack"
    psql_cmd = f"{db_cmd} {query_cmd}"
    deveui = os.popen(psql_cmd).read().strip('\n')

    return deveui.removeprefix('\\x')

def encode_data(deveui, channel, value):
    decoder = get_decoder(deveui)
    decoder_file = f'{CFG_ROOT}/config/decoders/{decoder}'
    with open(decoder_file) as f:
        decoder = f.readlines()
    context = quickjs.Context()
    context.eval(''.join(decoder))
    context.eval("""
        function f(ch, val) {
            var chanUnit = ChanDict[ch];
            var ipso = Dict[chanUnit.type];
            var value = ipso.encoder(val);
            var bytes = [];
            bytes.push(ch);
            bytes.push(chanUnit.type);
            bytes = bytes.concat(value);
            return bytes;
        }
    """)
    command = "f({}, {})".format(channel, value)
    logging.debug(f"[Encode] cmd: {command}")

    response = json.loads(context.eval(command).json())
    logging.debug(f"[Encode] resp: {response}")
    return response

def lorawan_dl_msg(dev_eui, f_port, channel, value):
    app_id = get_app_id(dev_eui)
    mqtt_topic = f"application/{app_id}/device/{dev_eui}/command/down"

    raw_data = encode_data(dev_eui, channel, value)

    ch = format(channel, '02x')
    val = format(raw_data[2], '02x')
    type = format(raw_data[1], '02x')

    ipso_str = f"{ch}{type}{val}"
    ipso_bytes = bytes.fromhex(ipso_str)
    data_b64 = base64.b64encode(ipso_bytes)
    data_str = data_b64.decode()
    payload = {
        "devEui": dev_eui,
        "confirmed": True,
        "fPort": f_port,
        "data": data_str
    }

    logging.debug(f"[MQTT_PUB] raw data: {raw_data}, ipsostr: {ipso_str}")

    if bacnet_app.mqtt:
        bacnet_app.mqtt.publish(mqtt_topic, json.dumps(payload))
        logging.debug(f"[MQTT_PUB] topic: {mqtt_topic}, payload: {payload}")

def wp_complete(obj_name, obj_val, obj_id):
    if obj_val == "active":
        val = 1
    elif obj_val == "inactive":
        val = 0
    else:
        val = obj_val

    bid = obj_id[1]
    dp_id = get_dp_id(bid)
    logging.debug(f"[DL] BacnetID: {bid}  dpid: {dp_id} ")

    deveui = dp_id.split('-')[0]
    ch_str = dp_id.split('-')[1]
    ch = int(ch_str)

    profile_id = get_profile_id(deveui)
    fport = get_fport(profile_id, ch)

    if fport == None:
        logging.debug(f"[DL] Unable to write data point, invalid fport!")
        return

    logging.debug(f"[DL] profile ID: {profile_id} Downlink fport: {fport}")
    lorawan_dl_msg(deveui, fport, ch, val)


def main():

    run = True

    # Copy defaults
    copy_recursive(f"{CFG_ROOT}/templates", f"{CFG_ROOT}/config")

    global config
    global bacnet_app

    config = Config()
    bacnet_app = BACnetApp()

    # Set logging level based on settings (10=DEBUG, 20=INFO, ...)
    level=config.get("logging.level", logging.INFO)
    logging.basicConfig(format='[%(asctime)s] %(message)s', level=level)
    logging.info(f"[MAIN] {APP_NAME} {APP_VERSION}")
    logging.debug(f"[MAIN] Setting logging level to {level}")

    # BACnet setup
    try:
        bacnet_app.create_device(
            ip=config.get('bacnet.ip', get_ip()),
            port=config.get('bacnet.port', 47808),
            mask=config.get('bacnet.mask', get_mask()),
            deviceId=config.get('bacnet.devid', 9000),
            vendorName=config.get('bacnet.vendor', 'RAKwireless'),
            localObjName=config.get('bacnet.objname', 'WisGateV2'),
            description=config.get('bacnet.desc', 'LoRaWAN BACnet Gateway'),
            modelName=config.get('bacnet.model', 'WisGateV2 BACnet Gateway'),
            firmwareRevision=config.get('bacnet.fwver', '2.0.0'),
        )
        load_bacnet_devices()
    except:
        logging.error(f"[BACNET] Error defining BACnet interface at {config.get('bacnet.ip', get_ip())}:{config.get('bacnet.port', 47808)}")
        run = False

    # MQTT setup
    try:
        def mqtt_message_callback(client, userdata, msg):
            update_objects(userdata, msg)
        mqtt_client = MQTTClient(
            config.get('mqtt.server', 'localhost'),
            int(config.get('mqtt.port', 1883)),
            config.get('mqtt.username'),
            config.get('mqtt.password'),
            userdata=bacnet_app.device
        )
        mqtt_client.on_message = mqtt_message_callback
        bacnet_app.set_mqtt_client(mqtt_client)
    except:
        logging.error(f"[MQTT] Error connecting to MQTT server at {config.get('mqtt.server', 'localhost')}:{config.get('mqtt.port', 1883)}")
        run = False

    # Save defaults
    config.save()

    # Run application
    if run:
        mqtt_client.run()
        bacnet_app.run()


if __name__ == "__main__":
    main()
