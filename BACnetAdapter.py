import datetime, json, threading, sys
from bacpypes.app import BIPSimpleApplication, BIPForeignApplication
from bacpypes.apdu import WhoIsRequest
from bacpypes.object import get_datatype
from bacpypes.pdu import GlobalBroadcast, Address

from BACnetDevices import BACnetDevices
from BACnetSensors import BACnetSensors
from Device import Device
from MQTT import MQTT
from clearblade import auth, Client


class BACnetAdapter(BIPSimpleApplication):
    def __init__(self, device, hostname, args):
        BIPSimpleApplication.__init__(self, device, hostname)
        self.who_is_request = None
        self.credentials = args
        self.interval = args["whoisInterval"]
        self.low_limit = args["lowerDeviceIdLimit"]
        self.high_limit = args["upperDeviceIdLimit"]
        self.mqtt = None
        self.cb_device_client = None
        self.bacnet_devices = None
	self.bacnet_sensors = None
        self._init_cb()

    def _init_cb(self):
        # first authenticate to CB using device auth
        cb_auth = auth.Auth()
        if self.cb_device_client is None:
            self.cb_device_client = Client.DeviceClient(self.credentials['systemKey'], self.credentials['systemSecret'], self.credentials['deviceName'], self.credentials['activeKey'], self.credentials['platformURL'])
            cb_auth.Authenticate(self.cb_device_client)
        # init cb mqtt
        if self.mqtt is None:
            self.mqtt = MQTT(self.cb_device_client)
	    self.mqtt.PublishTopic("testing", "testing")
        # init bacnet devices (comes from cb collection)
        if self.bacnet_devices is None:
            self.bacnet_devices = BACnetDevices(self.cb_device_client, self)
        # init bacnet sensors (comes from cb devices table)
	if self.bacnet_sensors is None:
	    self.bacnet_sensors = BACnetSensors(self.cb_device_client, self)
        # also init bacnet sensor profiles

    def do_IAmRequest(self, apdu):
	self.bacnet_devices.got_new_device_who_is_response(apdu)

    def do_UnconfirmedCOVNotificationRequest(self, apdu):
	now = datetime.datetime.utcnow().isoformat()
	#there is a chance the sensor hasn't been registered yet at a device, and the sensor list being updated, which means this key won't exist. if this happens just throw away the value, it should get picked up on the next pass
	try:
	    sensor_name = self.bacnet_sensors.sensors[(str(apdu.pduSource), apdu.monitoredObjectIdentifier[1])]["name"]
	except KeyError:
	    print "cov for an unregistered device, just throwing away for now"
	    return

	print sensor_name
	msg_to_send = {
	    "time_stamp": now,
	    "name": sensor_name
	}
	for element in apdu.listOfValues:
	    if element.propertyIdentifier == "presentValue":
		datatype = get_datatype(apdu.monitoredObjectIdentifier[0], "presentValue")
		msg_to_send["present_value"] = element.value.cast_out(datatype)
	self.send_value_to_platform(msg_to_send)

    def send_value_to_platform(self, value_to_send):
        #obj_to_send = {
        #    'device': {
        #        'id': device.id,
        #        'name': device.name,
        #        'source': device.source
        #    },
        #    'object': obj,
        #    'properties': props
        #}
        try:
            msg = json.dumps(value_to_send, ensure_ascii=False, default=json_serial)
            self.mqtt.PublishTopic("bacnet/in", str(msg))
        except Exception as e:
            print e

    def start(self):
        print "in start"
        #self.who_is(self.low_limit, self.high_limit, Address("10.16.163.20"))
        # todo - here we will want to loop through each device we have, and kick off getting all objects and properties for the device
        #timer = threading.Timer(self.interval, self.start)
        #timer.daemon = True
        #timer.start()


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    #if isinstance(obj, TimeStamp):
    return str(obj)
