import datetime, time, threading, sys

from bacpypes.apdu import PropertyReference, ReadAccessSpecification, ReadPropertyMultipleRequest, ReadPropertyMultipleACK, ReadPropertyRequest, SubscribeCOVRequest
from bacpypes.iocb import IOCB
from bacpypes.object import get_datatype
from bacpypes.pdu import Address

from clearblade import Devices

from utils import decode_multiple_properties


class BACnetSensors:
    def __init__(self, cb_device_client, bacnet_adapter):
	self.sensors = {}
	self.cb_device_client = cb_device_client
	self.bacnet_adapter = bacnet_adapter
	self.cb_devices = Devices.Devices(self.cb_device_client)
	self.pending_poll_requests = []
	self.pending_new_sensors = []
	self._update_cb_devices()
	self._resub_existing_covs()
	self._start_polling()
	self._process_poll_requests()
	self._process_new_sensors()

    def add_new_sensors_from_device(self, sensors, device_info):
	# first filter out any trendLogs, since we don't need them
	print "found some new sensors to add"
	new_sensors = {}
	for sensor in sensors:
	    if sensor[0] != 'trendLog' and sensor[0] != 'device':
		key = (device_info["ip_address"], sensor[1])
		self.sensors[key] = {
		    "bacnet_object_identifier": sensor,
		    "present_value": None,
		    "update_method": None
		}
		self.pending_new_sensors.append((device_info["ip_address"].encode("utf-8"), sensor))

    def _process_new_sensors(self):
	timer = threading.Timer(15, self._process_new_sensors)
	timer.daemon = True
	num_to_process = len(self.pending_new_sensors)
	if num_to_process > 100:
	     num_to_process = 100
	count = 0
	print("processing {} new sensors".format(num_to_process))
	while count < num_to_process:
	    new_sensor = self.pending_new_sensors.pop(0)
	    props_to_get = ['objectName', 'description', 'presentValue', 'units']
	    prop_ref_list = []
	    for prop in props_to_get:
		ref = PropertyReference(
		    propertyIdentifier=prop
		)
		prop_ref_list.append(ref)
	    read_access_spec = ReadAccessSpecification(
		objectIdentifier=new_sensor[1],
		listOfPropertyReferences=prop_ref_list
	    )
	    request = ReadPropertyMultipleRequest(
		listOfReadAccessSpecs=[read_access_spec],
		destination=Address(new_sensor[0])
	    )
	    iocb = IOCB(request)
	    iocb.add_callback(self._got_props_for_new_object, new_sensor[1])
	    self.bacnet_adapter.request_io(iocb)
	    count += 1	    
	timer.start()

    def _got_props_for_new_object(self, iocb, obj_id):
	timestamp = datetime.datetime.utcnow().isoformat()
	if iocb.ioError:
	    print("error getting property list: {0}".format(str(iocb.ioError)))
	    return
	else:
	    apdu = iocb.ioResponse
	    if not isinstance(apdu, ReadPropertyMultipleACK):
		print("response was not ReadPropertyMultipleACK")
		return	
	    props_obj = decode_multiple_properties(apdu.listOfReadAccessResults)
	    # might be a better place to do this translation, but doing it here for now
	    update_method = "cov" if props_obj["objectName"].find("ZN-T") != -1 else "polling"
	    sensor_obj = {
		"description": props_obj["description"],
		"name": props_obj["objectName"],
		"present_value": props_obj["presentValue"],
		"time_stamp": timestamp,
		"bacnet_object_type": obj_id[0],
		"bacnet_identifier": obj_id[1],
	    	"units": props_obj["units"],
		"update_method": update_method,
	        "new": True,
	        "parent_device_ip": str(apdu.pduSource)
	    }
	    self.sensors[(str(apdu.pduSource), obj_id[1])].update(sensor_obj)
	    self.bacnet_adapter.send_value_to_platform(sensor_obj)
	    #we don't need to do anything if method is polling, because it will just automatically get picked up next cycle
	    if sensor_obj["update_method"] == "polling":
		print "polling time"
	    elif sensor_obj["update_method"] == "cov":
		self._cov_subscribe(str(apdu.pduSource), obj_id)
	    else:
		print "unexpected update method for sensor"

    def _update_cb_devices(self):
	print "updating cb devices"
	timer = threading.Timer(60, self._update_cb_devices)
	timer.daemon = True
	cb_devices = self.cb_devices.getAllDevices()
	updated_sensors = {}
	for device in cb_devices:
	    if device["type"] != "adapter" and device["enabled"] == True:
	    	key = (device["parent_device_ip"], device["bacnet_identifier"])
	    	updated_sensors[key] = device
	self.sensors = updated_sensors
	print("{} devices pulled from cb".format(len(self.sensors)))
	timer.start()

    def _start_polling(self):
	timer = threading.Timer(120, self._start_polling)
	timer.daemon = True
	for key in self.sensors:
	    update_method = self.sensors[key]["update_method"]
	    if update_method is not None and update_method == "polling":
		self.pending_poll_requests.append((key[0].encode("utf-8"), (self.sensors[key]["bacnet_object_type"].encode("utf-8"), self.sensors[key]["bacnet_identifier"])))
		#self._get_present_value_prop(key[0].encode("utf-8"), (self.sensors[key]["bacnet_object_type"].encode("utf-8"), self.sensors[key]["bacnet_identifier"]))
	timer.start()

    def _process_poll_requests(self):
	timer = threading.Timer(15, self._process_poll_requests)
	time.daemon = True
	num_to_process = len(self.pending_poll_requests)
	if num_to_process < 100:
	    num_to_process = 100
	count = 0
	while count < num_to_process:
	    if len(self.pending_poll_requests) > 0:
		poll_request = self.pending_poll_requests.pop(0)
	    	self._get_present_value_prop(poll_request[0], poll_request[1])
	    	count += 1
	timer.start()

    def _get_present_value_prop(self, parent_device_ip, sensor_obj_id):
	request = ReadPropertyRequest(
	    destination=Address(parent_device_ip),
	    objectIdentifier=sensor_obj_id,
	    propertyIdentifier="presentValue"
	)
	iocb = IOCB(request)
	iocb.add_callback(self._got_present_value_for_existing_sensor, sensor_obj_id)
	self.bacnet_adapter.request_io(iocb)

    def _got_present_value_for_existing_sensor(self, iocb, obj_id):
	now = datetime.datetime.utcnow().isoformat()
	if iocb.ioError:
	    print("error during read present value: {}".format(str(iocb.ioError)))
	else:
	    apdu = iocb.ioResponse
	    datatype = get_datatype(obj_id[0], "presentValue")
	    value = apdu.propertyValue.cast_out(datatype)
	    msg_to_send = {
		"time_stamp": now,
		"name": self.sensors[(str(apdu.pduSource), obj_id[1])]["name"],
		"present_value": value
	    }
	    self.bacnet_adapter.send_value_to_platform(msg_to_send)
	
    def _resub_existing_covs(self):
	print "resubing covs"
	timer = threading.Timer(180, self._resub_existing_covs)
	timer.daemon = True
	for key in self.sensors:
	    update_method = self.sensors[key]["update_method"]
	    if update_method is not None and update_method == "cov":
		self._cov_subscribe(key[0].encode("utf-8"), (self.sensors[key]["bacnet_object_type"].encode("utf-8"), self.sensors[key]["bacnet_identifier"]))
	timer.start()

    def _cov_subscribe(self, parent_device_ip, sensor_obj_id):
	request = SubscribeCOVRequest(
	    subscriberProcessIdentifier=2367,
	    monitoredObjectIdentifier=sensor_obj_id,
	    destination=Address(parent_device_ip)
	)
	request.pduDestination = Address(parent_device_ip)
	request.lifetime = 240
	request.issueConfirmedNotifications = False
	iocb = IOCB(request)
	iocb.add_callback(self._cov_sub_complete)
	self.bacnet_adapter.request_io(iocb)

    def _cov_sub_complete(self, iocb):
	if iocb.ioError:
	    print "iocb io error"
	    print("error during cob subscribe: {0}".format(str(iocb.ioError)))
	else:
	    print("cov sub was successful")

