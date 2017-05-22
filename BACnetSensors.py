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
	self.sensor_lock = threading.Lock()
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
	print "found {} new sensors to add".format(len(sensors))
	count = 0
	self.sensor_lock.acquire()
	for sensor in sensors:
	    if sensor[0] != "trendLog" and sensor[0] != "device" and sensor[0] != "program":
		count += 1
		key = (device_info["ip_address"], sensor[1])
		self.sensors[key] = {
		    "bacnet_object_type": sensor[0],
		    "bacnet_identifier": sensor[1],
		    "present_value": None,
		    "update_method": None
		}
		self.pending_new_sensors.append((device_info["ip_address"].encode("utf-8"), sensor))
	self.sensor_lock.release()
	print "actually adding {} of those sensors".format(count)

    def _process_new_sensors(self):
	timer_length = 0.4
	num_to_process = len(self.pending_new_sensors)
	if num_to_process == 0:
	    timer_length = 10
	timer = threading.Timer(timer_length, self._process_new_sensors)
	timer.daemon = True
	num_to_process = len(self.pending_new_sensors)
	if num_to_process > 1:
	     num_to_process = 1
	count = 0
	print("processing {}/{} new sensors".format(num_to_process, len(self.pending_new_sensors)))
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
	print "got props for a new sensor {}".format(obj_id)
	timestamp = datetime.datetime.utcnow().isoformat()
	if iocb.ioError:
	    print("error getting object ({0}) details: {1}".format(obj_id, str(iocb.ioError)))
	    return
	else:
	    apdu = iocb.ioResponse
	    if not isinstance(apdu, ReadPropertyMultipleACK):
		print("response was not ReadPropertyMultipleACK")
		return	
	    props_obj = decode_multiple_properties(apdu.listOfReadAccessResults)
	    print "new sensor props are {}".format(props_obj)
	    print "new sensor obj id is {}".format(obj_id)
	    print "timestamp is {}".format(timestamp)
	    print "parent device ip is {}".format(str(apdu.pduSource))
	    # might be a better place to do this translation, but doing it here for now
	    update_method = "cov" if props_obj["objectName"].find("ZN-T") != -1 else "polling"
	    print "update method is {}".format(update_method)
	    sensor_obj = {
		"description": props_obj["description"],
		"name": props_obj["objectName"],
		"present_value": props_obj["presentValue"],
		"time_stamp": timestamp,
		"bacnet_object_type": obj_id[0],
		"bacnet_identifier": obj_id[1],
		"update_method": update_method,
	        "new": True,
	        "parent_device_ip": str(apdu.pduSource)
	    }
	    if "units" in props_obj:
		sensor_obj["units"] = props_obj["units"]
	    print "about to set sensor obj for {} {}".format(sensor_obj["name"], str(apdu.pduSource))
	    self.sensor_lock.acquire()
	    self.sensors[(str(apdu.pduSource), obj_id[1])] = sensor_obj
	    self.sensor_lock.release()
	    print "sending sensor obj to platfor for {}".format(sensor_obj["name"])
	    self.bacnet_adapter.send_value_to_platform(sensor_obj)
	    print "just sent new sensor obj to platform for sensor {}".format(sensor_obj["name"])
	    #we don't need to do anything if method is polling, because it will just automatically get picked up next cycle
	    if sensor_obj["update_method"] == "polling":
		print "will be picked up on next polling run"
	    elif sensor_obj["update_method"] == "cov":
		self._cov_subscribe(str(apdu.pduSource), obj_id)
	    else:
		print "unexpected update method for sensor"

    def _update_cb_devices(self):
	print "updating cb devices"
	timer = threading.Timer(30, self._update_cb_devices)
	timer.daemon = True
	cb_devices = self.cb_devices.getAllDevices()
	updated_sensors = {}
	for device in cb_devices:
	    if device["type"] != "adapter" and device["enabled"] == True:
		try:
	    	    key = (device["parent_device_ip"], device["bacnet_identifier"])
		except KeyError, e:
		    print "device is missing parent_device_ip or bacnet_identifier {}".format(device)	    	
		    time.sleep(5)
		    continue
		updated_sensors[key] = device
	self.sensor_lock.acquire()
	self.sensors = updated_sensors
	self.sensor_lock.release()
	print("{} devices pulled from cb".format(len(self.sensors)))
	timer.start()

    def _start_polling(self):
	print "adding more poll requests to queue"
	timer = threading.Timer(300, self._start_polling)
	timer.daemon = True
	self.sensor_lock.acquire()
	for key in self.sensors:
	    update_method = self.sensors[key]["update_method"]
	    if update_method is not None and update_method == "polling":
		self.pending_poll_requests.append((key[0].encode("utf-8"), (self.sensors[key]["bacnet_object_type"].encode("utf-8"), self.sensors[key]["bacnet_identifier"])))
		#self._get_present_value_prop(key[0].encode("utf-8"), (self.sensors[key]["bacnet_object_type"].encode("utf-8"), self.sensors[key]["bacnet_identifier"]))
	self.sensor_lock.release()
	timer.start()

    def _process_poll_requests(self):
	timer_length = 0.1
	#if there are no more polling requests, let's dial back this timer to 10 seconds
	num_to_process = len(self.pending_poll_requests)
	if num_to_process == 0:
	    timer_length = 10
	timer = threading.Timer(timer_length, self._process_poll_requests)
	time.daemon = True
	if num_to_process > 1:
	    num_to_process = 1
	count = 0
	print "processing {}/{} polling requests".format(num_to_process, len(self.pending_poll_requests))
	while count < num_to_process:
	    if len(self.pending_poll_requests) > 0:
		poll_request = self.pending_poll_requests.pop(0)
	    	self._get_present_value_prop(poll_request[0], poll_request[1])
	    	count += 1
	timer.start()

    def _get_present_value_prop(self, parent_device_ip, sensor_obj_id):
	print "getting present value for obj {}".format(sensor_obj_id)
	request = ReadPropertyRequest(
	    destination=Address(parent_device_ip),
	    objectIdentifier=sensor_obj_id,
	    propertyIdentifier="presentValue"
	)
	iocb = IOCB(request)
	iocb.add_callback(self._got_present_value_for_existing_sensor, sensor_obj_id)
	self.bacnet_adapter.request_io(iocb)
	print "sent request for present value for obj {}".format(sensor_obj_id)

    def _got_present_value_for_existing_sensor(self, iocb, obj_id):
	print "got present vale for sensor {}".format(obj_id[1])
	now = datetime.datetime.utcnow().isoformat()
	if iocb.ioError:
	    print("error during read present value: {}".format(str(iocb.ioError)))
	else:
	    apdu = iocb.ioResponse
	    print "getting datatype for presentValue for {}".format(obj_id[1])
	    datatype = get_datatype(obj_id[0], "presentValue")
	    value = apdu.propertyValue.cast_out(datatype)
	    print "creating msg to send to platform for {}".format(obj_id[1])
	    self.sensor_lock.acquire()
	    msg_to_send = {
		"time_stamp": now,
		"name": self.sensors[(str(apdu.pduSource), obj_id[1])]["name"],
		"present_value": value
	    }
	    self.sensor_lock.release()
	    self.bacnet_adapter.send_value_to_platform(msg_to_send)
	    print "msg sent to platform for {}".format(obj_id[1])
	
    def _resub_existing_covs(self):
	print "resubing covs"
	timer = threading.Timer(600, self._resub_existing_covs)
	timer.daemon = True
	count = 0
	self.sensor_lock.acquire()
	for key in self.sensors:
	    update_method = self.sensors[key]["update_method"]
	    if update_method is not None and update_method == "cov":
		self._cov_subscribe(key[0].encode("utf-8"), (self.sensors[key]["bacnet_object_type"].encode("utf-8"), self.sensors[key]["bacnet_identifier"]))
		count += 1
	self.sensor_lock.release()
	print "resubed {} cov subscriptions".format(count)
	timer.start()

    def _cov_subscribe(self, parent_device_ip, sensor_obj_id):
	print "cov sub for {}".format(sensor_obj_id[1])
	request = SubscribeCOVRequest(
	    subscriberProcessIdentifier=2367,
	    monitoredObjectIdentifier=sensor_obj_id,
	    destination=Address(parent_device_ip)
	)
	request.pduDestination = Address(parent_device_ip)
	request.lifetime = 660
	request.issueConfirmedNotifications = False
	iocb = IOCB(request)
	iocb.add_callback(self._cov_sub_complete)
	self.bacnet_adapter.request_io(iocb)
	print "sent cov sub for {}".format(sensor_obj_id[1])

    def _cov_sub_complete(self, iocb):
	if iocb.ioError:
	    print "iocb io error"
	    print("error during cob subscribe: {0}".format(str(iocb.ioError)))
	else:
	    print("cov sub was successful")

