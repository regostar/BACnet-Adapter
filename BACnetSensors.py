import datetime, time, threading, sys

from bacpypes.apdu import PropertyReference, ReadAccessSpecification, ReadPropertyMultipleRequest, ReadPropertyMultipleACK, SubscribeCOVRequest
from bacpypes.iocb import IOCB
from bacpypes.pdu import Address

from clearblade import Devices

from utils import decode_multiple_properties


class BACnetSensors:
    def __init__(self, cb_device_client, bacnet_adapter):
	self.sensors = {}
	self.cb_device_client = cb_device_client
	self.bacnet_adapter = bacnet_adapter
	self.cb_devices = Devices.Devices(self.cb_device_client)
	self.pending_cov_subscribes = []
	devices_from_cb = self.cb_devices.getAllDevices()
	print devices_from_cb
	#finally, kick off batch cov processing
	self._batch_cov_sub_process()
	# loop through these devices, and setup polling or COV

    def add_new_sensors_from_device(self, sensors, device_info):
	# first filter out any trendLogs, since we don't need them
	print device_info
	new_sensors = {}
	count = 0
	for sensor in sensors:
	    if sensor[0] != 'trendLog' and sensor[0] != 'device' and count <= 3000:
		key = (device_info["ip_address"], sensor[1])
		self.sensors[key] = {
		    "bacnet_object_identifier": sensor,
		    "default_sensor_data_fetch": "cov",
		    "present_value": None
		}
		props_to_get = ['objectName', 'description', 'presentValue', 'units']
		prop_ref_list = []
		for prop in props_to_get:
	    	    ref = PropertyReference(
			propertyIdentifier=prop
	    	    )
		    prop_ref_list.append(ref)
		read_access_spec = ReadAccessSpecification(
	    	    objectIdentifier=sensor,
	    	    listOfPropertyReferences=prop_ref_list
		)
		request = ReadPropertyMultipleRequest(
	    	    listOfReadAccessSpecs=[read_access_spec],
	    	    destination=Address(device_info["ip_address"].encode("utf-8"))
		)
		iocb = IOCB(request)
		iocb.add_callback(self._got_props_for_object, sensor)
		self.bacnet_adapter.request_io(iocb)
		count += 1

    def _got_props_for_object(self, iocb, obj_id):
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
	    sensor_obj = {
		"description": props_obj["description"],
		"object_name": props_obj["objectName"],
		"present_value": props_obj["presentValue"],
		"time_stamp": timestamp,
		"object_type": obj_id[0],
		"object_identifier": obj_id[1],
	    	"units": props_obj["units"]
	    }
	    self.sensors[(str(apdu.pduSource), obj_id[1])].update(sensor_obj)
	    self.bacnet_adapter.send_value_to_platform(sensor_obj)
	    self.pending_cov_subscribes.append((str(apdu.pduSource), obj_id))
	    #self._cov_subscribe(str(apdu.pduSource), obj_id)

    def _batch_cov_sub_process(self):
	print "in batch cov process"
	timer = threading.Timer(10, self._batch_cov_sub_process)
	timer.daemon = True
	#grab the top 200 pending covs and sign them up
	cov_to_process = len(self.pending_cov_subscribes)
	if cov_to_process > 50:
	    cov_to_process = 50
	count = 0
	print("cov to process is {}".format(cov_to_process))
	while count < cov_to_process:
	    cov = self.pending_cov_subscribes.pop(0)
	    print cov
	    self._cov_subscribe(cov[0], cov[1])
	    count += 1
	timer.start()

    def _cov_subscribe(self, parent_device_ip, sensor_obj_id):
	print "in cov subscribe"
	request = SubscribeCOVRequest(
	    subscriberProcessIdentifier=2367,
	    monitoredObjectIdentifier=sensor_obj_id,
	    destination=Address(parent_device_ip)
	)
	print "made request"
	request.pduDestination = Address(parent_device_ip)
	request.lifetime = 1200
	request.issueConfirmedNotifications = True
	print "set some stuff"
	iocb = IOCB(request)
	iocb.add_callback(self._cov_sub_complete)
    	print "send it off"
	try:
	    self.bacnet_adapter.process_io(iocb)
	except:
	    print "exception happened"
	    print sys.exc_info()[0]
	print sys.exc_info()[0]
	print "done"	

    def _cov_sub_complete(self, iocb):
	if iocb.ioError:
	    print "iocb io error"
	    print("error during cob subscribe: {0}".format(str(iocb.ioError)))
	else:
	    print("cov sub was successful")

