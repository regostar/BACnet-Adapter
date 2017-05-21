import threading

from clearblade import core, Devices

from bacpypes.apdu import WhoIsRequest, IAmRequest, ReadPropertyRequest
from bacpypes.basetypes import CharacterString
from bacpypes.constructeddata import ArrayOf
from bacpypes.iocb import IOCB
from bacpypes.pdu import Address
from bacpypes.primitivedata import ObjectIdentifier

class BACnetDevices():

    BACNET_DEVICES_COLLECTION_ID = "e6bcf9910b84b7aad4d7fee7d8c201"

    def __init__(self, cb_client, bacnet_adapter):
        self.cb_client = cb_client
        self.bacnet_adapter = bacnet_adapter
        self.cb_collection = core.Collection(self.cb_client, self.BACNET_DEVICES_COLLECTION_ID)
	self.devices = {}
	self.devices_lock = threading.Lock()
	self.first_run = True
	self._get_devices_from_cb_collection()
	self._check_for_unregistered_sensors()

    def _get_devices_from_cb_collection(self):
	print "checking for new bacnet controllers"
	timer = threading.Timer(120, self._get_devices_from_cb_collection)
	timer.daemon = True
	collection = self.cb_collection.fetch()
	devices = collection['DATA']
	self.devices_lock.acquire()
	for device in devices:
	    if device["device_name"] == None or device["device_name"] == "".encode("utf-8"):
		print "found new device {}".format(device["item_id"])
		self._initialize_new_deivce(device["item_id"], device["ip_address"].encode("utf-8"))
	    self.devices[device["ip_address"]] = device
	self.devices_lock.release()
	timer.start()

    def _initialize_new_deivce(self, device_item_id, device_ip):
        self.bacnet_adapter.who_is(None, None, Address(device_ip))

    def got_new_device_who_is_response(self, apdu):
        # first check that this new device is one we actually care about (some times we get whois responses from other devices, even though the request was targeted to a specific ip rather than a global address)
	self.devices_lock.acquire()
	if not str(apdu.pduSource) in self.devices:
	    print "got who is response from a device we don't care about"
	    self.devices_lock.release()
	    return
	self.devices_lock.release()
	# now we need to get the device name using the identifier we just got
        request = ReadPropertyRequest(
            destination=apdu.pduSource,
            objectIdentifier=apdu.iAmDeviceIdentifier,
            propertyIdentifier='objectName'
        )
        iocb = IOCB(request)
	iocb.add_callback(self._got_device_object_name, apdu.iAmDeviceIdentifier)
	self.bacnet_adapter.request_io(iocb)

    def _got_device_object_name(self, iocb, device_id):
        if iocb.ioError:
	    print("error (%s) when attempting to get objectName of device (%s %s)" % (str(iocb.ioError), iocb.pduSource))
        else:
	    apdu = iocb.ioResponse
            device_name = apdu.propertyValue.cast_out(CharacterString)
	    #we now have everything we need to update the device data in the collection
	    query = {
		"FILTERS": [
		    [{
			"EQ": [{
			    "ip_address": str(apdu.pduSource)
			}]
		    }]
		]
	    }
	    changes = {
		"device_name": device_name,
		"bacnet_device_identifier":  device_id[1],
		"all_sensors_registered": False
	    }
	    results = self.cb_collection.update(changes, query)   
	    if results["count".encode('utf-8')] != 1:
		print "failed to update the deivce"
	    else:
		#rather then fetch from the server again, just update these here
		self.devices_lock.acquire()
		self.devices[str(apdu.pduSource)]["device_name"] = device_name.decode("utf-8")
		self.devices[str(apdu.pduSource)]["bacnet_device_identifier"] = device_id[1]
		self.devices_lock.release()
		self._get_new_sensors_for_new_device(apdu.pduSource, device_id)

    def _get_new_sensors_for_new_device(self, source, device_id):
	request = ReadPropertyRequest(
	    destination=source,
	    objectIdentifier=device_id,
	    propertyIdentifier="objectList"
	)
	iocb = IOCB(request)
	iocb.add_callback(self._got_sensors_for_device)
	self.bacnet_adapter.request_io(iocb)
	    
    def _got_sensors_for_device(self, iocb):
	if iocb.ioError:
	    print("error (%s) when attempting to get objectList of device (%s)" % (str(iocb.ioError), iocb.pduSource))
	else:
	    apdu = iocb.ioResponse
	    all_sensors = apdu.propertyValue.cast_out(ArrayOf(ObjectIdentifier))
	    #now check that these sensors aren't already registered as a cb device
	    new_sensors = []
	    for sensor in all_sensors:
		key = (str(apdu.pduSource), sensor[1])
	    	if not key in self.bacnet_adapter.bacnet_sensors.sensors and sensor[0] != "trendLog" and sensor[0] != "device" and sensor[0] != "program":
		    new_sensors.append(sensor)
	    self.devices_lock.acquire()
	    print "device {} has {}/{} sensors not registered, now trying to register them".format(self.devices[str(apdu.pduSource)]["device_name"], len(new_sensors), len(all_sensors))
	    if len(new_sensors) == 0:
	        #update the controller in the db to say all sensors are registered finally
		query = {"FILTERS":[[{"EQ":[{"ip_address": str(apdu.pduSource)}]}]]}
		changes = {
		    "all_sensors_registered": True
		}
		results = self.cb_collection.update(changes, query)
		if results["count".encode("utf-8")] != 1:
		    print "failed to update the collection"
		else:
		    self.devices_lock.release()
		    return
	    self.bacnet_adapter.bacnet_sensors.add_new_sensors_from_device(new_sensors, self.devices[str(apdu.pduSource)])
	    self.devices_lock.release()

    def _check_for_unregistered_sensors(self):
	#when we are just starting give some time for devices to be initialized
	timer = threading.Timer(600, self._check_for_unregistered_sensors)
        timer.daemon = True
	if self.first_run:
	    self.first_run = False
	    first_run_timer = threading.Timer(60, self._check_for_unregistered_sensors)
	    first_run_timer.daemon = True
	    first_run_timer.start()
	    return
	collection = self.cb_collection.fetch()
        devices = collection['DATA']
        for device in devices:
            if device["device_name"] != None and device["device_name"] != "".encode("utf-8") and not device["all_sensors_registered"]:
                print "bacnet controller {} does not have all devices registered yet".format(device["device_name"])
                #self._initialize_new_deivce(device["item_id"], device["ip_address"].encode("utf-8"))
		self._get_new_sensors_for_new_device(Address(device["ip_address"].encode("utf-8")), ('device', device["bacnet_device_identifier"]))
	timer.start()
