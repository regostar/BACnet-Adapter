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
	self._get_devices_from_cb_collection()

    def _get_devices_from_cb_collection(self):
	print "checking for new bacnet controllers"
	timer = threading.Timer(120, self._get_devices_from_cb_collection)
	timer.daemon = True
	collection = self.cb_collection.fetch()
	devices = collection['DATA']
	for device in devices:
	    if device["device_name"] == None or device["device_name"] == "".encode("utf-8"):
		print "found new device {}".format(device["item_id"])
		self._initialize_new_deivce(device["item_id"], device["ip_address"].encode("utf-8"))
	    self.devices[device["ip_address"]] = device
	timer.start()

    def _initialize_new_deivce(self, device_item_id, device_ip):
        self.bacnet_adapter.who_is(None, None, Address(device_ip))

    def got_new_device_who_is_response(self, apdu):
        # first check that this new device is one we actually care about (some times we get whois responses from other devices, even though the request was targeted to a specific ip rather than a global address)
	if not str(apdu.pduSource) in self.devices:
	    print "got who is response from a device we don't care about"
	    return
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
	    }
	    results = self.cb_collection.update(changes, query)   
	    if results["count".encode('utf-8')] != 1:
		print "failed to update the deivce"
	    else:
		#rather then fetch from the server again, just update these here
		self.devices[str(apdu.pduSource)]["device_name"] = device_name.decode("utf-8")
		self.devices[str(apdu.pduSource)]["bacnet_device_identifier"] = device_id[1]
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
	    new_sensors = apdu.propertyValue.cast_out(ArrayOf(ObjectIdentifier))
	    self.bacnet_adapter.bacnet_sensors.add_new_sensors_from_device(new_sensors, self.devices[str(apdu.pduSource)])
	    #self.bacnet_adapter.bacnet_sensors.add_new_sensors_from_device([('analogInput', 3000126), ('analogInput', 3000145), ('analogValue', 3001213), ('analogValue', 3001215), ('analogValue', 3001195), ('analogValue', 3001203), ('analogValue', 3001204)], self.devices[str(apdu.pduSource)])

