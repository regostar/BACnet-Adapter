from bacpypes.apdu import ReadPropertyRequest, ReadPropertyACK
from bacpypes.constructeddata import ArrayOf
from bacpypes.iocb import IOCB
from bacpypes.primitivedata import ObjectIdentifier

from ObjectList import ObjectList


class Device:
    def __init__(self, device_identifier, device_source, bacnet_adapter):
        self.id = device_identifier
        self.source = device_source
        self.bacnet_adapter = bacnet_adapter
        self.object_list = None

    def get_object_list(self):
        request = ReadPropertyRequest(
            destination=self.source,
            objectIdentifier=self.id,
            propertyIdentifier="objectList"
        )

        iocb = IOCB(request)
        iocb.add_callback(self._got_object_list)
        self.bacnet_adapter.request_io(iocb)

    def _got_object_list(self, iocb):
        if iocb.ioError:
            print("error (%s) when attempting to get object-list of device (%s)", str(iocb.ioError), self.id)
        elif iocb.ioResponse:
            apdu = iocb.ioResponse
            if not isinstance(apdu, ReadPropertyACK):
                print("response was not ReadPropertyACK as expected")
                return
            obj_list = apdu.propertyValue.cast_out(ArrayOf(ObjectIdentifier))
            self.object_list = ObjectList(obj_list, self, self.bacnet_adapter)
            self.object_list.get_properties_for_each_object()



