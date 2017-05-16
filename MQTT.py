import time

from clearblade import Messaging


class MQTT:
    def __init__(self, cb_device_client):
	self.cb_device_client = cb_device_client
        #Connect to MQTT
        self.messaging_client = self.Connect()

    def Connect(self):
        messaging_client = Messaging.Messaging(self.cb_device_client)
        messaging_client.InitializeMQTT()
        # this is a temporary workaround for MONSOON-2501, once resolved this can be removed
	time.sleep(5)
	return messaging_client

    def PublishTopic(self, topic, message):
        self.messaging_client.publishMessage(topic, message, 0)

    def SubscribeToTopic(self, topic, callback):
        self.messaging_client.subscribe(topic, 0, callback)
