from shared import db
from sqlalchemy import Integer
from datetime import datetime
from config import Config
from models import Subscription, Message
import paho.mqtt.client as mqtt_api


cfg = Config.get_global_instance()


class MQTT(db.Model):
    id = db.Column(Integer, primary_key=True)
    uuid = db.Column(db.VARCHAR(40), nullable=False)
    timestamp_created = db.Column(db.TIMESTAMP, default=datetime.utcnow)

    def __init__(self, device):
        self.uuid = device

    def __repr__(self):
        return '<MQTT {}>'.format(self.uuid)

    def as_dict(self):
        data = {
            "uuid": self.service.as_dict(),
            "timestamp": int((self.timestamp_created - datetime.utcfromtimestamp(0)).total_seconds()),
        }
        return data

    @staticmethod
    def send_message(message):
        """

        :type message: Message to send to mqtt subscribers
        """
        subscriptions = Subscription.query.filter_by(service=message.service).all()
        if len(subscriptions) == 0:
            return 0

        # gets all devices uuids in the subscription object
        uuids = [sub.device for sub in subscriptions]

        if len(subscriptions) > 0:
            data = dict(message=message.as_dict(), encrypted=False)
            MQTT.mqtt_send(uuids, data)
            last_message = Message.query.order_by(Message.id.desc()).first()
            for l in subscriptions:
                l.timestamp_checked = datetime.utcnow()
                l.last_read = last_message.id if last_message else 0
            db.session.commit()
        return len(subscriptions)

    @staticmethod
    def mqtt_send(uuids, data):
        url = cfg.mqtt_broker_address
        if ":" in url:
            port = url.split(":")[1]
            url = url .split(":")[0]
        else:
            # default port
            port = 1883

        client = mqtt_api.Client()
        client.connect(url, port, 60)

        for uuid in uuids:
            client.publish(uuid, str(data))
        client.disconnect()
