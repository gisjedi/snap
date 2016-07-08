import datetime
import json
import os
import uuid


class S3Notification(object):
    def __init__(self):
        self.proto = {}
        with open(os.path.join(os.path.dirname(__file__), 'v2-message.json')) as proto_file:
            self.proto = json.load(proto_file)

    def generate(self, bucket_obj, s3_obj):
        record = dict(self.proto['Records'][0])
        records = []
        for s3_obj in s3_obj:
            record['awsRegion'] = bucket_obj.get_bucket_location()
            record['awsTime'] = '%sZ' % datetime.datetime.utcnow().isoformat()
            record['s3']['bucket'] = bucket_obj.get_repr()
            record['s3']['object']['key'] = s3_obj.key
            record['s3']['object']['size'] = s3_obj.size
            record['s3']['object']['eTag'] = s3_obj.e_tag

            records.append(record)

        with open(os.path.join(os.path.dirname(__file__), 'v2-notification.json')) as notification:
            payload = json.load(notification)
            payload['Timestamp'] = '%sZ' % datetime.datetime.utcnow().isoformat()
            payload['MessageId'] = str(uuid.uuid4())
            payload['Message'] = json.dumps({"Records": records})

        return json.dumps(payload)

    def publish(self, payload):
        raise NotImplementedError
