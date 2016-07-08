import json

import boto3


class S3Bucket(object):
    def __init__(self, bucket):
        # Get the service resource
        s3 = boto3.resource('s3')

        self.bucket = s3.Bucket(bucket)

    def list(self, limit=None, prefix=None):
        """Iterate over keys within a bucket and find all keys, optionally providing limit and prefix

        :param limit: Maximum number of results to return, set to None for unbounded
        :type limit: boolean
        :param prefix: Specify a prefix string to filter keys, set to None for unfiltered
        :type prefix: boolean
        :return: an iterable of S3 objects constrained by the given filters
        :rtype: ObjectSummaries
        """

        objects = self.bucket.objects.all()
        if limit is not None:
            objects = objects.limit(limit)
            print 'limit of %s applied' % limit
        if prefix is not None:
            objects = objects.filter(Prefix=prefix)
            print 'prefix of %s applied' % prefix

        return objects

    def get_repr(self):
        representation = {
            "name": self.bucket.name,
            "ownerIdentity": {
                "principalId": None
            },
            "arn": "arn:aws:s3:::%s" % self.bucket.name
        }

        return representation

    def get_bucket_location(self):
        return self.bucket.meta.client.meta.region_name

