from s3 import bucket
from s3 import notification

import argparse

parser = argparse.ArgumentParser(description='Generate faked S3 ObjectCreated notifications based on'
                                             ' currently existing files.')
parser.add_argument('bucket', type=str,
                    help='Bucket name to generate notifications from')
parser.add_argument('resource', type=str, default='sns',
                    help='arn for target of notification output')
parser.add_argument('--dry-run', '-d', dest='dry-run', action='store_true', default=False,
                    help='print operations on stdout, instead of persisting remote')
parser.add_argument('--type', '-t', dest='type', type=str, default='sqs',
                    help='notification system target (sns/sqs)')
parser.add_argument('--limit', '-l', dest='limit', type=int, default=None,
                    help='maximum number of results. defaults to unbounded')
parser.add_argument('--prefix', '-p', dest='prefix', type=str, default=None,
                    help='key prefix to filter data with. defaults to no filter')

args = parser.parse_args()
print('Launching with args: %s' % args)

# Listing bucket contents
bucket = bucket.S3Bucket(args.bucket)
s3_objects = bucket.list(limit=args.limit, prefix=args.prefix)

# Generating notification for objects found.
notification = notification.S3Notification()
print notification.generate(bucket, s3_objects)

