import boto3
import skimage.io
from skimage.color import rgb2gray
import os
from time import time


def log_image(client, domain, filename, processed, timestamp):
    response = client.put_attributes(
        DomainName=domain,
        ItemName='image',
        Attributes=[
            {
                'Name': 'image_name',
                'Value': filename,
                'Replace': True
            },
            {
                'Name': 'processed',
                'Value': processed,
                'Replace': True
            },
            {
                'Name': 'timestamp',
                'Value': str(timestamp),
                'Replace': True
            },
        ],
    )
    return response


AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

SIMPLE_DB_DOMAIN_NAME = os.environ['SIMPLE_DB_DOMAIN_NAME']

QUEUE_NAME = os.environ['QUEUE_NAME']
BUCKET_NAME = os.environ['BUCKET_NAME']

s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
sqs = boto3.resource('sqs', region_name='us-west-2', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
queue = sqs.get_queue_by_name(QueueName=QUEUE_NAME)

sdb = boto3.client('sdb', region_name='us-west-2', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

# sdb.create_domain(DomainName=SIMPLE_DB_DOMAIN_NAME)
# sdb.delete_domain(DomainName=SIMPLE_DB_DOMAIN_NAME)
# log_image(sdb, SIMPLE_DB_DOMAIN_NAME, "test", 'False', time())


def get_images(client):
    response = client.select(
        SelectExpression='SELECT * FROM {}'.format(SIMPLE_DB_DOMAIN_NAME),
        ConsistentRead=True
    )
    print(response)


while True:
    messages = queue.receive_messages(MaxNumberOfMessages=10, VisibilityTimeout=30)
    for m in messages:
        filename = m.body
        new_filename = 'bw_' + filename.split('/')[-1]
        s3.Bucket(BUCKET_NAME).download_file(filename, filename.split('/')[-1])

        img = skimage.io.imread(filename.split('/')[-1])
        new_img = rgb2gray(img)
        skimage.io.imsave(new_filename, new_img)

        s3.Bucket(BUCKET_NAME).upload_file(new_filename, 'uploads/' + new_filename)
        log_image(sdb, SIMPLE_DB_DOMAIN_NAME, filename, 'True', time())
        m.delete()

        os.remove(filename.split('/')[-1])
        os.remove(new_filename)
