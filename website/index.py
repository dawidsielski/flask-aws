import configparser
import os

from time import time
from flask import Flask, request, render_template, url_for
import boto3


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


app = Flask(__name__)


AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

SIMPLE_DB_DOMAIN_NAME = os.environ['SIMPLE_DB_DOMAIN_NAME']

QUEUE_NAME = os.environ['QUEUE_NAME']
BUCKET_NAME = os.environ['BUCKET_NAME']
BUCKET_URL = 'https://s3-us-west-2.amazonaws.com/{}'.format(BUCKET_NAME)

s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
sqs = boto3.resource('sqs', region_name='us-west-2', aws_access_key_id=AWS_ACCESS_KEY_ID,
                     aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
queue = sqs.get_queue_by_name(QueueName=QUEUE_NAME)

sdb = boto3.client('sdb', region_name='us-west-2', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)


@app.route("/")
def index():
    return render_template('index.html')


@app.route('/upload', methods=['POST'])
def upload():
    file = request.files['myfile']
    s3.Bucket(BUCKET_NAME).put_object(Key=file.filename, Body=file)

    log_image(sdb, SIMPLE_DB_DOMAIN_NAME, file.filename, 'False', time())
    return '<h1>File saved to S3</h1>'


def get_url(filename):
    return '{}/{}'.format(BUCKET_URL, filename)


@app.route('/images', methods=['GET', 'POST'])
def images():
    bucket_contents = s3.Bucket(BUCKET_NAME).objects.all()

    filenames = [file.key for file in bucket_contents if not file.key.startswith('bw_')]
    bw_filenames = [file.key for file in bucket_contents if file.key.startswith('bw_')]

    urls = ['{}/{}'.format(BUCKET_URL, filename) for filename in filenames]

    images_data = []
    for filename in filenames:
        processed = "bw_" + filename if "bw_" + filename in bw_filenames else 'Not processed.'
        processed_url = get_url(processed) if processed != 'No image.' else ''
        images_data.append([[filename, get_url(filename)], [processed, processed_url]])

    print(images_data)
    return render_template('images.html', filenames=list(zip(filenames, urls)), images_data=images_data)


def get_selected_images(request):
    items = request.form.getlist('imagesSelection')
    for filename in items:
        response = queue.send_message(MessageBody=filename)
        print(response.get('MD5OfMessageBody'))
    return items


@app.route('/selected', methods=['GET', 'POST'])
def selected_images():
    items = get_selected_images(request)
    return str(items)


if __name__ == "__main__":
    app.run()
