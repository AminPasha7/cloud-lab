import os
import boto3
from botocore.exceptions import ClientError
from azure.storage.blob import BlobServiceClient
from google.cloud import pubsub_v1

AWS_ENDPOINT = os.getenv("AWS_ENDPOINT", "http://localstack:4566")
AWS_REGION   = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET    = os.getenv("S3_BUCKET", "demo-bucket")

AZ_CONN      = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
AZ_CONTAINER = os.getenv("AZ_CONTAINER", "demo")

GCP_PROJECT  = os.getenv("GOOGLE_CLOUD_PROJECT", "demo")
# PUBSUB_EMULATOR_HOST is picked up from env (e.g., gcp-pubsub:8085)

def aws_s3_demo():
    s3 = boto3.client("s3",
        endpoint_url=AWS_ENDPOINT,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID","test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY","test"),
        region_name=AWS_REGION)
    try:
        s3.create_bucket(Bucket=S3_BUCKET)
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("BucketAlreadyOwnedByYou","BucketAlreadyExists"):
            raise
    s3.put_object(Bucket=S3_BUCKET, Key="from-app.txt", Body=b"hi from app -> S3")
    print("[AWS] OK")

def azure_blob_demo():
    svc = BlobServiceClient.from_connection_string(AZ_CONN)
    try:
        svc.create_container(AZ_CONTAINER)
    except Exception:
        pass
    blob = svc.get_blob_client(container=AZ_CONTAINER, blob="from-app.txt")
    blob.upload_blob(b"hi from app -> Azurite", overwrite=True)
    print("[AZURE] OK")

def gcp_pubsub_demo():
    publisher  = pubsub_v1.PublisherClient()
    subscriber = pubsub_v1.SubscriberClient()
    topic_path = publisher.topic_path(GCP_PROJECT, "demo-topic")
    sub_path   = subscriber.subscription_path(GCP_PROJECT, "demo-sub")
    try: publisher.create_topic(request={"name": topic_path})
    except Exception: pass
    publisher.publish(topic_path, b"hi from app -> PubSub").result()
    try: subscriber.create_subscription(request={"name": sub_path, "topic": topic_path})
    except Exception: pass
    resp = subscriber.pull(request={"subscription": sub_path, "max_messages": 1})
    if resp.received_messages:
        m = resp.received_messages[0]
        print("[GCP] received:", m.message.data.decode())
        subscriber.acknowledge(request={"subscription": sub_path, "ack_ids":[m.ack_id]})
    else:
        print("[GCP] no messages")

if __name__ == "__main__":
    aws_s3_demo()
    azure_blob_demo()
    gcp_pubsub_demo()
    print("All three emulators OK.")
