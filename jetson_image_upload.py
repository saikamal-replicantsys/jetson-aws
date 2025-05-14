import os
import time
import json
import boto3
import logging
from datetime import datetime
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

# CONFIGURATION

# Jetson device identity
DEVICE_ID = "jetson-tax"
IMAGE_PATH = "image.jpg"  # Path to our image file
AWS_REGION = "ap-south-1"

# AWS IoT Core MQTT
IOT_ENDPOINT = "OUR_ENDPOINT.iot.OUR_REGION.amazonaws.com"
MQTT_TOPIC = f"device/{DEVICE_ID}/image"

# AWS IoT TLS credentials (paths on Jetson)
ROOT_CA_PATH = "AmazonRootCA1.pem"
PRIVATE_KEY_PATH = "private.pem.key"
CERTIFICATE_PATH = "certificate.pem.crt"

# AWS S3
S3_BUCKET = "our-s3-bucket-name"
S3_PREFIX = f"devices/{DEVICE_ID}/images"

# LOGGING
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("JetsonUploader")

# UPLOAD TO S3
def upload_to_s3(local_path: str) -> str:
    s3 = boto3.client("s3", region_name=AWS_REGION)
    filename = f"{DEVICE_ID}_{int(time.time())}.jpg"
    s3_key = f"{S3_PREFIX}/{filename}"

    s3.upload_file(local_path, S3_BUCKET, s3_key)
    s3_url = f"s3://{S3_BUCKET}/{s3_key}"
    log.info(f"Image uploaded to S3: {s3_url}")
    return s3_url, filename

# PUBLISH METADATA TO MQTT
def publish_metadata_to_mqtt(s3_url: str, size_kb: float, resolution: str):
    mqtt_client = AWSIoTMQTTClient(DEVICE_ID)
    mqtt_client.configureEndpoint(IOT_ENDPOINT, 8883)
    mqtt_client.configureCredentials(ROOT_CA_PATH, PRIVATE_KEY_PATH, CERTIFICATE_PATH)

    mqtt_client.configureOfflinePublishQueueing(-1)
    mqtt_client.configureDrainingFrequency(2)
    mqtt_client.configureConnectDisconnectTimeout(10)
    mqtt_client.configureMQTTOperationTimeout(5)

    mqtt_client.connect()
    log.info("Connected to AWS IoT MQTT")

    payload = {
        "deviceId": DEVICE_ID,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "type": "image",
        "payload": {
            "s3Url": s3_url,
            "sizeKB": size_kb,
            "resolution": resolution
        }
    }

    mqtt_client.publish(MQTT_TOPIC, json.dumps(payload), 1)
    log.info("Published image metadata to MQTT")

# MAIN EXECUTION
def main():
    if not os.path.exists(IMAGE_PATH):
        log.error(f"Image file not found: {IMAGE_PATH}")
        return

    log.info("Starting upload process...")

    file_size_kb = round(os.path.getsize(IMAGE_PATH) / 1024, 2)
    resolution = "640x480"  # We can extract this using OpenCV if needed

    s3_url, filename = upload_to_s3(IMAGE_PATH)
    publish_metadata_to_mqtt(s3_url, file_size_kb, resolution)

    log.info(f"Upload + publish completed for: {filename}")

if __name__ == "__main__":
    main()
