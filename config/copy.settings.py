# rename file to settings.py and enter required fields

S3_BUCKET_NAME = ""
S3_BUCKET_URI = f"s3://{S3_BUCKET_NAME}"

TRANSACTIONS_PATH = f"{S3_BUCKET_URI}/transactions/"
ORDERS_PATH = f"{S3_BUCKET_URI}/orders/"
FACEBOOK_PATH = f"{S3_BUCKET_URI}/facebook/"
PREFERENCES_PATH = f"{S3_BUCKET_URI}/preferences/"

RDS_HOST = ""
RDS_USER = ""
RDS_PASSWORD = ""
RDS_DATABASE = ""
RDS_PORT = "3306"

# Kinesis Stream Details
KINESIS_STREAM = ""

AWS_REGION = ""

# Other global settings
DATA_INGESTION_BATCH_SIZE = 500