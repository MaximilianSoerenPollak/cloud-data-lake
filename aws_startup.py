# This file starts all AWS services that we need.
# THIS HAS TO RUN FIRST
""" 
This scrip is going to make a new IAM_role (if not so done yet) will then 
spin up a redshift cluster and also detect and see if it is avialable and up. 

So once this Script runs we can interact with the Redshift cluster and can be sure we 
have a connection to it. 
So this scrips ALWAYS should run first.

"""

# ----- CODE ----
import configparser
import boto3

# ---- Read Config ----
config = configparser.ConfigParser()
config.read_file(open("dl.cfg"))

AWS_ACCESS_KEY = config.get("KEYS", "AWS_ACCESS_KEY")
AWS_SECRET_KEY = config.get("KEYS", "AWS_SECRET_KEY")

# ---- Declaring AWS SDK instances ----

s3 = boto3.client(
    "s3",
    region_name="us-west-2",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
)

# ---- Functions ----

# Create an S3 bucket where our parquet files are stored.
def create_s3_bucket():
    bucket_dict = s3.create_bucket(
        Bucket=(config.get("AWS", "S3_BUCKET_NAME")),
        CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
    )


# Call everytihng in the right order once we execute the file.
if __name__ == "__main__":
    create_s3_bucket()
