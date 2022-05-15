import configparser
from time import sleep
import boto3

# ----- CODE ----

config = configparser.ConfigParser()
config.read_file(open("dl.cfg"))

# Delete the S3 bucket we created
def delete_s3():
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(config.get("AWS", "S3_BUCKET_NAME"))
    bucket.objects.all().delete()
    # s3.delete_bucket(Bucket=config.get("AWS", "S3_BUCKET_NAME"))


# Run all the functions if the script is called.
if __name__ == "__main__":
    delete_s3()
    print("All resources deleted.")
