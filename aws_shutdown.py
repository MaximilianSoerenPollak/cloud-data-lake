import configparser
from aws_startup import s3
from time import sleep

# ----- CODE ----

config = configparser.ConfigParser()
config.read_file(open("dl.cfg"))

# Delete the S3 bucket we created
def delete_s3():
    s3.delete_bucket(Bucket=config.get("AWS", "S3_BUCKET_NAME"))
    sleep(5)
    delete_s3()


# Run all the functions if the script is called.
if __name__ == "__main__":
    delete_s3()
    print("All resources deleted.")
