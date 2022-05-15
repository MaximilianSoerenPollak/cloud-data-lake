#!/bin/bash
echo "Enter your s3 bucket name, the same as in the dl.cfg file"
read bucket_name

#Creating S3 services
echo "Starting up your S3 bucket"
python aws_startup.py

#Copying the Data to local files
echo "Copying the Data to local, and mangeling it."
python etl.py

# Uploading the data into s3.
echo "Syncing all files to s3"
aws s3 sync ./temp/ s3://$bucket_name/


echo "Everything is done, if you want to delete the bucket run 'python aws_shutdown.py'"
