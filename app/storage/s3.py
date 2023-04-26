import boto3
import botocore
import os


class S3:
    bucket_name = "g15a2"
    aws_access_key_id = ""
    aws_secret_access_key = ""

    def __init__(self):
        self.s3 = boto3.client('s3',
                  aws_access_key_id=self.aws_access_key_id,
                  aws_secret_access_key=self.aws_secret_access_key)

    def upload(self, name, file):
        self.s3.upload_fileobj(file, self.bucket_name, name)

    def download(self, filename, downloadname):
        try:
            self.s3.download_file(self.bucket_name, filename, downloadname)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                print("File Not Found")
            else:
                print("Error Occurred")

    def list_all(self):
        res = self.s3.list_objects_v2(Bucket=self.bucket_name)
        return res.get('Contents', [])

    def delete_all(self):
        all = self.list_all()
        for name in all:
            self.s3.delete_object(Bucket=self.bucket_name, Key=name["Key"])
