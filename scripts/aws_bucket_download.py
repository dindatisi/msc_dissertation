import boto3
import botocore
import os 

BUCKET_NAME = 'warner-ucl'

def get_all_keys(bucket):
    keys = []
    for object in bucket.objects.all():
        keys.append(str(object.key))
    print('first 5 keys:',keys[:5])
    return keys
    
def download_to_dir(key):
    try:
        print('donwloading',key)
        filename = 'raw_data/'+key
        s3.Bucket(BUCKET_NAME).download_file(key, filename)
        print('success\n')
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            raise
    
if __name__ == "__main__":
    # establish connection to bucket
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(BUCKET_NAME)
    #collect keys
    print('collecting keys')
    keys = get_all_keys(bucket)
    #download files
    for key in keys:
        download_to_dir(key)
    
    