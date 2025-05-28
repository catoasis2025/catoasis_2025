import boto3

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Input format: { "bucket": "my-bucket", "prefix": "configs/" }
    s3_config_path = 's3://pily-glue-job-config/data-allocations/'
    print("S3 config path:", s3_config_path)
    # bucket = event['bucket']
    # prefix = event['prefix']

    bucket = 'pily-glue-job-config'
    prefix = 'data-allocations/'
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        files = []

        if 'Contents' in response:
            for obj in response['Contents']:
                files.append({
                    'fileName': obj['Key'].split('/')[-1],
                    's3Path': obj['Key']
                })
        # print("list of files:", files)
        return {
            'statusCode': 200,
            'files': files
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'error': str(e)
        }
