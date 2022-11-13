# trex_scripts

python3.7

### Install
`pip install -r requirements`

### Help:

`./s3_action_gen -s3_client=<minio|boto|awscli|curl> -s3_key_id=<access_key_id> ' 
               '-s3_key_secret=<secret_access_key> -endpoint=<IP|DNS-Name> -endpoint_port=<TCP_port> ' 
               '-files_amount=<Number> -file_size=<1b|1k|1m|1g|etc.> -bucket_count=<Number>'`

### Example:
Run script : `python3 s3_action_gen.py -s3_client=minio -s3_key_id=shuni -s3_key_secret=Aa123456 -endpoint=shuni-0.wekalab.io -endpoint_port=9000 -files_amount=5 -file_size=2m -bucket_count=1` 

#### Log file: log saved to the file with client name: `minio.log`

Notes:
cURL client supports only get objects. The bucket creation and file upload done by boto and only `get_object` using cURL
