##############################################
import random
import string
import uuid

import urllib3
import boto3
import logging
import sys
import time
import subprocess
from minio import Minio

##############################################


TYPES = {
    "awscli": "AwsCliClient",
    "boto": "BotoClient",
    "minio": "MinioClient",
    "curl": "CurlClient"
}


class Client:
    @staticmethod
    def get_client(client_type, *args):

        try:
            return eval(TYPES[client_type])(*args)
        except Exception:
            return None

    def create_bucket(self, bucket):
        NotImplemented

    def delete_bucket(self, bucket):
        NotImplemented

    def upload_file(self, source_file_path, bucket, dest_file_name):
        NotImplemented

    def head_object(self, bucket, key):
        NotImplemented

    def get_object(self, bucket, key):
        NotImplemented

    def delete_object(self, bucket, key):
        NotImplemented


class MinioClient(Client):
    def __init__(self, s3_key, port, s3_secret, s3_url):
        super().__init__()
        c = urllib3.PoolManager(s3_url, port=9000, cert_reqs='CERT_NONE', assert_hostname=False)
        url = "%s" % s3_url if port is None else "%s:%s" % (s3_url, port)
        self.client = Minio(url, s3_key, s3_secret, http_client=c)

    def create_bucket(self, bucket):
        self.client.make_bucket(bucket_name=str(bucket))

    def delete_bucket(self, bucket):
        self.client.remove_bucket(str(bucket))

    def upload_file(self, source_file_path, bucket, dest_file_name):
        self.client.fput_object(str(bucket), object_name=dest_file_name, file_path=source_file_path)

    def head_object(self, bucket, key):
        self.client.get_object(bucket_name=str(bucket), object_name=key)

    def get_object(self, bucket, key):
        letters = string.ascii_lowercase
        file_path = "files/{}".format(''.join(random.choice(letters) for i in range(10)))
        self.client.fget_object(bucket_name=bucket, object_name=key, file_path=file_path)

    def delete_object(self, bucket, key):
        self.client.remove_object(bucket_name=str(bucket), object_name=key)


class BotoClient(Client):
    def __init__(self, s3_key, port, s3_secret, s3_url) -> None:

        s3_url = 'https://%s' % s3_url if port is None else 'https://%s:%s' % (s3_url, port)
        session = boto3.session.Session()
        self.client = session.client('s3',
                                     endpoint_url=s3_url,
                                     aws_access_key_id=s3_key,
                                     aws_secret_access_key=s3_secret,
                                     region_name='ignored-by-minio',
                                     verify=False)

    def create_bucket(self, bucket):
        return self.client.create_bucket(Bucket=bucket)

    def delete_bucket(self, bucket):
        return self.client.delete_bucket(Bucket=bucket)

    def upload_file(self, source_file_path, bucket, dest_file_name):
        return self.client.upload_file(source_file_path, bucket, dest_file_name)

    def head_object(self, bucket, key):
        return self.client.head_object(Bucket=bucket, Key=key)

    def get_object(self, bucket, key):
        return self.client.get_object(Bucket=bucket, Key=key)

    def delete_object(self, bucket, key):
        return self.client.delete_object(Bucket=bucket, Key=key)


class AwsCliClient(Client):
    def __init__(self, s3_key, s3_port, s3_secret, s3_url):
        try:
            subprocess.check_output(["aws", "--version"])
        except Exception:
            msg = 'Error, Aws cli is not installed'
            logging.error(msg)
            print(msg)
            exit(1)
        s3_url = 'https://%s' % s3_url if s3_port is None else 'https://%s:%s' % (s3_url, s3_port)
        self.endpoint_url = s3_url,
        self.aws_access_key_id = s3_key,
        self.aws_secret_access_key = s3_secret

        subprocess.check_output(['aws', 'configure', 'set', 'aws_access_key_id', '%s' % self.aws_access_key_id])
        subprocess.check_output(['aws', 'configure', 'set', 'aws_secret_access_key', '%s' % self.aws_secret_access_key])
        subprocess.check_output(['aws', 'configure', 'set', 'region', 'default'])

    def create_bucket(self, bucket):
        cmd = subprocess.run(['aws',
                              's3api',
                              'create-bucket',
                              '--bucket',
                              '%s' % bucket,
                              '--endpoint-url',
                              '%s/' % self.endpoint_url,
                              '--no-verify-ssl'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logging.info(cmd.stderr)
        logging.info(cmd.stdout)
        if cmd.returncode != 0:
            print(cmd.stderr)

    def delete_bucket(self, bucket):
        cmd = subprocess.run(['aws',
                              's3api',
                              'delete-bucket',
                              '--bucket',
                              '%s' % bucket,
                              '--endpoint-url',
                              '%s/' % self.endpoint_url,
                              '--no-verify-ssl'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logging.info(cmd.stderr)
        logging.info(cmd.stdout)
        if cmd.returncode != 0:
            print(cmd.stderr)

    def upload_file(self, source_file_path, bucket, dest_file_name):
        cmd = subprocess.run(['aws',
                              's3api',
                              'put-object',
                              '--bucket',
                              '%s' % bucket,
                              '--key',
                              '%s' % dest_file_name,
                              '--body',
                              '%s' % source_file_path,
                              '--endpoint-url',
                              '%s/' % self.endpoint_url,
                              '--no-verify-ssl'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logging.info(cmd.stderr)
        logging.info(cmd.stdout)
        if cmd.returncode != 0:
            print(cmd.stderr)

    def head_object(self, bucket, key):
        cmd = subprocess.run(['aws',
                              's3api',
                              'head-object',
                              '--bucket',
                              '%s' % bucket,
                              '--key',
                              '%s' % key,
                              '--endpoint-url',
                              '%s/' % self.endpoint_url,
                              '--no-verify-ssl'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logging.info(cmd.stderr)
        logging.info(cmd.stdout)
        if cmd.returncode != 0:
            print(cmd.stderr)

    def get_object(self, bucket, key):
        cmd = subprocess.run(['aws',
                              's3api',
                              'get-object',
                              '--bucket',
                              '%s' % bucket,
                              '--key',
                              '%s' % key,
                              '--endpoint-url',
                              '%s/' % self.endpoint_url,
                              '--no-verify-ssl',
                              'object_output'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logging.info(cmd.stderr)
        logging.info(cmd.stdout)
        if cmd.returncode != 0:
            print(cmd.stderr)

    def delete_object(self, bucket, key):
        cmd = subprocess.run(['aws',
                              's3api',
                              'delete-object',
                              '--bucket',
                              '%s' % bucket,
                              '--key',
                              '%s' % key,
                              '--endpoint-url',
                              '%s/' % self.endpoint_url,
                              '--no-verify-ssl'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logging.info(cmd.stderr)
        logging.info(cmd.stdout)
        if cmd.returncode != 0:
            print(cmd.stderr)


class CurlClient(BotoClient):
    def __init__(self, s3_key, port, s3_secret, s3_url):
        super().__init__(s3_key, port, s3_secret, s3_url)

    def get_object(self, bucket, key):
        url = self.client.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': key}, ExpiresIn=600)
        try:
            subprocess.check_output(["curl", "-k", url, "-o", key], stderr=subprocess.PIPE)
        except Exception as err:
            logging.error("<-- CMD '%s', error msg: %s" % err)


def create_data_file(file_count, file_size, client_type):
    file_list = []
    for seq_num in range(0, file_count):
        file_name = f'{client_type}{seq_num}'
        logging.info("--> File name: '%s', file size: %s'" % (file_name, file_size))
        try:
            subprocess.check_output(['dd', 'if=/dev/urandom', f'of=./{file_name}', 'count=1', f'bs={file_size}'],
                                    stderr=subprocess.PIPE)
        except Exception as err:
            logging.error("<-- Fail to create file '%s', error msg: %s" % (file_name, err))
        file_list.append(file_name)
    return file_list


def run_s3_actions(argv):
    client_type = param['-s3_client']
    s3_key = param['-s3_key_id']
    s3_secret = param['-s3_key_secret']
    s3_url = param['-endpoint']
    try:
        port = param['-endpoint_port']
    except Exception:
        port = None
    files_cycles_count = int(param['-files_amount'])
    files_size = param['-file_size']
    buckets_cycles_count = param['-bucket_count']

    urllib3.disable_warnings()
    client = Client.get_client(client_type, s3_key, port, s3_secret, s3_url)

    logging.info("-> Creating data file")
    try:
        input_files = create_data_file(files_cycles_count, files_size, client_type)
    except Exception as err:
        logging.error(err)
    logging.info("<- File Created")

    for bucket_cycle in range(0, int(buckets_cycles_count)):
        logging.info("-> Creating bucket")
        bucket_name = f'{client_type}{bucket_cycle}'
        try:
            for i in range(3):
                try:
                    time.sleep(1)
                    client.create_bucket(bucket_name)
                    break
                except Exception:
                    continue
        except Exception as err:
            msg = 'Fail to createΩ bucket \'%s\'' % bucket_name
            logging.error("%s , error msg: %s" % (msg, err))
            print(msg)

        logging.info("<- Bucket '%s' created" % bucket_name)

        for cycle in range(0, files_cycles_count):
            test_files = []
            for count, file in enumerate(input_files):
                test_file_name = f'{client_type}{count}'
                logging.info("-> Uploading file '%s' to Bucket '%s'" % (test_file_name, bucket_name))
                try:
                    client.upload_file(f'./{file}', bucket_name, test_file_name)
                except Exception as err:
                    msg = "Fail to upload file '%s' to bucket '%s'" % (file, bucket_name)
                    logging.error("%s, error msg: %s" % (msg, err))
                    print(msg)

                logging.info("<- File '%s' Uploaded to Bucket '%s' (^)" % (test_file_name, bucket_name))
                test_files.append(test_file_name)

            for test_file in test_files:
                logging.info("-> Retrieving File '%s' metadata" % test_file)
                try:
                    client.head_object(bucket_name, test_file)
                except Exception as err:
                    msg = "Fail to execute head on object '%s' from bucket '%s'" % (file, bucket_name)
                    logging.error("%s, error msg: %s" % (msg, err))
                    print(msg)

                logging.info("<- File '%s' metadata retrieved (*)" % test_file)
                logging.info("-> Downloading File '%s'" % test_file)
                try:
                    client.get_object(bucket_name, test_file)
                except Exception as err:
                    msg = "Fail to get object '%s' from bucket '%s'" % (file, bucket_name)
                    logging.error("%s, error msg: %s" % (msg, err))
                    print(msg)

                logging.info("<- File '%s' downloaded (v)" % test_file)

            for test_file in test_files:
                logging.info("-> Deleting file '%s' from S3 bucket '%s'" % (file, bucket_name))
                try:
                    client.delete_object(bucket_name, test_file)
                except Exception as err:
                    msg = "Fail to delete object '%s' from bucket '%s'" % (file, bucket_name)
                    logging.error("%s, error msg: %s" % (msg, err))
                    print(msg)

                logging.info("<- File '%s' deleted from S3 bucket '%s' (X)" % (file, bucket_name))

            test_files.clear()

        logging.info("-> Deleting bucket '%s'" % bucket_name)
        try:
            client.delete_bucket(bucket_name)
        except Exception as err:
            msg = "Fail to delete bucket '%s', error msg: %s" % (bucket_name, err)
            logging.error("%s, error msg: %s" % (msg, err))
            print(msg)

        logging.info("<- Bucket '%s' deleted " % bucket_name)


if __name__ == "__main__":

    help_msg = 'Synx: ./s3_action_gen -s3_client=<minio|boto|awscli> -s3_key_id=<access_key_id> ' \
               '-s3_key_secret=<secret_access_key> -endpoint=<IP|DNS-Name> -endpoint_port=<TCP_port> ' \
               '-files_amount=<Number> -file_size=<1b|1k|1m|1g|etc.> -bucket_count=<Number>'

    if len(sys.argv) < 8:
        print("! Missing argument !")
        print(help_msg)
        exit(1)

    elif sys.argv[1] in ('-h', '--help'):
        print(help_msg)
        exit()

    param = {}
    for arg in sys.argv[1:]:
        param[arg.split("=")[0]] = arg.split("=")[1]

    str_format = "%(asctime)s: %(message)s"
    logging.basicConfig(filename=f"{param['-s3_client']}.log", filemode='a', format=str_format, level=logging.INFO, datefmt="%H:%M:%S")

    run_uid = int(random.random() * 10000000000)
    logging.info('######### Script Start, id: %s start time: %s #############' % (run_uid, int(time.time())))
    run_s3_actions(sys.argv)
    logging.info('######### Script is done, id: %s end time: %s #############\n\n' % (run_uid, int(time.time())))
