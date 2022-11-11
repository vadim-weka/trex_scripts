##############################################
import random
import urllib3
import boto3
import logging
import sys
import time
import subprocess
# from minio import Minio
##############################################


TYPES = {
    "awscli": "AwsCliClient",
    "boto": "BotoClient",
    "minio": "MinioClient",
}


class Client:
    @staticmethod
    def get_client(client_type, *args):

        try:
            return eval(TYPES[client_type])(*args)
        except Exception:
            return None

    def create_bucket(self, Bucket):
        NotImplemented

    def delete_bucket(self, Bucket):
        NotImplemented

    def upload_file(self, source_file_path, bucket, dest_file_name):
        NotImplemented

    def head_object(self, Bucket, Key):
        NotImplemented

    def get_object(self, Bucket, Key):
        NotImplemented

    def delete_object(self, Bucket, Key):
        NotImplemented


class MinioClient(Client):
    def __init__(self, s3_url, s3_key, s3_port, s3_secret):
        super().__init__()
        c = urllib3.PoolManager(s3_url, port=9000, cert_reqs='CERT_NONE', assert_hostname=False)
        url = "{}:{}".format(s3_url, s3_port)
        self.client = Minio(url, s3_secret, s3_url, http_client=c)

    def create_bucket(self, Bucket):
        print(Bucket)
        self.client.make_bucket(Bucket)

    def delete_bucket(self, Bucket):
        pass

    def upload_file(self, source_file_path, bucket, dest_file_name):
        pass

    def head_object(self, Bucket, Key):
        pass

    def get_object(self, Bucket, Key):
        pass

    def delete_object(self, Bucket, Key):
        pass


class BotoClient(Client):
    def __init__(self, s3_key, port, s3_secret, s3_url) -> None:
        s3_url = '%s:%s' % (s3_url, port)
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
    def __init__(self, s3_url, s3_port, s3_key, s3_secret):
        try:
            subprocess.check_output(["aws", "--version"])
        except Exception:
            msg = 'Error, Aws cli is not installed'
            logging.error(msg)
            print(msg)
            exit(1)

        self.endpoint_url = s3_url,
        self.endpoint_port = s3_port,
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

    def delete_bucket(self, Bucket):
        pass

    def upload_file(self, source_file_path, bucket, dest_file_name):
        pass

    def head_object(self, Bucket, Key):
        pass

    def get_object(self, Bucket, Key):
        pass

    def delete_object(self, Bucket, Key):
        pass


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
    client_type = argv[1]
    s3_key = argv[2]
    s3_secret = argv[3]
    endpoint_addr = argv[4]
    port = argv[5]
    files_cycles_count = int(argv[6])
    files_size = argv[7]
    buckets_cycles_count = argv[8]

    urllib3.disable_warnings()
    s3_url = 'https://%s' % endpoint_addr
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
            client.create_bucket(bucket_name)
        except Exception as err:
            msg = "Fail to create bucket '%s'" % bucket_name
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

    help_msg = "Synx: ./s3_action_gen <client_type> <s3_key> <s3_secret> <endpoint_address> <port> " \
               "<files_cycles_count> <files_size>  <buckets_cycles_count>"

    if len(sys.argv) < 8:
        print("! Missing argument !")
        print(help_msg)
        exit(1)

    elif sys.argv[1] in ('-h', '--help'):
        print(help_msg)
        exit()

    str_format = "%(asctime)s: %(message)s"
    logging.basicConfig(filename=f"log.log", filemode='a', format=str_format, level=logging.INFO, datefmt="%H:%M:%S")

    run_uid = int(random.random() * 10000000000)
    logging.info('######### Script Start, id: %s start time: %s #############' % (run_uid, int(time.time())))
    run_s3_actions(sys.argv)
    logging.info('######### Script is done, id: %s end time: %s #############\n\n' % (run_uid, int(time.time())))
