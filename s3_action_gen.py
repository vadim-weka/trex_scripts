##############################################
import random
import urllib3
import boto3
import logging
import sys
import time
import subprocess
##############################################


class AwsCliCmd:
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

        subprocess.check_output(['aws', 'configure', 'set', 'aws_secret_access_key', f'{self.aws_secret_access_key}'])
        subprocess.check_output(['aws', 'configure', 'set', 'aws_access_key_id', f'{self.aws_access_key_id}'])
        subprocess.check_output(['aws', 'configure', 'set', 'region', 'default'])

    def create_bucket(self, Bucket):
        cmd = subprocess.run(['aws',
                                 's3api',
                                 'create-bucket',
                                 '--bucket',
                                 '%s' % Bucket,
                                 '--endpoint-url',
                                 '%s/' % self.endpoint_url,
                                 '--no-verify-ssl'], stderr=subprocess.PIPE)
        cmd.stderr.decode('utf-8')

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


def get_client(client_sype, s3_key, s3_port, s3_secret, s3_url):
    if client_sype == 'boto':
        session = boto3.session.Session()
        client = session.client('s3',
                                endpoint_url=s3_url,
                                aws_access_key_id=s3_key,
                                aws_secret_access_key=s3_secret,
                                region_name='ignored-by-minio',
                                verify=False)

        return client
    elif client_sype == 'awscli':
        return AwsCliCmd(s3_url, s3_port, s3_key, s3_secret)


def create_data_file(file_count, file_size):
    file_list = []
    for seq_num in range(0, file_count):
        file_name = f'test_file{seq_num}'
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
    s3_url = 'https://%s:%s' % (endpoint_addr, port)
    client = get_client(client_type, s3_key, port, s3_secret, s3_url)

    logging.info("-> Creating data file")
    try:
        input_files = create_data_file(files_cycles_count, files_size)
    except Exception as err:
        logging.error(err)
    logging.info("<- File Created")

    for bucket_cycle in range(0, int(buckets_cycles_count)):
        logging.info("-> Creating bucket")
        bucket_name = f'test{bucket_cycle}'
        try:
            client.create_bucket(Bucket=bucket_name)
        except Exception as err:
            msg = "Fail to create bucket '%s'" % bucket_name
            logging.error("%s , error msg: %s" % (msg, err))
            print(msg)

        logging.info("<- Bucket '%s' created" % bucket_name)

        for cycle in range(0, files_cycles_count):
            test_files = []
            for count, file in enumerate(input_files):
                test_file_name = f'test_file{count}'
                logging.info("-> Uploading file '%s' to Bucket '%s'" % (test_file_name, bucket_name))
                test_file_name = f'test_file{count}'
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
                    client.head_object(Bucket=bucket_name, Key=test_file)
                except Exception as err:
                    msg = "Fail to execute head on object '%s' from bucket '%s'" % (file, bucket_name)
                    logging.error("%s, error msg: %s" % (msg, err))
                    print(msg)

                logging.info("<- File '%s' metadata retrieved (*)" % test_file)
                logging.info("-> Downloading File '%s'" % test_file)
                try:
                    client.get_object(Bucket=bucket_name, Key=test_file)
                except Exception as err:
                    msg = "Fail to get object '%s' from bucket '%s'" % (file, bucket_name)
                    logging.error("%s, error msg: %s" % (msg, err))
                    print(msg)

                logging.info("<- File '%s' downloaded (v)" % test_file)

            for test_file in test_files:
                logging.info("-> Deleting file '%s' from S3 bucket '%s'" % (file, bucket_name))
                try:
                    client.delete_object(Bucket=bucket_name, Key=test_file)
                except Exception as err:
                    msg = "Fail to delete object '%s' from bucket '%s'" % (file, bucket_name)
                    logging.error("%s, error msg: %s" % (msg, err))
                    print(msg)

                logging.info("<- File '%s' deleted from S3 bucket '%s' (X)" % (file, bucket_name))

            test_files.clear()

        logging.info("-> Deleting bucket '%s'" % bucket_name)
        try:
            client.delete_bucket(Bucket=bucket_name)
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
