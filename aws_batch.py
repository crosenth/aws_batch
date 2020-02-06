#!/usr/bin/env python3
'''
Given an AWS registered job-definition and optional command with uploads and
downloads, this script moves data to and from a local filesystem into an AWS S3
Bucket and Batch container for processing. All data written to the AWS Batch
logging system is monitored and displayed to the user.

Requirements:

1. An AWS S3 Bucket
2. awscli must be available in the container for moving data to and from
the AWS S3 bucket

Extended from https://github.com/awslabs/aws-batch-helpers/
blob/master/gpu-example/submit-job.py
---
This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
'''
import argparse
import boto3
import datetime
import logging
import os
import pkg_resources
import subprocess
import sys
import time

parser = argparse.ArgumentParser(
    formatter_class=argparse.RawDescriptionHelpFormatter)
parser.add_argument(
    "jobDefinition",
    help="name of the job job definition")
parser.add_argument(
    "--command",
    help="command to run",
    type=str)
parser.add_argument(
    "--job-name",
    default='default',
    help="name of the job (default: \"%(default)s\")",
    type=str)
parser.add_argument(
    "--job-queue",
    default='optimal',
    help="name of the job queue to submit this job (default: \"%(default)s)\"",
    type=str)
parser.add_argument(
    "--region-name",
    default='us-west-2',
    help="(default: %(default)s)",
    type=str)
s3_parser = parser.add_argument_group('s3 configuration')
s3_parser.add_argument(
    "--bucket",
    help='s3 bucket url to stage uploads and downloads')
s3_parser.add_argument(
    "--dirty",
    action='store_false',
    dest='teardown',
    help='leave files in work bucket')
s3_parser.add_argument(
    "--uploads",
    help='')
s3_parser.add_argument(
    "--downloads",
    help='')
container_parser = parser.add_argument_group('container options')
container_parser.add_argument(
    "--awscli",
    default='/home/ec2-user/miniconda/bin/aws',
    help=("awscli tool in batch job definition "
          "container/ami (default: %(default)s)"))
container_parser.add_argument(
    "--workdir",
    default='/tmp',
    help='container folder to execute command (default: %(default)s)')
container_parser.add_argument(
    '--cpus',
    type=int,
    help='number of vCPUs to reserve for the container')
container_parser.add_argument(
    '--memory',
    type=int,
    metavar='GiB',
    help='number of GiB (~GB) of memory reserved for the job')
logging_parser = parser.add_argument_group(
    title='logging and version options')
logging_parser.add_argument(
    '-V', '--version',
    action='version',
    version=pkg_resources.get_distribution('aws_batch').version,
    help='Print the version number and exit')
logging_parser.add_argument(
    '-l', '--log',
    metavar='',
    default=sys.stdout,
    type=argparse.FileType('a'),
    help='Send logging to a file')
logging_parser.add_argument(
    '-v', '--verbose',
    action='count',
    dest='verbosity',
    default=0,
    help='Increase verbosity of screen output '
         '(eg, -v is verbose, -vv more so)')
logging_parser.add_argument(
    '-q', '--quiet',
    action='store_const',
    dest='verbosity',
    const=0,
    help='Suppress output')

args = parser.parse_args()


def printLogs(cloudwatch, logStreamName, startTime):
    kwargs = {'logGroupName': '/aws/batch/job',
              'logStreamName': logStreamName,
              'startTime': startTime,
              'startFromHead': True}
    lastTimestamp = 0
    while True:
        logEvents = cloudwatch.get_log_events(**kwargs)
        for event in logEvents['events']:
            lastTimestamp = event['timestamp']
            lastTime = lastTimestamp / 1000.0
            timestamp = datetime.datetime.utcfromtimestamp(lastTime)
            msg = event['message'].strip()
            print(msg)
            logging.debug('[{:%Y-%m-%d %H:%M:%S}] {}'.format(timestamp, msg))
        nextToken = logEvents['nextForwardToken']
        if nextToken and kwargs.get('nextToken') != nextToken:
            kwargs['nextToken'] = nextToken
        else:
            break
    return lastTimestamp


# TODO: raise error if no awscli in container
# TODO: figure out aws batch/container error handling
def container_sh(cli, bucket, workdir, cmd, uploads, downloads):
    commands = ['mkdir -p ' + workdir, 'cd ' + workdir]
    for i in uploads + downloads:
        dname = os.path.dirname(i)
        if dname:
            commands.append('mkdir -p ' + dname)
    for i in uploads:
        s3_path = os.path.join(bucket, i.lstrip('/'))
        commands.append('{} s3 cp --only-show-errors {} {}'.format(
            cli, s3_path, i))
        commands.append('chmod 777 ' + i)
    commands.append(cmd)
    for i in downloads:
        s3_path = os.path.join(bucket, i.lstrip('/'))
        commands.append('{} s3 cp --only-show-errors {} {}'.format(
            cli, i, s3_path))
    return '; '.join(commands)


def s3_download(bucket, downloads):
    for i in downloads:
        path = os.path.join(bucket, i.lstrip('/'))
        cmd = 'aws s3 cp {} {}'.format(path, i)
        out = subprocess.run(
            cmd.split(),
            check=True,
            encoding='utf-8',
            stdout=subprocess.PIPE).stdout
        logging.info(out.strip())


# TODO: check for local awscli
def s3_upload(bucket, uploads):
    for i in uploads:
        path = os.path.join(bucket, i.lstrip('/'))
        cmd = 'aws s3 cp {} {}'.format(i, path)
        out = subprocess.run(
            cmd.split(),
            check=True,
            encoding='utf-8',
            stdout=subprocess.PIPE).stdout
        logging.info(out.strip())


def setup_logging(namespace):
    log = namespace.log
    loglevel = {
        0: logging.ERROR,
        1: logging.INFO,
        2: logging.DEBUG,
    }.get(namespace.verbosity, logging.DEBUG)
    logging.basicConfig(stream=log, format='%(message)s', level=loglevel)


def check_args(args):
    if not args.bucket and (args.uploads or args.downloads):
        raise ValueError('--uploads and --downloads requires --bucket')
    if args.bucket and not (args.uploads or args.downloads):
        logging.warn('--bucket specified without --uploads or --downloads')


def main():
    setup_logging(args)
    check_args(args)
    batch = boto3.client(
        service_name='batch',
        region_name=args.region_name,
        endpoint_url='https://batch.{}.amazonaws.com'.format(args.region_name))
    cloudwatch = boto3.client(
        service_name='logs',
        region_name=args.region_name,
        endpoint_url='https://logs.{}.amazonaws.com'.format(args.region_name))
    uploads = args.uploads.split(',') if args.uploads else []
    downloads = args.downloads.split(',') if args.downloads else []
    workdir = args.workdir.lstrip('/')
    if args.bucket:
        s3_upload(args.bucket, uploads)
    sh = container_sh(
        args.awscli, args.bucket, workdir, args.command, uploads, downloads)
    overrides = {'command': ['/bin/bash',  '-c', sh]}
    if args.cpus:
        overrides.update({'vcpus': args.cpus})
    if args.memory:
        overrides.update({'memory': args.memory * 1024})
    logging.info(sh)
    submitJobResponse = batch.submit_job(
        jobName=args.job_name,
        jobQueue=args.job_queue,
        jobDefinition=args.jobDefinition,
        containerOverrides=overrides)
    jobId = submitJobResponse['jobId']
    logStreamName = None
    startTime = 0
    status = None
    logging.info('{} "{}"'.format(args.jobDefinition, args.command))
    while True:
        time.sleep(1)
        describeJobsResponse = batch.describe_jobs(jobs=[jobId])
        job = describeJobsResponse['jobs'][0]
        if status != job['status']:
            status = job['status'].strip()
            if status not in ['SUCCEEDED', 'FAILED']:
                logging.info(status)
            if status in ['RUNNING', 'SUCCEEDED', 'FAILED']:
                if (logStreamName is None and
                        'logStreamName' in job['container']):
                    logStreamName = job['container']['logStreamName']
                if logStreamName:
                    startTime = printLogs(cloudwatch, logStreamName, startTime)
                    startTime += 1
            if status in ['SUCCEEDED', 'FAILED']:
                logging.info(status)
                break
    if args.bucket:
        s3_download(args.bucket, downloads)
        if args.teardown:
            cloud_path = os.path.join(args.bucket, workdir)
            cmd = 'aws s3 rm --recursive ' + cloud_path
            out = subprocess.run(
                cmd.split(),
                check=True,
                encoding='utf-8',
                stdout=subprocess.PIPE).stdout.strip()
            if out:
                logging.info(out)


if __name__ == "__main__":
    main()
