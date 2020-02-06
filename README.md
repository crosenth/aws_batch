# AWS Batch Helper

A tool for uploading data to and from a local file system to an AWS S3 Bucket 
and into an AWS Batch container for processing.  All container text output
is monitored through the AWS Logging system and reported back to the user.

Inspired by the [submit-job.py](https://github.com/awslabs/aws-batch-helpers/blob/master/gpu-example/submit-job.py)
script by [kiukA9](https://github.com/kiukA9)

## dependencies

* Python 3.x
* [awscli](https://aws.amazon.com/cli/)

The awscli must also be available within the batch container.

## installation

```
% pip install aws_batch
```

or

```
% pip install git+https://github.com/crosenth/aws_batch
```

## Usage

Running a simple command and sending the results to a script with a little verbosity `-v`:

```
aws_batch -v --job-queue some-queue --bucket s3://my_bucket/ --command "echo hello world > hello_world.txt" --downloads hello_world.txt some_job_definition
Found credentials in shared credentials file: ~/.aws/credentials
mkdir -p tmp; cd tmp; echo hello world > hello_world.txt; /home/ec2-user/miniconda/bin/aws s3 cp --only-show-errors hello_world.txt s3://my_bucket/hello_world.txt
SUBMITTED
RUNNABLE
STARTING
SUCCEEDED
download: s3://my_bucket/hello_world.txt to hello_world.txt
```
