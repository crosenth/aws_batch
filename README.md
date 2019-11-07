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

