---
title: Authoring detections
sidebar_position: 2
---

Each detection you create occupies a directory under the `detections/` directory in your Matano directory.

A detection directory has the following structure:

```
my-matano-directory
├── detections
│   └── my_detection
│       ├── detect.py
│       ├── requirements.txt
│       └── detection.yml
```

## Detection script

_Detection scripts_ are Python programs containing the logic of your detection. To create a detection script, create a file called `detect.py` in your detection directory.

Inside the detection script, you define the following functions:

The `detect` function is the python function that is invoked for your detection. The function will be invoked with a data record.

The function has the following signature:

```python
def detect(record) -> bool | None:
    ...
```

### Returning values from your detection

Your `detect` function must return a boolean `True` to signal an alert. A return value of `False` or `None` will be interpreted as no alert for detection on that record.

### Examples

Here is a sample Python detection. It runs on AWS CloudTrail logs and detects a failed attempt to export an AWS EC2 instance.

```python
def detect(record):
  if (
    record.get("event", {}).get("action")
    == "CreateInstanceExportTask"
    and record.get("event", {}).get("provider")
    == "ec2.amazonaws.com"
  ):
    aws_cloudtrail = record.get("aws", {}).get("cloudtrail", {})
    if (
      aws_cloudtrail.get("error_message")
      or aws_cloudtrail.get("error_code")
      or "Failure" in aws_cloudtrail.get("response_elements")
    ):
        return True
```



## Detection configuration file (`detection.yml`)

Each detection requires a configuration file named `detection.yml`. The file has the following structure:

```yml
name: "my_detection" # The name of the detection
tables: # An array of table names for which to run the detection
  - "aws_cloudtrail"
```

## Python requirements

You can add a `requirements.txt` file to the detection directory to make PyPI dependencies available to your detection program. The listed dependencies will be installed and made available to your program.
