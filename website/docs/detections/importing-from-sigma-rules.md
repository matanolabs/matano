---
title: Importing from Sigma rules
sidebar_position: 2
---

[Sigma][1] is a popular open source vendor agnostic format for writing detection rules in a YAML-based signature format. You can import Sigma rules into Matano Python detections.

## How to import Sigma rules into Python detections

:::note

Matano only supports the newer [pySigma][1], not the legacy sigma.

:::

:::info

You cannot currently use the [`sigma` cli][5] to convert Sigma rules to Matano detections. You can use a Python script that you can execute. Follow this [Sigma issue][2] for integration with sigma CLI.

:::

To import Sigma rules into Matano Python detections, you use the [_Matano backend for Sigma_][3] which translates Sigma rules into Python detections.

### Steps

1. Install the Matano backend for Sigma by running `pip install git+https://github.com/matanolabs/pySigma-backend-matano.git`.
1. Download the [following Python script][4].
1. Execute the script by running `python sigma_generate.py <filepath>`.
   - You can pass a pipeline using the `--pipeline` argument. Run the help command for a list of supported pipelines (e.g. ECS CloudTrail).
1. The script will output a ready to use detection directory in the current directory, containing a `detect.py` script and `detection.yml` configuration file.
1. You can copy the detection directory into your Matano directory to use the detection.

## Example

The following Sigma Rule:

```yml
title: AWS EC2 Disable EBS Encryption
id: 16124c2d-e40b-4fcc-8f2c-5ab7870a2223
status: stable
description: Identifies disabling of default Amazon Elastic Block Store (EBS) encryption in the current region. Disabling default encryption does not change the encryption status of your existing volumes.
author: Sittikorn S
date: 2021/06/29
modified: 2021/08/20
references:
  - https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DisableEbsEncryptionByDefault.html
tags:
  - attack.impact
  - attack.t1486
  - attack.t1565
logsource:
  product: aws
  service: cloudtrail
detection:
  selection:
    eventSource: ec2.amazonaws.com
    eventName: DisableEbsEncryptionByDefault
  condition: selection
falsepositives:
  - System Administrator Activities
  - DEV, UAT, SAT environment. You should apply this rule with PROD account only.
level: medium
```

will be converted into a Matano detection with the following `detection.yml` and `detect.py`:

```yml
# detection.yml
# This file was generated from a Sigma rule

author: Sittikorn S
date: "2021-06-29"
description: Identifies disabling of default Amazon Elastic Block Store (EBS) encryption
  in the current region. Disabling default encryption does not change the encryption
  status of your existing volumes.
id: 16124c2d-e40b-4fcc-8f2c-5ab7870a2223
level: medium
references:
  - https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DisableEbsEncryptionByDefault.html
status: stable

name: aws_ec2_disable_ebs_encryption
tables:
  - aws_cloudtrail
```

```python
# detect.py
import re, json, functools, ipaddress
from fnmatch import fnmatch

def detect(record):
    return (
        record.get("event", {}).get("provider") == "ec2.amazonaws.com"
        and record.get("event", {}).get("action") == "DisableEbsEncryptionByDefault"
    )
```

[1]: https://github.com/SigmaHQ/pySigma
[2]: https://github.com/SigmaHQ/sigma-cli/issues/3
[3]: https://github.com/matanolabs/pySigma-backend-matano
[4]: https://github.com/matanolabs/matano/blob/main/scripts/sigma_generate.py
[5]: https://github.com/SigmaHQ/sigma-cli
