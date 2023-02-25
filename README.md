<p align="center">
  <a href="https://www.matano.dev"><img src="assets/cover_wide.png" width=600></a>
</p>

<p align="center">
         <!-- <a href="#"><img src="https://img.shields.io/badge/Deploys%20to-AWS-%23FF9900.svg?style=for-the-badge&logo=amazon-aws&logoColor=white&labelColor=232F3E"/></a>
        <a href="#"><img src="https://img.shields.io/badge/rust-%233A3B3C.svg?style=for-the-badge&logo=rust&labelColor=B1513E&logoColor=white"/></a>
         <br/> -->
         <a href="https://discord.gg/YSYfHMbfZQ" target="_blank"><img src="https://img.shields.io/badge/rust-%233A3B3C.svg?label=built with&logo=rust&logoColor=ffffff&color=B1513E&labelColor=0d1117"/></a>
                  <a href="#"><img src="https://img.shields.io/badge/deploys%20to-aws-%23FF9900.svg?logo=amazon-aws&logoColor=white&labelColor=232F3E"/></a>
 <a href="https://discord.gg/YSYfHMbfZQ" target="_blank"><img src="https://img.shields.io/discord/996484553290022973.svg?label=join us&logo=discord&logoColor=ffffff&color=7389D8&labelColor=6A7EC2"/></a>
        <a href="https://twitter.com/intent/follow?screen_name=matanolabs" target="_blank"><img src="https://img.shields.io/twitter/follow/matanolabs?style=social" alt="Twitter Follow"/></a>
                <a href="/LICENSE" target="_blank"><img src="https://img.shields.io/github/license/matanolabs/matano?style=flat"/></a>
<a href="https://bestpractices.coreinfrastructure.org/projects/6478"><img src="https://bestpractices.coreinfrastructure.org/projects/6478/badge"></a>
</p>

## Matano is an open source security lake platform for AWS

<p align="center">
  <strong>
    <a href="https://www.matano.dev/blog/2022/08/11/announcing-matano?utm_source=ghr">🔔 Read our announcement blog post 🔔</a>
  </strong>
</p>

Matano is an open source **cloud-native alternative to SIEM**, built for security teams on AWS.

We are on a mission to build the first open platform for threat hunting, detection & response, and cybersecurity analytics at petabyte scale.

<div>
<h3 align="center">
        <a href="https://www.matano.dev/docs">Docs</a>
        <span> | </span>
        <a href="https://www.matano.dev">Website</a>
        <span> | </span>
        <a href="https://discord.gg/YSYfHMbfZQ">Community</a>
</h3>
</div>

## Features

<div align="center">
  <br>
  <img src="assets/matano_athena.png" width="650">
</div>

<br>

- **Security Data Lake:** Matano normalizes unstructured security logs into a structured realtime data lake in your AWS account.
- **Collect All Your Logs:** Matano integrates out of the box with [50+ sources](https://www.matano.dev/docs/log-sources/managed-log-sources) for security logs and can easily be extended with custom sources.
- **Detection-as-Code:** Use Python to build realtime detections as code. Support for automatic import of [Sigma](https://www.matano.dev/docs/detections/importing-from-sigma-rules) detections to Matano.
- **Log Transformation Pipeline:** Matano supports custom VRL ([Vector Remap Language](https://vector.dev/docs/reference/vrl/)) scripting to parse, enrich, normalize and transform your logs as they are ingested without managing any servers.
- **No Vendor Lock-In:** Matano uses an open table format ([Apache Iceberg](https://iceberg.apache.org/)) and open schema standards ([ECS](https://github.com/elastic/ecs)), to give you full ownership of your security data in a vendor-neutral format.
- **Bring Your Own Analytics:** Query your security lake directly from any Iceberg-compatible engine (AWS Athena, Snowflake, Spark, Trino etc.) without having to copy data around.
- **Serverless:** Matano is _fully serverless_ and designed specifically for AWS and focuses on enabling high scale, low cost, and zero-ops.

## Architecture

<div align="center">
  <br>
  <img src="assets/diagram.png" width="600">
</div>

## 👀 Use cases

- Reduce SIEM costs drastically (1/10th the cost).
- Augment your SIEM with a security data lake for additional context during investigations.
- Instantly search for matches to IOCs across your data lake using standardized fields.
- Write detections-as-code using Python to detect suspicious behavior & create contextualized alerts.
- Easier to use cloud-native open source SIEM alternative for detection & response.
- ECS-compatible serverless alternative to ELK / Elastic Security stack.

## ✨ Integrations

#### Managed log sources

- [**AWS CloudTrail**](https://www.matano.dev/docs/log-sources/managed-log-sources/aws/cloudtrail)
- [**AWS Route53**](https://www.matano.dev/docs/log-sources/managed-log-sources/aws/route53-resolver-logs)
- [**AWS VPC Flow**](https://www.matano.dev/docs/log-sources/managed-log-sources/aws/vpcflow)
- [**AWS Config**](https://www.matano.dev/docs/log-sources/managed-log-sources/aws/aws-config)
- [**AWS ELB**](https://www.matano.dev/docs/log-sources/managed-log-sources/aws/aws-elb)
- [**Amazon S3 Server Access**](https://www.matano.dev/docs/log-sources/managed-log-sources/aws/amazon-s3-server-access-logs)
- [**Amazon S3 Inventory Reports**](https://www.matano.dev/docs/log-sources/managed-log-sources/aws/s3-inventory-report)
- [**Amazon Inspector**](https://www.matano.dev/docs/log-sources/managed-log-sources/aws/amazon-inspector)
- [**Amazon WAF**](https://www.matano.dev/docs/log-sources/managed-log-sources/aws/amazon-waf)
- [**Google Workspace**](https://www.matano.dev/docs/log-sources/managed/google-workspace)
- [**Office 365**](https://www.matano.dev/docs/log-sources/managed-log-sources/office365)
- [**Crowdstrike**](https://www.matano.dev/docs/log-sources/managed-log-sources/crowdstrike)
- [**Duo**](https://www.matano.dev/docs/log-sources/managed-log-sources/duo)
- [**Okta**](https://www.matano.dev/docs/log-sources/managed-log-sources/okta)
- [**GitHub**](https://www.matano.dev/docs/log-sources/managed-log-sources/github)
- [**Snyk**](https://www.matano.dev/docs/log-sources/managed-log-sources/snyk)
- [**Suricata**](https://www.matano.dev/docs/log-sources/managed-log-sources/suricata)
- [**Zeek**](https://www.matano.dev/docs/log-sources/managed-log-sources/zeek)
- [**Custom 🔧**](#-log-transformation--data-normalization)

#### Alert destinations

- [**Amazon SNS**](https://www.matano.dev/docs/detections/alerting)
- [**Slack**](https://www.matano.dev/docs/detections/alerting/slack)

#### Enrichment / Threat Intelligence

- **AbuseCH** ([URLhaus](https://urlhaus.abuse.ch), [MalwareBazaar](https://bazaar.abuse.ch), [ThreatFox](https://threatfox.abuse.ch))
- [**AlienVault OTX**](https://otx.alienvault.com/api)
- **MaxMind GeoIP** (_coming soon_)
- **GreyNoise Intelligence** (_coming soon_)
- **Custom 🔧** (_coming soon_)

#### Query engines

- [**Amazon Athena**](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg.html) (default)
- [**Snowflake**](https://www.snowflake.com/blog/iceberg-tables-powering-open-standards-with-snowflake-innovations/) (preview)
- [**Spark**](https://iceberg.apache.org/spark-quickstart/)
- [**Trino**](https://trino.io/docs/current/connector/iceberg.html)
- [**BigQuery Omni (BigLake)**](https://cloud.google.com/biglake)
- [**Dremio**](https://docs.dremio.com/software/data-formats/apache-iceberg/)

## Quick start

[**View the complete installation instructions**](https://www.matano.dev/docs/getting-started#installation)

### Installation

Install the matano CLI to deploy Matano into your AWS account, and manage your Matano deployment.

**Linux**

```bash
curl -OL https://github.com/matanolabs/matano/releases/download/nightly/matano-linux-x64.sh
chmod +x matano-linux-x64.sh
sudo ./matano-linux-x64.sh
```

**macOS**

```bash
curl -OL https://github.com/matanolabs/matano/releases/download/nightly/matano-macos-x64.sh
chmod +x matano-macos-x64.sh
sudo ./matano-macos-x64.sh
```

### Deployment

[**Read the complete docs on getting started**](https://www.matano.dev/docs/getting-started)

To get started with Matano, run the `matano init` command.

- Make sure you have AWS credentials in your environment (or in an AWS CLI profile).
- The interactive CLI wizard will walk you through getting started by generating an initial [Matano directory](https://www.matano.dev/docs/matano-directory) for you, initializing your AWS account, and deploying Matano into your AWS account.
- Initial deployment takes a few minutes.

<div align="center">
  <img src="assets/matano-init.gif" width="600">
</div>

<br>

### Directory structure

Once initialized, your [Matano directory](https://www.matano.dev/docs/matano-directory) is used to control & manage all resources in your project e.g. log sources, detections, and other configuration. It is structured as follows:

```bash
➜  example-matano-dir git:(main) tree
├── detections
│   └── aws_root_credentials
│       ├── detect.py
│       └── detection.yml
├── log_sources
│   ├── cloudtrail
│   │   ├── log_source.yml
│   │   └── tables
│   │       └── default.yml
│   └── zeek
│       ├── log_source.yml
│       └── tables
│           └── dns.yml
├── matano.config.yml
└── matano.context.json
```

When onboarding a new log source or authoring a detection, run `matano deploy` from anywhere in your project to deploy the changes to your account.

## 🔧 Log Transformation & Data Normalization

[**Read the complete docs on configuring custom log sources**](https://www.matano.dev/docs/log-sources/configuration)

Matano uses [Vector Remap Language (VRL)](https://vector.dev/docs/reference/vrl/), to allow users to easily onboard custom log sources and encourages you to normalize fields according to the [Elastic Common Schema (ECS)](https://www.elastic.co/guide/en/ecs/current/ecs-reference.html) to enable enhanced pivoting and bulk search for IOCs across your security data lake.

Users can define custom VRL programs to parse and transform unstructured logs as they are being ingested through one of the supported mechanisms for a log source (e.g. S3, SQS).

VRL is an expression-oriented language designed for transforming observability data (e.g. logs) in a safe and performant manner. It features a simple syntax and a rich set of built-in functions tailored specifically to observability use cases.

### Example: parsing JSON

Let's have a look at a simple example. Imagine that you're working with
HTTP log events that look like this:

```json
{
  "line": "{\"status\":200,\"srcIpAddress\":\"1.1.1.1\",\"message\":\"SUCCESS\",\"username\":\"ub40fan4life\"}"
}
```

You want to apply these changes to each event:

- Parse the raw `line` string into JSON, and explode the fields to the top level
- Rename `srcIpAddress` to the `source.ip` ECS field
- Remove the `username` field
- Convert the `message` to lowercase

Adding this VRL program to your log source as a `transform` step would accomplish all of that:

###### log_source.yml

```yml
transform: |
  . = object!(parse_json!(string!(.json.line)))
  .source.ip = del(.srcIpAddress)
  del(.username)
  .message = downcase(string!(.message))

schema:
  ecs_field_names:
    - source.ip
    - http.status
```

The resulting event 🎉:

```json
{
  "message": "success",
  "status": 200,
  "source": {
    "ip": "1.1.1.1"
  }
}
```

## 📝 Writing Detections

[**Read the complete docs on detections**](https://www.matano.dev/docs/detections)

Use Matano detections to define rules that can alert on threats in your security logs. Matano users define _detections as code_ (DaC). A _detection_ is a Python program that is invoked with data from a log source in realtime and can create an _alert_.

### Examples

#### Detect failed attempts to export AWS EC2 instance in AWS CloudTrail logs.

```python
def detect(record):
  return (
    record.deepget("event.action") == "CreateInstanceExportTask"
    and record.deepget("event.provider") == "ec2.amazonaws.com"
    and record.deepget("event.outcome") == "failure"
  )
```

#### Detect Brute Force Logins by IP across all configured log sources (e.g. Okta, AWS, GWorkspace)

###### detect.py

```python
def detect(r):
    return (
        "authentication" in r.deepget("event.category", [])
        and r.deepget("event.outcome") == "failure"
    )


def title(r):
    return f"Multiple failed logins from {r.deepget('user.full_name')} - {r.deepget('source.ip')}"


def dedupe(r):
    return r.deepget("source.ip")
```

###### detection.yml

```yaml
---
tables:
  - aws_cloudtrail
  - okta_system
  - o365_audit
alert:
  severity: medium
  threshold: 5
  deduplication_window_minutes: 15
  destinations:
    - slack_my_team
```

#### Detect Successful Login from never before seen IP for User

```python
from detection import remotecache

# a cache of user -> ip[]
user_to_ips = remotecache("user_ip")

def detect(record):
    if (
      record.deepget("event.action") == "ConsoleLogin" and
      record.deepget("event.outcome") == "success"
    ):
        # A unique key on the user name
        user = record.deepget("user.name")

        existing_ips = user_to_ips[user] or []
        updated_ips = user_to_ips.add_to_string_set(
          user,
          record.deepget("source.ip")
        )

        # Alert on new IPs
        new_ips = set(updated_ips) - set(existing_ips)
        if existing_ips and new_ips:
            return True
```

## 🚨 Alerting

[**Read the complete docs on alerting**](https://www.matano.dev/docs/detections/alerting)

#### Alerts Matano table

All alerts are automatically stored in a Matano table named `matano_alerts`. The alerts and rule matches are normalized to ECS and contain context about the original event that triggered the rule match, along with the alert and rule data.

**Example Queries**

Summarize alerts in the last week that are activated (exceeded the threshold)

```sql
select
  matano.alert.id as alert_id,
  matano.alert.rule.name as rule_name,
  max(matano.alert.title) as title,
  count(*) as match_count,
  min(matano.alert.first_matched_at) as first_matched_at,
  max(ts) as last_matched_at,
  array_distinct(flatten(array_agg(related.ip))) as related_ip,
  array_distinct(flatten(array_agg(related.user))) as related_user,
  array_distinct(flatten(array_agg(related.hosts))) as related_hosts,
  array_distinct(flatten(array_agg(related.hash))) as related_hash
from
  matano_alerts
where
  matano.alert.first_matched_at > (current_timestamp - interval '7' day)
  and matano.alert.activated = true
group by
  matano.alert.rule.name,
  matano.alert.id
order by
  last_matched_at desc
```

#### Delivering alerts

Matano allows you to deliver alerts to external systems. You can use the Matano alerting SNS topic to deliver alerts to Email, Slack, and other services.

<div align="center">
  <br>
  <img src="assets/matano_slack_alert.png" width="600">
  <br>
  <i>A medium severity alert delivered to Slack</i>
</div>

## ❔ Why Matano?

- Traditional tools used to analyze security data (SIEMs) don’t scale, and are too expensive and difficult to manage for cloud-based security teams.
- Cybersecurity vendors lock your data in proprietary formats which make it difficult to use outside of their product. With Matano, all your data is in open Apache Iceberg tables that can can be directly queried from different tools (AWS Athena, Snowflake, etc.) without having to copy any data.
- **Security is a Big Data problem**: collecting data from your network, SaaS, and cloud environments can exceed 100TBs of data. Security teams are forced to either not collect some data, leave data unprocessed, or build an in-house data lake to cost-effectively analyze large datasets. Matano helps you easily build a security data lake with all features needed for detection and response.
- At scale, without a strategy to normalize data into a structured format, it is difficult to correlate across data sources & build effective alerts that don’t create many false positives. Traditional SIEM query-based rules fail to accurately identify threats. Matano's detection-as-code approach offers greater flexibility and help's you harden your detections over time.

## ❤️ Community support

For general help on using Matano, please refer to the official Matano [documentation](https://matano.dev/docs). For additional help, feel free to use one of these channels to ask a question:

- [Discord](https://discord.gg/YSYfHMbfZQ) \(Come join the Matano family, and hang out with the team and community\)
- [Forum](https://github.com/matanolabs/matano/discussions) \(For deeper conversations about features, the project, or problems\)
- [GitHub](https://github.com/matanolabs/matano) \(Bug reports, Contributions\)
- [Twitter](https://twitter.com/matanolabs) \(Get news hot off the press\)

## 👷 Contributors

Thanks go to these wonderful people ([emoji key](https://allcontributors.org/docs/en/emoji-key)):

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tbody>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/shaeqahmed"><img src="https://avatars.githubusercontent.com/u/13088492?v=4?s=100" width="100px;" alt="Shaeq Ahmed"/><br /><sub><b>Shaeq Ahmed</b></sub></a><br /><a href="#maintenance-shaeqahmed" title="Maintenance">🚧</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://www.matano.dev/"><img src="https://avatars.githubusercontent.com/u/9027301?v=4?s=100" width="100px;" alt="Samrose"/><br /><sub><b>Samrose</b></sub></a><br /><a href="#maintenance-Samrose-Ahmed" title="Maintenance">🚧</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/kai-ten"><img src="https://avatars.githubusercontent.com/u/11355908?v=4?s=100" width="100px;" alt="Kai Herrera"/><br /><sub><b>Kai Herrera</b></sub></a><br /><a href="https://github.com/matanolabs/matano/commits?author=kai-ten" title="Code">💻</a> <a href="#ideas-kai-ten" title="Ideas, Planning, & Feedback">🤔</a> <a href="#infra-kai-ten" title="Infrastructure (Hosting, Build-Tools, etc)">🚇</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/rams3sh"><img src="https://avatars.githubusercontent.com/u/5143597?v=4?s=100" width="100px;" alt="Ram"/><br /><sub><b>Ram</b></sub></a><br /><a href="https://github.com/matanolabs/matano/issues?q=author%3Arams3sh" title="Bug reports">🐛</a> <a href="#ideas-rams3sh" title="Ideas, Planning, & Feedback">🤔</a> <a href="#userTesting-rams3sh" title="User Testing">📓</a></td>
      <td align="center" valign="top" width="14.28%"><a href="http://zbmowrey.com/"><img src="https://avatars.githubusercontent.com/u/14931610?v=4?s=100" width="100px;" alt="Zach Mowrey"/><br /><sub><b>Zach Mowrey</b></sub></a><br /><a href="#ideas-zbmowrey" title="Ideas, Planning, & Feedback">🤔</a> <a href="https://github.com/matanolabs/matano/issues?q=author%3Azbmowrey" title="Bug reports">🐛</a> <a href="#userTesting-zbmowrey" title="User Testing">📓</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/marcin-kwasnicki"><img src="https://avatars.githubusercontent.com/u/91739800?v=4?s=100" width="100px;" alt="marcin-kwasnicki"/><br /><sub><b>marcin-kwasnicki</b></sub></a><br /><a href="#userTesting-marcin-kwasnicki" title="User Testing">📓</a> <a href="https://github.com/matanolabs/matano/issues?q=author%3Amarcin-kwasnicki" title="Bug reports">🐛</a> <a href="#ideas-marcin-kwasnicki" title="Ideas, Planning, & Feedback">🤔</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/gdrapp"><img src="https://avatars.githubusercontent.com/u/346463?v=4?s=100" width="100px;" alt="Greg Rapp"/><br /><sub><b>Greg Rapp</b></sub></a><br /><a href="https://github.com/matanolabs/matano/issues?q=author%3Agdrapp" title="Bug reports">🐛</a> <a href="#ideas-gdrapp" title="Ideas, Planning, & Feedback">🤔</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/niheconomoum"><img src="https://avatars.githubusercontent.com/u/22075648?v=4?s=100" width="100px;" alt="Matthew X. Economou"/><br /><sub><b>Matthew X. Economou</b></sub></a><br /><a href="https://github.com/matanolabs/matano/issues?q=author%3Aniheconomoum" title="Bug reports">🐛</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/jarretraim"><img src="https://avatars.githubusercontent.com/u/981154?v=4?s=100" width="100px;" alt="Jarret Raim"/><br /><sub><b>Jarret Raim</b></sub></a><br /><a href="https://github.com/matanolabs/matano/issues?q=author%3Ajarretraim" title="Bug reports">🐛</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://mdfranz.dev/"><img src="https://avatars.githubusercontent.com/u/47213?v=4?s=100" width="100px;" alt="Matt Franz"/><br /><sub><b>Matt Franz</b></sub></a><br /><a href="https://github.com/matanolabs/matano/issues?q=author%3Amdfranz" title="Bug reports">🐛</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://www.linkedin.com/in/francescofaenzi/"><img src="https://avatars.githubusercontent.com/u/45026063?v=4?s=100" width="100px;" alt="Francesco Faenzi"/><br /><sub><b>Francesco Faenzi</b></sub></a><br /><a href="#ideas-FrancescoFaenzi" title="Ideas, Planning, & Feedback">🤔</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://nishant.daspatnaik.com/"><img src="https://avatars.githubusercontent.com/u/1339669?v=4?s=100" width="100px;" alt="Nishant Das Patnaik"/><br /><sub><b>Nishant Das Patnaik</b></sub></a><br /><a href="#ideas-dpnishant" title="Ideas, Planning, & Feedback">🤔</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/timoguin"><img src="https://avatars.githubusercontent.com/u/671968?v=4?s=100" width="100px;" alt="Tim O'Guin"/><br /><sub><b>Tim O'Guin</b></sub></a><br /><a href="#ideas-timoguin" title="Ideas, Planning, & Feedback">🤔</a> <a href="https://github.com/matanolabs/matano/issues?q=author%3Atimoguin" title="Bug reports">🐛</a></td>
      <td align="center" valign="top" width="14.28%"><a href="https://github.com/francescor"><img src="https://avatars.githubusercontent.com/u/424577?v=4?s=100" width="100px;" alt="Francesco R."/><br /><sub><b>Francesco R.</b></sub></a><br /><a href="https://github.com/matanolabs/matano/issues?q=author%3Afrancescor" title="Bug reports">🐛</a></td>
    </tr>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="http://grue.io"><img src="https://avatars.githubusercontent.com/u/555914?v=4?s=100" width="100px;" alt="Joshua Sorenson"/><br /><sub><b>Joshua Sorenson</b></sub></a><br /><a href="https://github.com/matanolabs/matano/commits?author=grue" title="Code">💻</a> <a href="https://github.com/matanolabs/matano/commits?author=grue" title="Documentation">📖</a></td>
    </tr>
  </tbody>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

This project follows the [all-contributors](https://allcontributors.org) specification.
Contributions of any kind are welcome!

## License

- [Apache-2.0 License](LICENSE)

<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=03c989f6-90f5-4982-b002-a48635f10b5d"/>
