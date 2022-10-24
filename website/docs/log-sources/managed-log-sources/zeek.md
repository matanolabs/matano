---
title: Zeek
---

The Zeek Matano managed log source lets you ingest your Zeek logs directly into Matano.

## Usage

Use the managed log source by specifying the `managed.type` property in your `log_source` as `ZEEK`.

```yml
name: "zeek"

managed:
  type: "ZEEK"
```

Then create tables for each of the Zeek logs you want to ingest. For example, if you want to ingest Zeek DNS and HTTP logs, create table files like so:

```yml
# log_sources/zeek/tables/dns.yml
name: "dns"
```

```yml
# log_sources/zeek/tables/http.yml
name: "http"
```

For a complete reference on configuring log sources, including extending the table schema, see [_Log source configuration_](../configuration.md).

## Tables

The Zeek managed log source supports the following tables:

- capture_loss
- connection
- dce_rpc
- dhcp
- dnp3
- dns
- dpd
- files
- ftp
- http
- intel
- irc
- kerberos
- known_certs
- known_hosts
- known_services
- modbus
- mysql
- notice
- ntlm
- ntp
- ocsp
- pe
- radius
- rdp
- rfb
- signature
- sip
- smb_cmd
- smb_files
- smb_mapping
- smtp
- snmp
- socks
- software
- ssh
- ssl
- stats
- syslog
- traceroute
- tunnel
- weird
- x509

## Schema

Zeek data is normalized to ECS fields. You can view the [complete mappings][1] to see the full schemas.

[1]: https://github.com/matanolabs/matano/blob/main/data/managed/zeek/log_source.yml
