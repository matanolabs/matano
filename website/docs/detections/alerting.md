---
title: Realtime alerting
sidebar_position: 3
---

You can use detections to implement realtime alerting on your data.

## Triggering alerts

When a positive detection is encountered (i.e. a detection for which your `detect` function returned `True`), an alert is created.

## Alerting delivery

Alerts are delivered to an SNS topic created in your account.

To retrieve the value of this Matano alerting topic, use the `matano info` command. See [Retrieving resource values](../getting-started.md#retrieving-resource-values).
