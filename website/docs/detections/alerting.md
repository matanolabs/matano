---
title: Realtime alerting
sidebar_position: 3
---

You can use detections to implement realtime alerting on your data.

## Rule matches

When a positive detection is encountered (i.e. a detection for which your `detect` function returned `True`), a *rule match* is created.

## Alerts

Rule matches are used to create alerts.

### Deduping alerts

#### Dedupe string

You can deduplicate alerts using a *dedupe string* returned from your detection. Alerts with the same dedupe will be grouped together.

#### Deduplication window

You can use a deduplication window to add rule matches to an existing alert within a time duration. During this window, rule matches will not create new alerts but instead be appended to the existing alert for the detection (and dedupe).

You can specify a max deduplication window of **1 day**.

### Alert threshold

The alert threshold specifies how many rule matches are needed to create an alert. For example, if you set the alert threshold to 10, ten rule matches will be required within the deduplication window for an alert to be created.


## Interacting with alerts

Alerts are automatically stored in a Matano table named `matano_alerts`. The alerts and rule matches are normalized to ECS and contain context about the original event that triggered the rule match, along with the alert and rule data.
