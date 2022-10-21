---
title: Configuring alerts
sidebar_position: 1
---

## Deduplicating alerts

Matano lets you deduplicate common alerts to reduce alert fatigue.

### Dedupe string

You can return a _dedupe string_ from your detection. Rule matches with the same dedupe will be grouped together.

To return a dedupe string from your detection, create a `dedupe` python function and return the dedupe string. The `dedupe` function will be passed the record being detected on, so you can dynamically create the dedupe string.

```python
def dedupe(record) -> str:
    ...
```

### Deduplication window

You can use a deduplication window to add rule matches to an existing alert within a time duration. During this window, rule matches will not create new alerts but instead be appended to the existing alert for the detection (and dedupe).

You can specify a max deduplication window of **1 day (86400 seconds)**.

You can configure a deduplication window per detection by using the `alert.deduplication_window` key in your `detection.yml`. Specify the value in **seconds**.

```yml
alert:
  deduplication_window: 21600
```

## Alert threshold

The alert threshold specifies how many rule matches are needed to create an alert. For example, if you set the alert threshold to 10, ten rule matches will be required within the deduplication window for an alert to be created.

You can configure an alert threshold per detection by using the `alert.threshold` key in your `detection.yml`.

```yml
alert:
  threshold: 10
```
