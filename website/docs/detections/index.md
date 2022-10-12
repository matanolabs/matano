---
title: Detections
---

To react to your security data in realtime in Matano, you work with detections. Matano lets you define *detections as code* (DaaC). A *detection* is a Python program that is invoked with data from a [log source](../log-sources/index.md) in realtime and can create an *alert*.

To create a detection, you configure the Matano table(s) that you want the detection to respond to and author Python code that processes records from the log source and, if the data matches whatever condition you express in your Python code, creates an alert that will notify you in realtime.

The topics in this section provide an overview of working with detections and alerts in Matano. They include information about creating, defining, and authoring detections and creating, configuring, and responding to alerts.
