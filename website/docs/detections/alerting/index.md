---
title: Realtime alerting
---

You can use detections to implement realtime alerting on your data. When a positive detection is encountered (i.e. a detection for which your `detect` function returned `True`), a _rule match_ is created. Rule matches are used to create _alerts_.

Rule matches and alerts are delivered in realtime into a Matano table where all historical alert data is stored. You can also respond in realtime to alerts using a SNS topic.
