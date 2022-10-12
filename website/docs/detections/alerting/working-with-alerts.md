---
title: Working with alerts
sidebar_position: 2
---

## Alerts Matano table

All alerts are automatically stored in a Matano table named `matano_alerts`. The alerts and rule matches are normalized to ECS and contain context about the original event that triggered the rule match, along with the alert and rule data.

### Common queries

#### View alerts that breached threshold

```sql
select matano.alert.id as alert_id,
    count(matano.alert.rule.match.id) as rule_match_count,
    array_agg(matano.alert.rule.match.id) as rule_matches,
from matano_alerts
    where
        ts < current_timestamp - interval '1' hour
        and matano.alert.breached = true
    group by matano.alert.id, matano.alert.dedupe
```

#### Group rule matches by original data

Because the Matano schema for a rule match includes the original event that the detection ran on, you can use this information in your queries. For example, to see what actions are causing rule matches:

```sql
select
    event.action,
    count(matano.alert.rule.match.id) as rule_match_count
from matano_alerts
    where matano.alert.breached = true
    group by event.action
    order by rule_match_count desc
```
