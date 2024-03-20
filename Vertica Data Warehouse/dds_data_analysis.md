## DDS Analytics Queries

[DDS Schema](img\dwh_dds_data_vault_diagram.png)

[SQL Scripts](src\sql\analysis.sql)
1. [Age Distribution](#age-distribution)
2. [User Group Message Activity Analysis](#user-group-message-activity-analysis)
3. [User Addition Analysis for Early Groups](#user-addition-analysis-for-early-groups)
4. [Recent Group Conversion Analysis](#recent-group-conversion-analysis)
5. [Group Growth Over Time](#group-growth-over-time)
6. [Distribution of Messages by User](#distribution-of-messages-by-user)
7. [Correlation Between Group Size and Activity (Monthly)](#correlation-between-group-size-and-activity-monthly)

## Age Distribution

* **Purpose:** Analyze the distribution of users across different age groups for early adopters.

| Age | Count |
| --- | ----- |
| 18  | 778   |
| 19  | 789   |
| 20  | 795   |
| ... | ...   |


## User Group Message Activity Analysis

* **Purpose:** Identify early groups with the most members who actively send messages.

| hk_group_id            | cnt_users_in_group_with_messages |
| ---------------------- | -------------------------------- |
| 7279971728630971062    | 861                              |
| 7757992142189260835    | 1136                             |
| ...                    | ...                              |


## User Addition Analysis for Early Groups

* **Purpose:** Track the rate at which new users joined early groups, helping understand growth patterns.

| hk_group_id            | cnt_added_users |
| ---------------------- | --------------- |
| 7279971728630971062    | 861             |
| 7757992142189260835    | 1136            |
| ...                    | ...             |


## Recent Group Conversion Analysis

* **Purpose:** Measure how effectively early groups convert new members into active message senders.

| hk_group_id            | cnt_added_users | cnt_users_in_group_with_messages | group_conversion |
| ---------------------- | --------------- | -------------------------------- | ---------------- |
| 1594722103625592852    | 173             | 96                               | 0.55             |
| 2538717228269667607    | 614             | 335                              | 0.55             |
| ...                    | ...             | ...                              | ...              |


## Group Growth Over Time

* **Purpose:** Track how the number of groups has evolved over time, identifying periods of rapid or slow growth.

| Month     | new_groups |
|-----------|-------|
| September | 2     |
| October   | 11    |
| May       | 13    |
| April     | 23    |
| November  | 30    |
| February  | 41    |
| March     | 42    |
| December  | 44    |
| January   | 44    |

## Distribution of Messages by User

* **Purpose:** Understand if message activity is dominated by a few highly active users or distributed more evenly

| Message_count | q1 | median | q3 |
|-------|---------|---------|---------|
| 5278  | 4.0     | 2.0     | 2.0     |
| 5010  | 4.0     | 2.0     | 2.0     |
| 4586  | 4.0     | 2.0     | 2.0     |
| 4297  | 4.0     | 2.0     | 2.0     |
| 4069  | 4.0     | 2.0     | 2.0     |
| 3948  | 4.0     | 2.0     | 2.0     |
| 3858  | 4.0     | 2.0     | 2.0     |
| 3620  | 4.0     | 2.0     | 2.0     |
| ... | ... | ... | ... |

## Correlation Between Group Size and Activity (Monthly)

* **Purpose:** Investigate whether larger groups tend to have more messages sent.

| member_count | avg_messages_per_month  |
|-------|---------|
| 19    | 38.00   |
| 131   | 952.73  |
| 153   | 795.60  |
| 168   | 1152.00 |
| 202   | 1178.33 |
| 208   | 1626.18 |
| 237   | 2217.64 |
| 283   | 2883.06 |
| 328   | 3546.50 |
| 329   | 3478.00 |
| ... | ... |
