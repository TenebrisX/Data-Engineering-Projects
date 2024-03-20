## STG Analytics Queries

Below are SQL queries designed to analyze the quality and consistency of a dataset. The results are presented in Markdown tables for clarity.

[Staging Diagram](img\dwh_stg_diagram.png)

[SQL-Scripts](src\sql\staging_data_exploration.sql)
1. [Count of Records in Users, Groups, and Dialogs](#count-of-records-in-users-groups-and-dialogs)
2. [Count of Hashed Group Names in Groups](#count-of-hashed-group-names-in-groups)
3. [Count and Sum of Ages in Users](#count-and-sum-of-ages-in-users)
4. [Earliest and Latest Timestamps in Users, Groups, and Dialogs](#earliest-and-latest-timestamps-in-users-groups-and-dialogs)
5. [Checking Future and False-Start Dates](#checking-future-and-false-start-dates)
6. [Count of Group Admin IDs with No Corresponding User](#count-of-group-admin-ids-with-no-corresponding-user)
7. [Count of Missing Information in Groups and Dialogs](#count-of-missing-information-in-groups-and-dialogs)
8. [Distribution of Users by top 15 Countries](#distribution-of-users-by-top-15-countries)
9. [Average Group Size](#average-group-size)
10. [Dialog Activity Over Time (Yearly)](#dialog-activity-over-time-yearly)

### Count of Records in Users, Groups, and Dialogs

* **Purpose:** Get simple counts of total rows and unique values per dataset.

| total   | uniq    | dataset |
| ------- | ------- | ------- |
| 170000  | 170000  | users   |
| 250     | 250     | groups  |
| 1037747 | 1037747 | dialogs |


### Count of Hashed Group Names in Groups

* **Purpose:** Count unique values within the 'group_name' column to check for potential duplicates.

| count   | count   |
| ------- | ------- |
| 250     | 250     |


### Count and Sum of Ages in Users

* **Purpose:** Determine the number of users with a valid age (not null).

| count   | sum     |
| ------- | ------- |
| 170000  | 170000  |


### Earliest and Latest Timestamps in Users, Groups, and Dialogs

* **Purpose:** Find timestamps for the first and last registrations (users, groups) and the earliest/latest message.

| datestamp               | info                        |
| ----------------------- | --------------------------- |
| 2020-09-18 19:30:43.000 | earliest user registration  |
| 2021-05-24 05:11:09.000 | latest user registration    |
| 2020-09-21 09:52:10.810 | earliest group creation     |
| 2021-05-29 04:57:58.190 | latest group creation       |
| 2004-08-30 14:49:00.000 | earliest group creation (dialogs) |
| 2021-06-21 13:23:57.000 | latest group creation (dialogs)   |


### Checking Future and False-Start Dates

* **Purpose:** Ensure timestamps are within expected ranges: no future dates, and all registrations after the start date.

| no future dates | no false-start dates | dataset |
| --------------- | -------------------- | ------- |
| true            | true                 | users   |
| true            | true                 | groups  |
| true            | false                | dialogs |


### Count of Group Admin IDs with No Corresponding User

* **Purpose:** Count groups where the admin reference is missing.

| count |
| ----- |
| 0     |


### Count of Missing Information in Groups and Dialogs

* **Purpose:** Identify dialogs missing sender ('message_from') or receiver ('message_to') data.

| count | info                               |
| ----- | ---------------------------------- |
| 0     | missing group admin info           |
| 0     | missing sender info                |
| 0     | missing receiver info              |
| 1037747 | norm receiver info                |


### Distribution of Users by top 15 Countries

* **Purpose:** Get insights into the geographical makeup of the user base.

| Country         | Value |
| --------------- | ----- |
| Algeria         | 1798  |
| Guam            | 1797  |
| Honduras        | 1788  |
| El Salvador     | 1783  |
| Haiti           | 1772  |
| Finland         | 1762  |
| Ethiopia        | 1760  |
| Guinea Bissau   | 1757  |
| Brunei          | 1754  |
| Belize          | 1754  |
| Czech Republic  | 1751  |
| Denmark         | 1751  |
| Burkina Faso    | 1749  |
| Guyana          | 1749  |
| Belgium         | 1747  |


### Average Group Size

* **Purpose:** Understand how large groups tend to be.

| Average Group Size |
| ------------------- |
|        3559         |


### Dialog Activity Over Time (Yearly)

* **Purpose:** Analyze trends in message volume, potentially identifying periods of high or low engagement.

| Year | Value   |
| ---- | ------- |
| 2004 | 611     |
| 2005 | 5093    |
| 2006 | 7006    |
| 2007 | 8555    |
| 2008 | 8442    |
| 2009 | 7442    |
| 2010 | 6269    |
| 2011 | 5193    |
| 2012 | 3305    |
| 2020 | 363815  |
| 2021 | 622016  |

