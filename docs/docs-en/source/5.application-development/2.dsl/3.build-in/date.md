# Date

GeaFlow support the following date function:
* [from_unixtime](#from_unixtime)
* [from_unixtime_millis](#from_unixtime_millis)
* [unix_timestamp](#unix_timestamp)
* [unix_timestamp_millis](#unix_timestamp_millis)
* [isdate](#isdate)
* [now](#now)
* [day](#day)
* [weekday](#weekday)
* [lastday](#lastday)
* [day_of_month](#day_of_month)
* [week_of_year](#week_of_year)
* [quarter](#quarter)
* [day_of_year](#day_of_year)
* [date_add](#date_add)
* [date_sub](#date_sub)
* [date_diff](#date_diff)
* [add_months](#add_months)
* [date_format](#date_format)
* [date_part](#date_part)
* [date_trunc](#date_trunc)

## from_unixtime
**Syntax**

```sql
string from_unixtime(int unixtime)
string from_unixtime(long unixtime)
string from_unixtime(int unixtime, string format)
string from_unixtime(long unixtime, string format)
string from_unixtime(string unixtime, string format)
```
**Description**
Translate unix timestamp to the format time. The default format is "yyyy-MM-dd HH:mm:ss". Return Null if any of the input is null.

**Example**

```sql
from_unixtime(11111111) = '1970-05-09 22:25:11'
from_unixtime(11111111, 'yyyy-MM-dd HH:mm:ss.SSSSSS') = '1970-05-09 22:25:11.000000'
```

## from_unixtime_millis
**Syntax**

```sql
string from_unixtime_millis(int unixtime)
string from_unixtime_millis(long unixtime)
string from_unixtime_millis(string unixtime)
string from_unixtime_millis(int unixtime, string format)
string from_unixtime_millis(long unixtime, string format)
```
**Description**
Translate unix millis timestamp to the format time. The default format is "yyyy-MM-dd HH:mm:ss.SSS". Return Null if any of the input is null.
**Example**

```sql
from_unixtime_millis(11111111) = '1970-01-01 11:05:11.111'
from_unixtime_millis(11111111, 'yyyy-MM-dd HH:mm:ss') = '1970-01-01 11:05:11'
from_unixtime_millis(11111111, 'yyyy-MM-dd HH:mm:ss.SSSSSS') = '1970-01-01 11:05:11.111000'
```

## unix_timestamp
**Syntax**

```sql
long unix_timestamp()
long unix_timestamp(string dateText)
long unix_timestamp(string dateText, string patternText)
```
**Description**
Return Unix timestamp. If no argument is provided, return the current timestamp. If dateText is given, return the corresponding timestamp. When the format patternText is not specified, parse dateText using the default formats "yyyy-MM-dd", "yyyy-MM-dd HH:mm:ss" or "yyyy-MM-dd HH:mm:ss.SSSSSS". Return Null if any of the input is null.

**Example**

```sql
unix_timestamp('1987-06-05 00:11:22') = 549817882
unix_timestamp('1987-06-05 00:11', 'yyyy-MM-dd HH:mm') = 549817860
```

## unix_timestamp_millis
**Syntax**

```sql
long unix_timestamp_millis()
long unix_timestamp_millis(string dateText)
long unix_timestamp_millis(string dateText, string patternText)
```
**Description**
Return Unix millis timestamp. Similar to the function **unix_timestamp**.
**Example**

```sql
unix_timestamp_millis('1987-06-05 00:11:22') = 549817882000
unix_timestamp_millis('1987-06-05', 'yyyy-mm-dd') = 536774760000
```

## isdate
**Syntax**

```sql
boolean isdate(string date)
boolean isdate(string date, string format)
```
**Description**
Returns whether string is a date of the format. The default format is "yyyy-MM-dd HH:mm:ss". Return false if any of the input is null.

**Example**

```sql
isdate('1987-06-05 00:11:22') = true
isdate('xxxxxxxxxxxxx') = false
isdate('1987-06-05 00:11:22', 'yyyy-MM-dd HH:mm:ss.SSSSSS') = false
```

## now
**Syntax**

```sql
long now()
long now(int offset)
long now(long offset)
```
**Description**
Returns the current timestamp with an optional offset.

**Example**

```sql
now()
now(1000)
```

## day
**Syntax**

```sql
int day(string dateString)
```
**Description**
Returns the day of date. The default format is "yyyy-MM-dd" or "yyyy-MM-dd HH:mm:ss". Return null if any of the input is null.

**Example**

```sql
day('1987-06-05 00:11:22') = 5
```

## weekday
**Syntax**

```sql
int weekday(string dateString)
```
**Description**
Returns the weekday of date. The default format is "yyyy-MM-dd" or "yyyy-MM-dd HH:mm:ss". Return null if any of the input is null.

**Example**

```sql
weekday('1987-06-05 00:11:22') = 5
```

## lastday
**Syntax**

```sql
string lastday(string dateString)
```
**Description**
Returns the last day of the month which the date belongs to. The default format is "yyyy-MM-dd" or "yyyy-MM-dd HH:mm:ss". Return null if any of the input is null.

**Example**

```sql
lastday('1987-06-05') = '1987-06-30 00:00:00'
```

## day_of_month
**Syntax**

```sql
int day_of_month(string dateString)
```
**Description**
Returns the date of the month of date. The default format is "yyyy-MM-dd" or "yyyy-MM-dd HH:mm:ss". Return null if any of the input is null.

**Example**

```sql
day_of_month('1987-06-05 00:11:22') = 5
```

## week_of_year
**Syntax**

```sql
int week_of_year(string dateString)
```
**Description**
Returns the week of the year of the given date. The default format is "yyyy-MM-dd" or "yyyy-MM-dd HH:mm:ss". Return null if any of the input is null.

**Example**

```sql
week_of_year('1987-06-05 00:11:22') = 23
```
## quarter
**Syntax**

```sql
int quarter(string dateString)
```

**Description**
Returns the quarter of the year of the given date (1-4). The default format is "yyyy-MM-dd" or "yyyy-MM-dd HH:mm:ss". Returns null if the input is null.

**Example**
```sql
quarter('1987-06-05 00:11:22') = 2
```
## day_of_year

**Syntax**

```sql
int day_of_year(string dateString)
```
**Description**
Returns the day of the year of the given date (1-366). The default format is "yyyy-MM-dd" or "yyyy-MM-dd HH:mm:ss". Returns null if the input is null.

**Example**
```sql
day_of_year('1987-06-05 00:11:22') = 156
```

## date_add
**Syntax**

```sql
string date_add(string date, int days)
```
**Description**
Add a number of days to the specified date. The default format is "yyyy-MM-dd" or "yyyy-MM-dd HH:mm:ss". Return Null if any of the input is null.

**Example**

```sql
date_add('2017-09-25 10:00:00', 1) = '2017-09-26'
date_add('2017-09-25', 1) = '2017-09-26'
date_add('2017-09-25', -1) = '2017-09-24'
```

## date_sub
**Syntax**

```sql
string date_sub(string date, int days)
```
**Description**
Sub a number of days to the specified date. The default format is "yyyy-MM-dd" or "yyyy-MM-dd HH:mm:ss". Return Null if any of the input is null.

**Example**

```sql
date_sub('2017-09-25 10:00:00', 1) = '2017-09-24'
date_sub('2017-09-25', 1) = '2017-09-24'
date_sub('2017-09-25', -1) = '2017-09-26'
```

## date_diff
**Syntax**

```sql
int date_diff(string dateString1, string dateString2)
```
**Description**
Returns the number of days from dateString2 to dateString1. The default format is "yyyy-MM-dd" or "yyyy-MM-dd HH:mm:ss". Return Null if any of the input is null.

**Example**

```sql
date_diff('2017-09-26', '2017-09-25') = 1
date_diff('2017-09-24', '2017-09-25') = -1
```

## add_months
**Syntax**

```sql
string add_months(string date, int months)
```
**Description**
Add a number of months to the specified date. The default format is "yyyy-MM-dd" or "yyyy-MM-dd HH:mm:ss". Return Null if any of the input is null.

**Example**

```sql
add_months('2017-09-25 10:00:00', 1) = '2017-10-25 10:00:00'
add_months('2017-09-25', 1) = '2017-10-25'
add_months('2017-09-25', -1) = '2017-08-25'
```

## date_format
**Syntax**

```sql
string date_format(string dateText)
string date_format(string dateText, string toFormat)
string date_format(string dateText, string fromFormat, string toFormat)
```
**Description**
Returns convert the date from a format to another. The default **fromFormat** is "yyyy-MM-dd HH:mm:ss" or "yyyy-MM-dd HH:mm:ss.SSSSSS", and the default **toFormat** is "yyyy-MM-dd HH:mm:ss". Return Null if any of the input is null.

**Example**

```sql
date_format('1987-06-05 00:11:22') = '1987-06-05 00:11:22'
date_format('1987-06-05 00:11:22', 'MM-dd-yyyy') = '06-05-1987'
date_format('00:11:22 1987-06-05', 'HH:mm:ss yyyy-MM-dd', 'MM-dd-yyyy') = '06-05-1987'
```

## date_part
**Syntax**

```sql
int date_part(string dateText, string datePart)
```
**Description**
Returns part of the date by date part format. The default date format is "yyyy-MM-dd HH:mm:ss" or "yyyy-MM-dd". Return Null if any of the input is null. The datePart format is as shown in the table below.

| Select | datePart |
| -------- | -------- |
| YEAR     | yyyy, year     |
| MONTH     | mm, mon, month     |
| DAY_OF_MONTH     | dd, day     |
| HOUR_OF_DAY     | hh, hour     |
| MINUTE     | mi, minute     |
| SECOND     | ss, second     |

**Example**

```sql
date_part('1987-06-05 00:11:22', 'yyyy') = 1987
date_part('1987-06-05 00:11:22', 'mm') = 6
date_part('1987-06-05 00:11:22', 'dd') = 5
date_part('1987-06-05 00:11:22', 'hh') = 0
date_part('1987-06-05 00:11:22', 'mi') = 11
date_part('1987-06-05 00:11:22', 'ss') = 22
date_part('1987-06-05', 'ss') = 0
```

## date_trunc
**Syntax**

```sql
string date_trunc(string dateText, string datePart)
```
**Description**
Returns date truncated to the unit specified by the format. The default date format is "yyyy-MM-dd HH:mm:ss" or "yyyy-MM-dd". Return Null if any of the input is null. The datePart format is the same as the function **date_part**.

**Example**

```sql
date_trunc('1987-06-05 00:11:22', 'yyyy') = '1987-01-01 00:00:00'
date_trunc('1987-06-05 00:11:22', 'mm') = '1987-06-01 00:00:00'
date_trunc('1987-06-05 00:11:22', 'dd') = '1987-06-05 00:00:00'
date_trunc('1987-06-05 00:11:22', 'hh') = '1987-06-05 00:00:00'
date_trunc('1987-06-05 00:11:22', 'mi') = '1987-06-05 00:11:00'
date_trunc('1987-06-05 00:11:22', 'ss') = '1987-06-05 00:11:22'
date_trunc('1987-06-05', 'ss') = '1987-01-01 00:00:00'
```