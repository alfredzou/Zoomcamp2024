## Question 0

_This question is just a warm-up to introduce dynamic filter, please attempt it before viewing its solution._

What are the dropoff taxi zones at the latest dropoff times?

For this part, we will use the [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/).

``` sql
CREATE MATERIALIZED VIEW latest_dropoff_zone AS
SELECT taxi_zone.zone as taxi_zone, trip_data.tpep_dropoff_datetime as dropoff_datetime
FROM trip_data
LEFT JOIN taxi_zone ON trip_data.dolocationid = taxi_zone.location_id
WHERE trip_data.tpep_dropoff_datetime IN (SELECT MAX(tpep_dropoff_datetime) FROM trip_data);
```

   taxi_zone    |  dropoff_datetime   
----------------+---------------------
 Midtown Center | 2022-01-03 17:24:54

## Question 1

Create a materialized view to compute the average, min and max trip time **between each taxi zone**.

Note that we consider the do not consider `a->b` and `b->a` as the same trip pair.
So as an example, you would consider the following trip pairs as different pairs:
```plaintext
Yorkville East -> Steinway
Steinway -> Yorkville East
```

From this MV, find the pair of taxi zones with the highest average trip time.
You may need to use the [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/) for this.

Bonus (no marks): Create an MV which can identify anomalies in the data. For example, if the average trip time between two zones is 1 minute,
but the max trip time is 10 minutes and 20 minutes respectively.

Options:
1. Yorkville East, Steinway
2. Murray Hill, Midwood
3. East Flatbush/Farragut, East Harlem North
4. Midtown Center, University Heights/Morris Heights

p.s. The trip time between taxi zones does not take symmetricity into account, i.e. `A -> B` and `B -> A` are considered different trips. This applies to subsequent questions as well.

``` sql
CREATE MATERIALIZED VIEW zone_trip_times AS
WITH dif AS (
    SELECT
    pickup_zone.zone AS pickup_zone
    , dropoff_zone.zone AS dropoff_zone
    , tpep_dropoff_datetime
    , tpep_pickup_datetime
    , tpep_dropoff_datetime - tpep_pickup_datetime AS trip_duration
FROM trip_data
LEFT JOIN taxi_zone pickup_zone ON trip_data.pulocationid = pickup_zone.location_id
LEFT JOIN taxi_zone dropoff_zone ON trip_data.dolocationid = dropoff_zone.location_id
)
SELECT 
    pickup_zone
    , dropoff_zone
    , MIN(trip_duration) as min_trip_duration
    , MAX(trip_duration) as max_trip_duration
    , AVG(trip_duration) as avg_trip_duration
    , COUNT(1) as trips
FROM dif
GROUP BY 1, 2
ORDER BY AVG(trip_duration) DESC;
```

            pickup_zone             |            dropoff_zone             | min_trip_duration | max_trip_duration | avg_trip_duration | trips 
-------------------------------------+-------------------------------------+-------------------+-------------------+-------------------+-------
 Yorkville East                      | Steinway                            | 23:59:33          | 23:59:33          | 23:59:33          |     1
 Stuy Town/Peter Cooper Village      | Murray Hill-Queens                  | 23:58:44          | 23:58:44          | 23:58:44          |     1
 Washington Heights North            | Highbridge Park                     | 23:58:40          | 23:58:40          | 23:58:40          |     1
 Two Bridges/Seward Park             | Bushwick South                      | 23:58:14          | 23:58:14          | 23:58:14          |     1
 Clinton East                        | Prospect-Lefferts Gardens           | 23:53:56          | 23:53:56          | 23:53:56          |     1
 SoHo                                | South Williamsburg                  | 23:49:58          | 23:49:58          | 23:49:58          |     1
 Downtown Brooklyn/MetroTech         | Garment District                    | 23:41:43          | 23:41:43          | 23:41:43          |     1
 Lower East Side                     | Sunset Park West                    | 20:50:34          | 20:50:34          | 20:50:34          |     1

## Question 2

Recreate the MV(s) in question 1, to also find the **number of trips** for the pair of taxi zones with the highest average trip time.

Options:
1. 5
2. 3
3. 10
4. 1

## Question 3

From the latest pickup time to 17 hours before, what are the top 3 busiest zones in terms of number of pickups?
For example if the latest pickup time is 2020-01-01 17:00:00,
then the query should return the top 3 busiest zones from 2020-01-01 00:00:00 to 2020-01-01 17:00:00.

HINT: You can use [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/)
to create a filter condition based on the latest pickup time.

NOTE: For this question `17 hours` was picked to ensure we have enough data to work with.

Options:
1. Clinton East, Upper East Side North, Penn Station
2. LaGuardia Airport, Lincoln Square East, JFK Airport
3. Midtown Center, Upper East Side South, Upper East Side North
4. LaGuardia Airport, Midtown Center, Upper East Side North

``` sql
CREATE MATERIALIZED VIEW busiest_zones_17hr_window AS
WITH latest_pickup AS (
    SELECT MAX(tpep_pickup_datetime) as max_pickup_datetime FROM trip_data
)
SELECT 
    MAX(tpep_pickup_datetime) OVER () as max_pickup_datetime
    , tpep_pickup_datetime as pickup_datetime
    , MAX(tpep_pickup_datetime) OVER () - tpep_pickup_datetime as pickup_interval
    , pickup_zone.zone as pickup_zone
    , COUNT(*) OVER (PARTITION BY pickup_zone.zone) as trips
FROM trip_data
LEFT JOIN taxi_zone pickup_zone on trip_data.pulocationid = pickup_zone.location_id
WHERE tpep_pickup_datetime > (SELECT max_pickup_datetime - interval '17 hour' FROM latest_pickup)
ORDER BY trips desc;
```
max_pickup_datetime |   pickup_datetime   | pickup_interval |          pickup_zone           | trips 
---------------------+---------------------+-----------------+--------------------------------+-------
 2022-01-03 10:53:33 | 2022-01-02 17:57:22 | 16:56:11        | LaGuardia Airport              |    19
 2022-01-03 10:53:33 | 2022-01-02 17:54:50 | 16:58:43        | LaGuardia Airport              |    19
 2022-01-03 10:53:33 | 2022-01-03 10:53:33 | 00:00:00        | LaGuardia Airport              |    19
 2022-01-03 10:53:33 | 2022-01-02 17:56:46 | 16:56:47        | LaGuardia Airport              |    19
 2022-01-03 10:53:33 | 2022-01-02 17:55:14 | 16:58:19        | LaGuardia Airport              |    19
 2022-01-03 10:53:33 | 2022-01-02 17:55:09 | 16:58:24        | LaGuardia Airport              |    19
 2022-01-03 10:53:33 | 2022-01-02 17:58:17 | 16:55:16        | LaGuardia Airport              |    19
 2022-01-03 10:53:33 | 2022-01-02 17:56:29 | 16:57:04        | LaGuardia Airport              |    19
 2022-01-03 10:53:33 | 2022-01-02 17:55:15 | 16:58:18        | LaGuardia Airport              |    19
 2022-01-03 10:53:33 | 2022-01-02 19:02:55 | 15:50:38        | LaGuardia Airport              |    19
 2022-01-03 10:53:33 | 2022-01-02 17:55:15 | 16:58:18        | LaGuardia Airport              |    19
 2022-01-03 10:53:33 | 2022-01-02 17:59:08 | 16:54:25        | LaGuardia Airport              |    19
 2022-01-03 10:53:33 | 2022-01-02 17:56:28 | 16:57:05        | LaGuardia Airport              |    19
 2022-01-03 10:53:33 | 2022-01-02 17:56:04 | 16:57:29        | LaGuardia Airport              |    19
 2022-01-03 10:53:33 | 2022-01-02 17:56:26 | 16:57:07        | LaGuardia Airport              |    19
 2022-01-03 10:53:33 | 2022-01-02 17:57:31 | 16:56:02        | LaGuardia Airport              |    19
 2022-01-03 10:53:33 | 2022-01-02 17:58:43 | 16:54:50        | LaGuardia Airport              |    19
 2022-01-03 10:53:33 | 2022-01-02 17:58:15 | 16:55:18        | LaGuardia Airport              |    19
 2022-01-03 10:53:33 | 2022-01-02 17:58:38 | 16:54:55        | LaGuardia Airport              |    19
 2022-01-03 10:53:33 | 2022-01-02 17:54:27 | 16:59:06        | Lincoln Square East            |    17
 2022-01-03 10:53:33 | 2022-01-02 17:56:51 | 16:56:42        | Lincoln Square East            |    17
 2022-01-03 10:53:33 | 2022-01-02 17:57:24 | 16:56:09        | JFK Airport                    |    17
 2022-01-03 10:53:33 | 2022-01-02 17:56:43 | 16:56:50        | JFK Airport                    |    17