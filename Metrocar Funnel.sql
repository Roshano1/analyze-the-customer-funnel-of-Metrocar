-- 1 How many times was the app downloaded?
SELECT COUNT (download_ts)
FROM app_downloads

-- 2 How many users signed up on the app?
SELECT COUNT(user_id)
FROM signups

-- 3 How many rides were requested through the app?
SELECT COUNT(request_ts)
FROM ride_requests

-- 4 How many rides were requested and completed through the app?
SELECT COUNT(*) AS total_rides
FROM ride_requests
WHERE request_ts IS NOT NULL AND dropoff_ts IS NOT NULL;

-- 5 How many rides were requested and how many unique users requested a ride?
SELECT COUNT(DISTINCT user_id)
FROM ride_requests

-- 6 What is the average time of a ride from pick up to drop off?
SELECT TO_CHAR(INTERVAL '1 second' * AVG(EXTRACT(EPOCH FROM (dropoff_ts - pickup_ts))), 'MI "minutes" SS "seconds"') AS average_ride_duration
FROM ride_requests
WHERE pickup_ts IS NOT NULL AND dropoff_ts IS NOT NULL;
 -- 7 How many rides were accepted by a driver?
 
 SELECT COUNT(accept_ts)
 FROM ride_requests
 
 -- 8 How many rides did we successfully collect payments and how much was collected?
 SELECT COUNT(charge_status), SUM(purchase_amount_usd) as collected
 FROM transactions
 WHERE charge_status = 'Approved'
 
 
-- 9  How many ride requests happened on each platform?

SELECT platform, COUNT(ride_id) AS ride_requests_count
FROM metrocar
GROUP BY platform;

-- 10 What is the drop-off from users signing up to users requesting a ride?
SELECT dropoff_ts, request_ts
FROM ride_requests

-- Calculate the number of users who have signed up
SELECT COUNT(DISTINCT user_id) AS users_signing_up
FROM ride_requests;

-- Calculate the number of users who have signed up and made ride requests
SELECT COUNT(DISTINCT user_id) AS users_requesting_a_ride
FROM ride_requests
WHERE request_ts IS NOT NULL;

-- 
SELECT
    (100.0 - (ride_requests_count * 100.0 / signups_count)) AS drop_off_rate
FROM
    (SELECT COUNT(DISTINCT user_id) AS signups_count FROM signups) AS signups,
    (SELECT COUNT(DISTINCT user_id) AS ride_requests_count FROM ride_requests) AS ride_requests;

 