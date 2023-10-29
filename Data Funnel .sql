


 ************************************************

 --Rides and revenue
 WITH totals AS (
SELECT 
  ROUND(SUM(ts.purchase_amount_usd)) AS total_revenue, 
  COUNT(ts.ride_id) AS total_rides
FROM transactions AS ts
INNER JOIN ride_requests AS rr 
ON ts.ride_id = rr.ride_id
INNER JOIN signups AS su
ON rr.user_id = su.user_id
INNER JOIN app_downloads AS ad
ON su.session_id = ad.app_download_key
),
totals_by_platform AS (
SELECT 
  UPPER(ad.platform) AS platform,
  ROUND(SUM(ts.purchase_amount_usd)) AS revenue, 
  COUNT(ts.ride_id) AS rides
  
FROM transactions AS ts
INNER JOIN ride_requests AS rr 
ON ts.ride_id = rr.ride_id
INNER JOIN signups AS su
ON rr.user_id = su.user_id
INNER JOIN app_downloads AS ad
ON su.session_id = ad.app_download_key
GROUP BY ad.platform
ORDER BY revenue DESC
)

SELECT 
platform,
totals_by_platform.revenue,
(totals_by_platform.revenue/totals.total_revenue) AS pct_of_total_rev,
totals_by_platform.rides,
(totals_by_platform.rides::numeric/totals.total_rides::numeric) AS pct_of_total_rides
FROM totals_by_platform, totals


*******************************************

--3) Rides Funnel using 'Percent of Previous' metric

-- ride_status
WITH user_ride_status AS (
        SELECT
            ride_id,
            MAX(
                CASE
                    WHEN accept_ts IS NOT NULL
                    THEN 1
                    ELSE 0
                END
            ) AS ride_accepted,
            MAX(
                CASE
                    WHEN dropoff_ts IS NOT NULL
                    THEN 1
                    ELSE 0
                END
            ) AS ride_completed
        FROM ride_requests
        GROUP BY ride_id
    ),

-- payment_status
    payment_status AS (
        SELECT
            r.ride_id,
            COUNT(*) AS total_rides_with_payment
        FROM transactions AS t
        LEFT JOIN ride_requests AS r
        ON t.ride_id = r.ride_id
      	WHERE charge_status = 'Approved'
        GROUP BY r.ride_id
    ),

-- review_status
    review_status AS (
        SELECT
            ride_id,
            COUNT(*) AS total_reviews_per_ride
        FROM reviews
        GROUP BY ride_id
    ),

-- steps
    steps AS (
        SELECT
            1 AS funnel_step,
            'app_download' AS funnel_name,
            0 AS ride_count
        UNION
        SELECT
            2 AS funnel_step,
            'sign_up' AS funnel_name,
            0 AS ride_count
        UNION
        SELECT
            3 AS funnel_step,
            'ride_requested' AS funnel_name,
            COUNT(*) AS ride_count   --total_users_ride_requested
        FROM user_ride_status
        UNION
        SELECT
            4 AS funnel_step,
            'ride_accepted' AS funnel_name,
            SUM(ride_accepted) AS ride_count   --total_users_ride_accepted
        FROM user_ride_status
        UNION
        SELECT
            5 AS funnel_step,
            'ride_completed' AS funnel_name,
            SUM(ride_completed) AS ride_count     --total_users_ride_completed
        FROM user_ride_status
        UNION
        SELECT
            6 AS funnel_step,
            'payment' AS funnel_name,
            COUNT(*) AS ride_count
        FROM payment_status
      	UNION
        SELECT
            7 AS funnel_step,
      			'review' AS funnel_name,
            COUNT(*) AS ride_count
        FROM review_status
        )


SELECT
    funnel_step,
    funnel_name,
    ride_count,
    lag(ride_count, 1) OVER (ORDER BY funnel_step),
    (lag(ride_count, 1) OVER (ORDER BY funnel_step)) - ride_count AS diff,
    ROUND(ride_count::numeric / lag(ride_count, 1) OVER (ORDER BY funnel_step), 4) AS conversion_rate,
    ROUND((1.0 - ride_count::numeric / lag(ride_count, 1) OVER (ORDER BY funnel_step)), 4) AS dropoff_percent
FROM steps
WHERE ride_count > 0
ORDER BY funnel_step ASC
;

********************************

with user_details AS (
	SELECT app_download_key, user_id, platform, age_range, date(download_ts) AS download_dt
	FROM app_downloads
	LEFT JOIN signups
	  ON app_downloads.app_download_key = signups.session_id),
downloads AS (
	SELECT 0 as funnel_step, 
		'download' as funnel_name,
		platform, 
		age_range,
		download_dt,
		COUNT (DISTINCT app_download_key) as users_count,
		0 as count_rides
	FROM user_details
	GROUP BY platform, age_range, download_dt),

signup AS (
	SELECT 1 as funnel_step,
		'signup' as funnel_name,
		user_details.platform,
		user_details.age_range,
		user_details.download_dt,
		COUNT (DISTINCT user_id) as users_count,
		0 as count_rides
	FROM signups
	JOIN user_details
	USING (user_id)
	WHERE signup_ts is not null
	GROUP BY user_details.platform, user_details.age_range, user_details.download_dt),

requested AS (
	SELECT 2 as funnel_step,
		'ride_requested' as funnel_name,
		user_details.platform,
		user_details.age_range,
		user_details.download_dt,
		COUNT (DISTINCT user_id) as users_count,
		COUNT (DISTINCT ride_id) as count_rides
	FROM ride_requests
	JOIN user_details
	USING (user_id)
	WHERE request_ts is not null
	GROUP BY user_details.platform, user_details.age_range, user_details.download_dt),

completed AS (
	SELECT 3 as funnel_step, 
		'ride_completed' as funnel_name, 
		user_details.platform,
		user_details.age_range,
		user_details.download_dt,
		COUNT (DISTINCT user_id) as users_count,
		COUNT (DISTINCT ride_id) as count_rides
	FROM ride_requests
	JOIN user_details
	USING (user_id)
	WHERE dropoff_ts is not null
	GROUP BY user_details.platform, user_details.age_range, user_details.download_dt)

SELECT *
FROM downloads
UNION
SELECT *
FROM signup
UNION
SELECT *
FROM requested
UNION
SELECT *
FROM completed
ORDER BY funnel_step, platform, age_range, download_dt;

******************************

SELECT EXTRACT(HOUR FROM request_ts) AS time_hour,
			 COUNT(*)
FROM ride_requests
GROUP BY time_hour
LIMIT 1000;

--how many ride were requested at each hour

SELECT EXTRACT(HOUR FROM request_ts) AS time_hour,
			 COUNT(*),
       CASE WHEN cancel_ts IS NULL THEN 'accepted' ELSE 'cancelled' END AS cancelled_status
FROM ride_requests
GROUP BY time_hour, cancelled_status
LIMIT 1000;

************************************

WITH
rides_requested AS (
SELECT
  'ride requested' AS funnel,
  COUNT(DISTINCT rr.ride_id) AS rides,
  ad.platform,
  COALESCE(su.age_range, 'Not specified') AS age_group,
  CAST(rr.request_ts AS TIME) AS request_date,
  0 AS revenue
FROM
  ride_requests AS rr
  INNER JOIN signups AS su ON rr.user_id = su.user_id
  INNER JOIN app_downloads AS ad ON su.session_id = ad.app_download_key
GROUP BY
  ad.platform,
  su.age_range,
  request_date
),

rides_accepted AS (
SELECT
  'ride accepted' AS funnel,
  COUNT(DISTINCT rr.ride_id) AS rides,
  ad.platform,
  COALESCE(su.age_range, 'Not specified') AS age_group,
  CAST(rr.request_ts AS TIME) AS request_date,
  0 AS revenue
FROM
  ride_requests AS rr
  INNER JOIN signups AS su ON rr.user_id = su.user_id
  INNER JOIN app_downloads AS ad ON su.session_id = ad.app_download_key
WHERE rr.accept_ts IS NOT NULL
GROUP BY
  ad.platform,
  su.age_range,
  request_date
),

rides_completed AS (
SELECT
  'ride completed' AS funnel,
  COUNT(DISTINCT rr.ride_id) AS rides,
  ad.platform,
  COALESCE(su.age_range, 'Not specified') AS age_group,
  CAST(rr.request_ts AS TIME) AS request_date,
  0 AS revenue
FROM
  ride_requests AS rr
  INNER JOIN signups AS su ON rr.user_id = su.user_id
  INNER JOIN app_downloads AS ad ON su.session_id = ad.app_download_key
WHERE rr.accept_ts IS NOT NULL AND rr.dropoff_ts IS NOT NULL
GROUP BY
  ad.platform,
  su.age_range,
  request_date
),

rides_paid AS (
SELECT
  'ride paid' AS funnel,
  COUNT(DISTINCT rr.ride_id) AS rides,
  ad.platform,
  COALESCE(su.age_range, 'Not specified') AS age_group,
  CAST(rr.request_ts AS TIME) AS request_date,
  SUM(ts.purchase_amount_usd) AS revenue
FROM
  ride_requests AS rr
  INNER JOIN signups AS su ON rr.user_id = su.user_id
  INNER JOIN app_downloads AS ad ON su.session_id = ad.app_download_key
  INNER JOIN transactions AS ts ON rr.ride_id = ts.ride_id
WHERE rr.accept_ts IS NOT NULL AND rr.dropoff_ts IS NOT NULL AND ts.charge_status = 'Approved'
GROUP BY
  ad.platform,
  su.age_range,
  request_date
),

rides_reviewed AS (
SELECT
  'ride reviewed' AS funnel,
  COUNT(DISTINCT rr.ride_id) AS rides,
  ad.platform,
  COALESCE(su.age_range, 'Not specified') AS age_group,
  CAST(rr.request_ts AS TIME) AS request_date,
  SUM(ts.purchase_amount_usd) AS revenue
FROM
  ride_requests AS rr
  INNER JOIN signups AS su ON rr.user_id = su.user_id
  INNER JOIN app_downloads AS ad ON su.session_id = ad.app_download_key
  INNER JOIN transactions AS ts ON rr.ride_id = ts.ride_id
  INNER JOIN reviews AS rv ON ts.ride_id = rv.ride_id
WHERE rr.accept_ts IS NOT NULL AND rr.dropoff_ts IS NOT NULL AND ts.charge_status = 'Approved'
GROUP BY
  ad.platform,
  su.age_range,
  request_date)

SELECT *
FROM rides_requested
UNION ALL
SELECT *
FROM rides_accepted 
UNION ALL
SELECT *
FROM rides_completed
UNION ALL
SELECT *
FROM rides_paid 
UNION ALL
SELECT *
FROM rides_reviewed

