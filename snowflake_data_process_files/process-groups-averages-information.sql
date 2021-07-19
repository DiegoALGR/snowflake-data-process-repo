CREATE TABLE "GROUPS_AVGS_INFORMATION" AS
SELECT 
AVG(RATING) AS AVG_RATING,
AVG(MEMBERS) AS AVG_MEMBERS,
COUNT(STATE) COUNT_EVENTS,
STATE, 
YEAR(MAX(CREATED)) AS LAST_EVENT_CREATED_YEAR,
MONTH(MAX(CREATED)) AS LAST_EVENT_CREATED_MONTH,
DAY(MAX(CREATED)) AS LAST_EVENT_CREATED_DAY,
CASE STATE
    WHEN 'NY' THEN 'https://www.google.com.mx/maps/search/' || 'NEW YORK'
    WHEN 'IL' THEN 'https://www.google.com.mx/maps/search/' || 'ILLINOIS'
    WHEN 'CA' THEN 'https://www.google.com.mx/maps/search/' || 'CALIFORNIA'
    WHEN 'NJ' THEN 'https://www.google.com.mx/maps/search/' || 'NEW JERSEY'
    WHEN 'AL' THEN 'https://www.google.com.mx/maps/search/' || 'ALABAMA'
    WHEN 'AZ' THEN 'https://www.google.com.mx/maps/search/' || 'ARIZONA'
    WHEN 'AR' THEN 'https://www.google.com.mx/maps/search/' || 'ARKANSAS'
    WHEN 'CO' THEN 'https://www.google.com.mx/maps/search/' || 'COLORADO'
    WHEN 'DC' THEN 'https://www.google.com.mx/maps/search/' || 'WASHINGTON'
    WHEN 'FL' THEN 'https://www.google.com.mx/maps/search/' || 'FLORIDA'
    WHEN 'GA' THEN 'https://www.google.com.mx/maps/search/' || 'GEORGIA'
    WHEN 'KY' THEN 'https://www.google.com.mx/maps/search/' || 'KENTUCKY'
    WHEN 'MI' THEN 'https://www.google.com.mx/maps/search/' || 'MICHIGAN'
END AS GROUP_STATE_VIEW_MAP
FROM "GROUPS" GROUP BY STATE ORDER BY AVG_RATING DESC;