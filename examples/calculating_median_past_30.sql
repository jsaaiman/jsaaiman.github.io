-- “Given a table with user id and post id, calculate a 30-day moving median of the post count per user on each date.”

WITH post_count AS (
    SELECT
        DATE(DATETIME) AS date,
        User AS user_id,
        COUNT(Activity) AS post_count
    FROM sql_training.posts
    GROUP BY 1, 2
),
posts_last_30 AS (
    SELECT 
        c.user_id, 
        c.date,
        (SELECT
          SUM(l.post_count)
        FROM post_count l
        WHERE l.user_id = c.user_id AND l.date BETWEEN DATE_SUB(c.date, INTERVAL 30 DAY) AND c.date
        ) AS total_past_30
    FROM 
        post_count c
    ORDER BY 
        c.user_id, 
        c.date
),
grp AS (SELECT
    user_id,
    date,
    total_past_30,
    ROW_NUMBER() OVER(PARTITION BY date ORDER BY total_past_30 ASC) AS ranking
  FROM posts_last_30
GROUP BY 1,2,3
ORDER BY date DESC
),
floor_ceil AS (SELECT 
  date,
  MAX(ranking) AS frequency,
  CASE 
    WHEN MOD(MAX(ranking),2) <> 0 THEN CEILING(MAX(ranking)/2) 
      ELSE FLOOR(MAX(ranking)/2) END AS floor,
  CASE 
    WHEN MOD(MAX(ranking),2) = 0 THEN CEILING(MAX(ranking)/2)+1
      ELSE CEILING(MAX(ranking)/2) END AS ceiling,
FROM grp
GROUP BY 1
)
SELECT 
  g.date,
  ROUND(AVG(g.total_past_30),2) AS median_posts
FROM grp g
JOIN floor_ceil fc ON fc.date = g.date
WHERE g.ranking BETWEEN fc.floor and fc.ceiling
GROUP BY 1
ORDER BY 1 DESC
