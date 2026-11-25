-- 03_olap.sql  -  the 7 OLAP questions (run in beeline: !run 03_olap.sql)
USE curveup_dw;

-- Q1  Message volume by programme > channel > month, with subtotals (ROLLUP)
SELECT p.program_id, c.channel, f.msg_month, SUM(f.message_count) AS messages
FROM fact_message f
JOIN dim_program p ON f.program_key = p.program_key
JOIN dim_channel c ON f.channel_key = c.channel_key
GROUP BY p.program_id, c.channel, f.msg_month WITH ROLLUP
ORDER BY program_id, channel, msg_month;

-- Q2  How often does each bot fail to understand users, by language group? (CUBE)
SELECT p.program_id, l.language_group,
       SUM(CASE WHEN i.intent = 'off_topic' THEN 1 ELSE 0 END) AS off_topic,
       COUNT(*) AS bot_replies,
       ROUND(100 * SUM(CASE WHEN i.intent = 'off_topic' THEN 1 ELSE 0 END) / COUNT(*), 1) AS off_topic_pct
FROM fact_message f
JOIN dim_program p  ON f.program_key  = p.program_key
JOIN dim_language l ON f.language_key = l.language_key
JOIN dim_intent i   ON f.intent_key   = i.intent_key
JOIN dim_sender s   ON f.sender_key   = s.sender_key
WHERE s.sender_type = 'bot' AND i.intent <> 'none'
GROUP BY p.program_id, l.language_group WITH CUBE
ORDER BY program_id, language_group;

-- Q3  Share of conversations where staff had to step in
SELECT p.program_id, COUNT(*) AS conversations,
       SUM(fc.had_staff_reply) AS with_staff_reply,
       ROUND(100 * AVG(fc.had_staff_reply), 1) AS staff_reply_pct,
       ROUND(AVG(fc.n_messages), 1) AS avg_messages
FROM fact_conversation fc JOIN dim_program p ON fc.program_key = p.program_key
GROUP BY p.program_id
ORDER BY conversations DESC;

-- Q4  Reply speed: bot vs staff, per programme (median and mean seconds)
SELECT p.program_id, s.sender_type, COUNT(f.response_time_sec) AS replies,
       ROUND(percentile_approx(f.response_time_sec, 0.5), 1) AS median_sec,
       ROUND(AVG(f.response_time_sec), 1) AS mean_sec
FROM fact_message f
JOIN dim_program p ON f.program_key = p.program_key
JOIN dim_sender s  ON f.sender_key  = s.sender_key
WHERE f.response_time_sec IS NOT NULL
GROUP BY p.program_id, s.sender_type
ORDER BY program_id, sender_type;

-- Q5  Top 3 busiest hours (UTC) for each main programme (window RANK)
SELECT * FROM (
  SELECT p.program_id, f.msg_hour_utc, COUNT(*) AS user_msgs,
         RANK() OVER (PARTITION BY p.program_id ORDER BY COUNT(*) DESC) AS rnk
  FROM fact_message f
  JOIN dim_program p ON f.program_key = p.program_key
  JOIN dim_sender s  ON f.sender_key  = s.sender_key
  WHERE s.sender_type = 'user' AND p.main_project
  GROUP BY p.program_id, f.msg_hour_utc) t
WHERE rnk <= 3 ORDER BY program_id, rnk;

-- Q6  Week-over-week change in user messages (LAG). Driven from dim_date so empty weeks show as 0
SELECT w.week_of_year, w.user_msgs,
       w.user_msgs - LAG(w.user_msgs) OVER (ORDER BY w.week_of_year) AS change_vs_prev_week
FROM (
  SELECT d.week_of_year, COUNT(f.message_id) AS user_msgs
  FROM dim_date d
  LEFT JOIN (SELECT f1.* FROM fact_message f1
             JOIN dim_sender s ON f1.sender_key = s.sender_key
             WHERE s.sender_type = 'user') f
         ON f.date_key = d.date_key
  GROUP BY d.week_of_year) w
ORDER BY week_of_year;

-- Q7  Low-confidence bot replies (< 0.6) by intent group and intent (GROUPING SETS)
SELECT i.intent_group, i.intent, COUNT(*) AS low_conf_replies
FROM fact_message f JOIN dim_intent i ON f.intent_key = i.intent_key
WHERE f.confidence < 0.6
GROUP BY i.intent_group, i.intent GROUPING SETS ((i.intent_group, i.intent), (i.intent_group), ())
ORDER BY intent_group, intent;
