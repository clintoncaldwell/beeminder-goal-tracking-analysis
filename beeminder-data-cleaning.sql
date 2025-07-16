-- Beeminder 2019 Data Cleaning

/* 	Beeminder is a goal-tracking app where users set their own measurable goals 
	and face financial consequences if they fail to meet their commitments. */

-- ----------------------------------------------------------------
-- 1. GOALS
-- ----------------------------------------------------------------

-- 1a. Creating a staging table for goals data
CREATE TABLE stg_goals (
    user_id VARCHAR(255),
    has_credit_card VARCHAR(255),
    premium_plan VARCHAR(255),
    goal_id VARCHAR(255),
    created_at VARCHAR(255),
    end_state VARCHAR(50),
    end_date VARCHAR(255),
    is_archived VARCHAR(50),
    goal_type VARCHAR(50),
    slope FLOAT,
    target_date VARCHAR(255),
    respite_days INT,
    has_no_excuses VARCHAR(50),
    autodata_source VARCHAR(255),
    updated_at VARCHAR(255),
    data_source VARCHAR(50) -- Identifies data origin (main, strava, fitbit)
);


-- 1b. Join data from multiple sources
INSERT INTO stg_goals
SELECT *, 'main' AS data_source FROM main_goals
UNION ALL
SELECT *, 'strava' AS data_source FROM strava_goals
UNION ALL
SELECT *, 'fitbit' AS data_source FROM fitbit_goals;

    
-- 1c. Check for Duplicates
-- Note: Multiple entries with the same [user_id, goal_id] are versioned records, useful for revenue calculation
-- goal_id is only unique per user_id, and not unique globally. Composite Key = [user_id, goal_id, created_at] 
WITH duplicate_cte AS (
	SELECT 
		user_id, 
		goal_id,
		created_at,	
		ROW_NUMBER() OVER (
			PARTITION BY user_id, goal_id, created_at 
			ORDER BY user_id, goal_id, created_at DESC
		) AS row_num
	FROM stg_goals
)
SELECT 
	user_id, 
	goal_id,
	created_at,
	row_num
FROM duplicate_cte
WHERE row_num > 1
ORDER BY 
	user_id, 
	goal_id, 
	created_at DESC, 
	row_num;


-- Check that each user's premium plan is consistent across all user's entries
SELECT i.user_id, i.premium_plan, j.premium_plan
FROM stg_goals i
INNER JOIN stg_goals j
	ON i.user_id = j.user_id
	AND i.premium_plan != j.premium_plan
	

-- 1d. Standardize the Data

-- Boolean columns data type conversion
UPDATE stg_goals
SET 
	has_credit_card = CASE
		WHEN has_credit_card = 'TRUE' THEN TRUE
		ELSE FALSE 
	END,
	is_archived = CASE
		WHEN is_archived = 'TRUE' THEN TRUE
		ELSE FALSE 
	END, 
	has_no_excuses = CASE
		WHEN has_no_excuses = 'TRUE' THEN TRUE
		ELSE FALSE 
	END,
	end_state = CASE
		WHEN end_state = 'lost' THEN 0
		WHEN end_state = 'won' THEN 1
		ELSE 2 -- ongoing 
	END;


ALTER TABLE stg_goals
RENAME COLUMN end_state TO completion_status;


ALTER TABLE stg_goals
MODIFY COLUMN has_credit_card BOOL,
MODIFY COLUMN is_archived BOOL,
MODIFY COLUMN has_no_excuses BOOL
MODIFY COLUMN completion_status INT;


-- Date Standardization (to UTC)
-- created_at column
UPDATE stg_goals
SET 
	created_at = CONVERT_TZ(
				STR_TO_DATE(LEFT(created_at, LENGTH(created_at) - 6), '%Y-%m-%dT%H:%i:%s'),
		        RIGHT(created_at, 6), 
		        '+00:00');
	

-- end_date column
UPDATE stg_goals
SET 
	end_date = CASE
		WHEN end_date = '' OR end_date IS NULL THEN NULL
		ELSE CONVERT_TZ(
			STR_TO_DATE(LEFT(end_date, 
				CASE 
		        	WHEN LENGTH(end_date) = 25 THEN LENGTH(end_date) - 6
	        		ELSE LENGTH(end_date) - 10
	        	END),
				'%Y-%m-%dT%H:%i:%s'
			),
		    RIGHT(end_date, 6), 
		    '+00:00'
	    )
	END;


-- target_date column
UPDATE stg_goals
SET 
	target_date = CONVERT_TZ(
		STR_TO_DATE(LEFT(target_date, LENGTH(target_date) - 6), '%Y-%m-%dT%H:%i:%s'),
        RIGHT(target_date, 6), 
        '+00:00');


-- updated_at column
UPDATE stg_goals
SET 
	updated_at = CONVERT_TZ(
		STR_TO_DATE(LEFT(updated_at, LENGTH(updated_at) - 6), '%Y-%m-%dT%H:%i:%s'),
        RIGHT(updated_at, 6), 
        '+00:00');


-- Datetime Type Conversion
ALTER TABLE stg_goals
MODIFY COLUMN created_at DATETIME,
MODIFY COLUMN end_date DATETIME,
MODIFY COLUMN target_date DATETIME,
MODIFY COLUMN updated_at DATETIME;


-- 1e. Null Values or Blank Values 
UPDATE stg_goals
SET
	premium_plan = CASE
		WHEN premium_plan = '' THEN NULL
		ELSE premium_plan
	END;
	

UPDATE stg_goals 
SET 
	autodata_source = CASE
		WHEN autodata_source = '' THEN NULL
		ELSE autodata_source
	END;
	


-- ----------------------------------------------------------------
-- 2. DATAPOINTS
-- ----------------------------------------------------------------

-- 2a. Creating a staging table for datapoints data
CREATE TABLE stg_datapoints (
    user_id VARCHAR(64),
    goal_id VARCHAR(64),
    created_at VARCHAR(50),
    logged_at VARCHAR(50),
    value FLOAT,
    entry_date VARCHAR(50),
    data_source VARCHAR(50) -- Identifies data origin (main, strava, fitbit)
);


-- 2b. Join data from multiple sources
INSERT INTO stg_datapoints
SELECT *, 'main' AS data_source FROM main_datapoints
UNION ALL
SELECT *, 'strava' AS data_source FROM strava_datapoints
UNION ALL
SELECT *, 'fitbit' AS data_source FROM fitbit_datapoints;


-- 2c. Check for Duplicates
-- logged_at column can have duplicates if the datapoints are recorded simultaneously
WITH duplicate_cte AS (
	SELECT 
		user_id, 
		goal_id,
		created_at,	
		logged_at,
		value,
		entry_date,
		ROW_NUMBER() OVER (
			PARTITION BY user_id, goal_id, created_at, logged_at, value, entry_date, data_source 
			ORDER BY user_id, goal_id, created_at DESC
		) AS row_num
	FROM stg_datapoints
)
SELECT 
	user_id, 
	goal_id,
	created_at,
	logged_at,
	value,
	entry_date,
	row_num
FROM duplicate_cte
WHERE row_num > 1
ORDER BY 
	user_id, 
	goal_id, 
	created_at DESC, 
	row_num;


-- 2d. Standardize the Data

-- Date Standardization 
-- created_at column (convert to UTC)
UPDATE stg_datapoints
SET 
	created_at = CONVERT_TZ(
		STR_TO_DATE(LEFT(created_at, LENGTH(created_at) - 6), '%Y-%m-%dT%H:%i:%s'),
	    RIGHT(created_at, 6), 
	    '+00:00');


-- logged_at column (already in UTC)
UPDATE stg_datapoints
SET 
	logged_at = CASE
		WHEN logged_at = '' THEN NULL
		ELSE REPLACE(LEFT(logged_at, LENGTH(logged_at)-1), 'T', ' ')
	END;


-- entry_date column
UPDATE stg_datapoints
SET 
	entry_date = CASE
		WHEN entry_date = '' THEN NULL
		ELSE entry_date
	END;


-- Column Data Type Conversion
ALTER TABLE stg_datapoints
MODIFY COLUMN created_at DATETIME,
MODIFY COLUMN logged_at DATETIME,
MODIFY COLUMN entry_date DATE;



-- ----------------------------------------------------------------
-- 3. PLEDGES
-- ----------------------------------------------------------------
-- 3a. Creating a staging table for pledges data
CREATE TABLE stg_pledges (
    user_id VARCHAR(64),
    goal_id VARCHAR(64),
    created_at VARCHAR(50),
    logged_at VARCHAR(50),
    new_amount VARCHAR(50),
    event VARCHAR(50),
    data_source VARCHAR(50) -- Identifies data origin (main, strava, fitbit)
);


-- 3b. Join data from multiple sources
INSERT INTO stg_pledges
SELECT *, 'main' AS data_source FROM main_pledges
UNION ALL
SELECT *, 'strava' AS data_source FROM strava_pledges
UNION ALL
SELECT *, 'fitbit' AS data_source FROM fitbit_pledges;


-- 3c. Check for Duplicates
WITH duplicate_cte AS (
	SELECT 
		user_id, 
		goal_id,
		created_at,	
		logged_at,
		value,
		entry_date,
		ROW_NUMBER() OVER (
			PARTITION BY user_id, goal_id, created_at, logged_at, value, entry_date, data_source 
			ORDER BY user_id, goal_id, created_at DESC
		) AS row_num
	FROM stg_datapoints
)
SELECT 
	user_id, 
	goal_id,
	created_at,
	logged_at,
	value,
	entry_date,
	row_num
FROM duplicate_cte
WHERE row_num > 1
ORDER BY 
	user_id, 
	goal_id, 
	created_at DESC, 
	row_num;


-- 3d. Standardize the Data

-- Date Standardization 
-- created_at column (convert to UTC)
UPDATE stg_pledges
SET 
	created_at = CONVERT_TZ(
		STR_TO_DATE(LEFT(created_at, LENGTH(created_at) - 6), '%Y-%m-%dT%H:%i:%s'),
	    RIGHT(created_at, 6), 
	    '+00:00');


-- logged_at column (from unix timestamp)
UPDATE stg_pledges
SET
	logged_at = CASE
		WHEN logged_at = '-' THEN NULL
		ELSE FROM_UNIXTIME(logged_at)
	END;
	

UPDATE stg_pledges
SET
	logged_at = CASE
		WHEN logged_at IS NOT NULL THEN LEFT(logged_at, LENGTH(logged_at) - 7)
		ELSE logged_at
	END;


-- 3e. Null Values or Blank Values
-- new_amount column
UPDATE stg_pledges
SET new_amount = NULL
WHERE new_amount = '-';


-- event column
UPDATE stg_pledges
SET event = NULL
WHERE event = '-';


-- Column Data Type Conversion
ALTER TABLE stg_pledges
MODIFY COLUMN created_at DATETIME,
MODIFY COLUMN logged_at DATETIME,
MODIFY COLUMN new_amount INT;



-- ----------------------------------------------------------------
-- 4. AUTODATA - FITBIT
-- ----------------------------------------------------------------
-- 4a. Creating a staging table for fitbit metrics data
CREATE TABLE stg_fitbit_autodata (
    user_id VARCHAR(64),
    goal_id VARCHAR(64),
    created_at VARCHAR(50),
    metrics VARCHAR(50)
);


-- 4b. Insert data into staging table
INSERT INTO stg_fitbit_autodata
SELECT 
	user,
	slug,
	createdat,
	fitbit_field
FROM fitbit_autodata;


-- 4c. Check for Duplicates
-- No duplicates
WITH duplicate_cte AS (
	SELECT 
		user_id, 
		goal_id,
		created_at,
		ROW_NUMBER() OVER (
			PARTITION BY user_id, goal_id, created_at 
			ORDER BY user_id, goal_id, created_at DESC
		) AS row_num
	FROM stg_fitbit_autodata
)
SELECT 
	user_id, 
	goal_id,
	created_at,
	row_num
FROM duplicate_cte
WHERE row_num > 1
ORDER BY 
	user_id, 
	goal_id, 
	created_at DESC, 
	row_num;


-- 4d. Standardize the Data

-- Date Standardization 
-- created_at column (convert to UTC)
UPDATE stg_fitbit_autodata
SET 
	created_at = CONVERT_TZ(
		STR_TO_DATE(LEFT(created_at, LENGTH(created_at) - 6), '%Y-%m-%dT%H:%i:%s'),
	    RIGHT(created_at, 6), 
	    '+00:00');


ALTER TABLE stg_fitbit_autodata 
MODIFY COLUMN created_at DATETIME;


-- ----------------------------------------------------------------
-- 5. AUTODATA - STRAVA
-- ----------------------------------------------------------------
-- 5a. Creating a staging table for strava metrics data
CREATE TABLE stg_strava_autodata (
    user_id VARCHAR(64),
    goal_id VARCHAR(64),
    created_at VARCHAR(50),
    metrics VARCHAR(50),
    activity_type VARCHAR(50),
    activity_group VARCHAR(50)
);


-- 5b. Insert data into staging table
INSERT INTO stg_strava_autodata
SELECT 
	user,
	slug,
	createdat,
	metric,
	activityTypes,
	NULL
FROM strava_autodata;


-- 5c. Check for Duplicates
-- No duplicates
WITH duplicate_cte AS (
	SELECT 
		user_id, 
		goal_id,
		created_at,
		activity_type,
		ROW_NUMBER() OVER (
			PARTITION BY user_id, goal_id, created_at, activity_type 
			ORDER BY user_id, goal_id, created_at DESC
		) AS row_num
	FROM stg_strava_autodata
)
SELECT 
	user_id, 
	goal_id,
	created_at,
	activity_type,
	row_num
FROM duplicate_cte
WHERE row_num > 1
ORDER BY 
	user_id, 
	goal_id, 
	created_at DESC, 
	row_num;


-- 5d. Standardize the Data

-- Date Standardization 
-- created_at column (convert to UTC)
UPDATE stg_strava_autodata
SET 
	created_at = CONVERT_TZ(
		STR_TO_DATE(LEFT(created_at, LENGTH(created_at) - 6), '%Y-%m-%dT%H:%i:%s'),
	    RIGHT(created_at, 6), 
	    '+00:00');


-- changing from camel case to snake case for activity_type column
UPDATE stg_strava_autodata
SET
	activity_type = LOWER(
        	REGEXP_REPLACE(activity_type, '([a-z])([A-Z])', '$1_$2', 1, 0, 'c')
		);
	

-- setting values into the right category
UPDATE stg_strava_autodata
SET metrics = 'weighted_duration'
WHERE metrics = 'weighted_duration2';


-- 5e. Null Values or Blank Values

-- Creating groups for each activity
UPDATE stg_strava_autodata
SET 
	activity_group = CASE
        -- Foot Sports
        WHEN activity_type IN ('run', 'hike', 'walk') THEN 'foot_sports'
        -- Cycle Sports
        WHEN activity_type IN ('ride', 'virtual_ride') THEN 'cycle_sports'
        -- Water Sports
        WHEN activity_type IN ('swim', 'canoeing', 'stand_up_paddling', 'surfing', 'rowing', 'windsurf', 'kayaking', 'kitesurf') THEN 'water_sports'
        -- Winter Sports
        WHEN activity_type IN ('ice_skate', 'alpine_ski', 'backcountry_ski', 'nordic_ski', 'snowboard', 'snowshoe') THEN 'winter_sports'
        -- Adaptive Sports
        WHEN activity_type IN ('wheel_chair', 'handcycle') THEN 'adaptive_sports'
        -- Fitness Training
        WHEN activity_type IN ('crossfit', 'elliptical', 'stair_stepper', 'weight_training', 'workout', 'yoga') THEN 'fitness_training'
        -- Other
        WHEN activity_type IN ('inline_skate', 'rock_climbing') THEN 'other'
	END;
		

	
-- ----------------------------------------------------------------
-- 6. SLOPE HISTORY
-- ----------------------------------------------------------------
-- 6a. Creating a staging table for goal slope history
CREATE TABLE stg_slope_history (
    user_id VARCHAR(64),
    goal_id VARCHAR(64),
    created_at VARCHAR(50),
    end_date VARCHAR(50),
    value VARCHAR(50),
    rate VARCHAR(50),
    data_source VARCHAR(50) -- Identifies data origin (main, strava, fitbit)
);


-- 6b. Join data from multiple sources
INSERT INTO stg_slope_history
SELECT *, 'main' AS data_source FROM main_slope_history
UNION ALL
SELECT *, 'strava' AS data_source FROM strava_slope_history
UNION ALL
SELECT *, 'fitbit' AS data_source FROM fitbit_slope_history;


-- 6c. Check for Duplicates
WITH duplicate_cte AS (
	SELECT 
		*,	
		ROW_NUMBER() OVER (
			PARTITION BY user_id, goal_id, created_at, end_date, value, rate, data_source
			ORDER BY user_id, goal_id, created_at DESC
		) AS row_num
	FROM stg_slope_history
)
SELECT 
	*,
	row_num
FROM duplicate_cte
WHERE row_num > 1
ORDER BY 
	user_id, 
	goal_id, 
	created_at DESC, 
	row_num;


-- 6d. Standardize the Data

-- Date Standardization 
-- created_at column (convert to UTC)
UPDATE stg_slope_history
SET 
	created_at = CONVERT_TZ(
		STR_TO_DATE(LEFT(created_at, LENGTH(created_at) - 6), '%Y-%m-%dT%H:%i:%s'),
	    RIGHT(created_at, 6), 
	    '+00:00');


-- end_date column (from unix timestamp)
UPDATE stg_slope_history
SET
	end_date = CASE
		WHEN end_date = '-' OR end_date = '61200' THEN NULL
		ELSE FROM_UNIXTIME(end_date)
	END;


UPDATE stg_slope_history
SET
	end_date = SUBSTR(
		end_date, 1, LENGTH(end_date) - 7);


-- 6e. Null Values or Blank Values

-- value column
UPDATE stg_slope_history
SET value = NULL
WHERE value = '-';


-- rate column
UPDATE stg_slope_history
SET rate = NULL
WHERE rate = '-';


-- Column Data Type Conversion
ALTER TABLE stg_slope_history
MODIFY COLUMN created_at DATETIME,
MODIFY COLUMN end_date DATETIME,
MODIFY COLUMN value FLOAT,
MODIFY COLUMN rate FLOAT;



-- ----------------------------------------------------------------
-- NORMALIZE DATA (3NF)
-- ----------------------------------------------------------------

-- GOALS -------------------------------------------------------------------

-- create goals table
CREATE TABLE goals LIKE stg_goals;

INSERT INTO goals SELECT * FROM stg_goals;


-- create users table
CREATE TABLE users AS
SELECT user_id, has_credit_card, premium_plan 
FROM stg_goals;


-- create premium_plans table
CREATE TABLE premium_plans (
	id INT AUTO_INCREMENT,
	type VARCHAR(50),
	monthly_fee DECIMAL(10,2),
	PRIMARY KEY (id)
);

INSERT INTO premium_plans(type, monthly_fee) VALUES
	('beelite', 8.00),
	('planbee', 16.00),
	('infinibee', 8.00),
	('beeplus', 16.00),
	('beemium', 32.00);


-- convert users' premium_plan column entries to id numbers using premium_plans table
UPDATE users u
LEFT JOIN premium_plans p 
	ON u.premium_plan = p.type
SET u.premium_plan = p.id;


-- rename and change data type of column to reflect changing to id's
ALTER TABLE users
CHANGE premium_plan premium_plan_id INT;


-- rename user_id to id
ALTER TABLE users
RENAME COLUMN user_id TO id;


-- remove duplicate id entries
ALTER TABLE users
ADD COLUMN temp_id INT AUTO_INCREMENT PRIMARY KEY;


WITH duplicate_users AS (
  	SELECT *,
		ROW_NUMBER() OVER (PARTITION BY id ORDER BY temp_id) AS r
  	FROM users
)
DELETE FROM users
WHERE temp_id IN (
	SELECT temp_id
	FROM duplicate_users
	WHERE r > 1
);


ALTER TABLE users
DROP COLUMN temp_id;


-- assign users table's primary and foreign key
ALTER TABLE users
ADD PRIMARY KEY (id),
ADD CONSTRAINT fk_users_premium_plans_id
FOREIGN KEY (premium_plan_id)
REFERENCES premium_plans(id);


-- drop original columns from the goal's table 
ALTER TABLE goals
DROP COLUMN has_credit_card, 
DROP COLUMN premium_plan;


-- goals table: assign composite primary key and foreign key
ALTER TABLE goals
ADD PRIMARY KEY (user_id, goal_id, created_at),
ADD CONSTRAINT fk_goals_users_id
FOREIGN KEY (user_id)
REFERENCES users(id);


-- increment completion status to become an id number starting at 1
UPDATE goals
SET completion_status = completion_status + 1;


-- rename goals table's completion_status column to reflect changing to id's
ALTER TABLE goals
CHANGE completion_status status_id INT;


-- create completion_status table
CREATE TABLE completion_status(
	id INT,
	type VARCHAR(50),
	PRIMARY KEY (id)
);


INSERT INTO completion_status(id, type) VALUES
	(1, 'lost'),
	(2, 'won'),
	(3, 'ongoing');


-- assign goals table's foreign key to completion_status table
ALTER TABLE goals
ADD CONSTRAINT fk_goals_completion_status_id
FOREIGN KEY (status_id)
REFERENCES completion_status(id);


-- create goal_types table
CREATE TABLE goal_types(
	id INT AUTO_INCREMENT,
	name VARCHAR(50),
	PRIMARY KEY (id)
);


INSERT INTO goal_types(name) VALUES
	('hustler'),
	('fatloser'),
	('biker'),
	('drinker'),
	('inboxer'),
	('gainer'),
	('netcalorie'),
	('custom');


-- change goals table's goal_type entries to id numbers
UPDATE goals g
LEFT JOIN goal_types t
	ON g.goal_type = t.name
SET g.goal_type = t.id;


-- rename goals table's goal_type column to reflect changing to id's
ALTER TABLE goals
CHANGE goal_type goal_type_id INT;


-- assign goals table's foreign key to goal_types table
ALTER TABLE goals
ADD CONSTRAINT fk_goals_goal_types_id
FOREIGN KEY (goal_type_id)
REFERENCES goal_types(id);


-- create autodata_integrations table
CREATE TABLE autodata_integrations AS 
SELECT autodata_source
FROM goals
WHERE autodata_source IS NOT NULL
GROUP BY autodata_source
ORDER BY COUNT(autodata_source) DESC;


ALTER TABLE autodata_integrations
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;


ALTER TABLE autodata_integrations
CHANGE autodata_source name VARCHAR(50);


-- change goals table's autodata_source entries to id numbers
UPDATE goals g
LEFT JOIN autodata_integrations a
	ON g.autodata_source = a.name
SET g.autodata_source = a.id;


-- rename goals table's autodata_source column to reflect changing to id's
ALTER TABLE goals
CHANGE autodata_source autodata_id INT;


-- assign goals table's foreign key to autodata_integrations table
ALTER TABLE goals
ADD CONSTRAINT fk_goals_autodata_integrations_id
FOREIGN KEY (autodata_id)
REFERENCES autodata_integrations(id);


-- create data_sources table
CREATE TABLE data_sources(
	id INT AUTO_INCREMENT,
	name VARCHAR(50),	
	PRIMARY KEY (id)
);


INSERT INTO data_sources(name) VALUES
	('main'),
	('fitbit'),
	('strava');


-- change goals table's data_source entries to id numbers
UPDATE goals g
LEFT JOIN data_sources d
	ON g.data_source = d.name
SET g.data_source = d.id;


-- rename goals table's data_source column to reflect changing to id's
ALTER TABLE goals
CHANGE data_source data_source_id INT;


-- assign goals table's foreign key to data_sources table
ALTER TABLE goals
ADD CONSTRAINT fk_goals_data_sources_id
FOREIGN KEY (data_source_id)
REFERENCES data_sources(id);


-- DATAPOINTS --------------------------------------------------------------

-- create datapoints table
CREATE TABLE datapoints LIKE stg_datapoints;

INSERT INTO datapoints SELECT * FROM stg_datapoints;


-- drop data_source column
ALTER TABLE datapoints
DROP COLUMN data_source;


-- add surrogate primary key
ALTER TABLE datapoints
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;


-- [user_id, goal_id, created_at] as composite foreign key to the goals table 
ALTER TABLE datapoints
ADD CONSTRAINT fk_datapoints_goals
FOREIGN KEY (user_id, goal_id, created_at)
REFERENCES goals(user_id, goal_id, created_at);


-- user_id as a foreign key to the users table
ALTER TABLE datapoints
ADD CONSTRAINT fk_datapoints_users
FOREIGN KEY (user_id)
REFERENCES users(id);

 
-- PLEDGES -----------------------------------------------------------------

-- create pledges table
CREATE TABLE pledges LIKE stg_pledges;

INSERT INTO pledges SELECT * FROM stg_pledges;


-- drop data_source column
ALTER TABLE pledges
DROP COLUMN data_source;


-- add surrogate primary key
ALTER TABLE pledges
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;


-- [user_id, goal_id, created_at] as composite foreign key to the goals table 
ALTER TABLE pledges
ADD CONSTRAINT fk_pledges_goals
FOREIGN KEY (user_id, goal_id, created_at)
REFERENCES goals(user_id, goal_id, created_at);


-- user_id as a foreign key to the users table
ALTER TABLE pledges
ADD CONSTRAINT fk_pledges_users
FOREIGN KEY (user_id)
REFERENCES users(id);


-- make the pledges table's event column a foreign key with id numbers that connect to an events table
CREATE TABLE events(
	id INT AUTO_INCREMENT,
	type VARCHAR(50),
	PRIMARY KEY (id)
);


INSERT INTO events(type) VALUES
	('RCM'),
	('P--'),
	('P++'),
	('SDN'),
	('RRL'),
	('SHC'),
	('CRD'),
	('SSD'),
	('CSD');


UPDATE pledges p
LEFT JOIN events e
	ON p.event = e.type
SET p.event = e.id;


-- rename goals table's data_source column to reflect changing to id's
ALTER TABLE pledges
CHANGE event event_id INT;


-- convert new_amount column data type from INT to DECIMAL to reflect monetary values
ALTER TABLE pledges
MODIFY COLUMN new_amount DECIMAL(10,2);


-- event_id as a foreign key to the events table
ALTER TABLE pledges
ADD CONSTRAINT fk_pledges_events
FOREIGN KEY (event_id)
REFERENCES events(id);


-- SLOPE HISTORY -----------------------------------------------------------

-- create slope_history table
CREATE TABLE slope_history LIKE stg_slope_history;

INSERT INTO slope_history SELECT * FROM stg_slope_history;


-- drop data_source column
ALTER TABLE slope_history
DROP COLUMN data_source;


-- create surrogate primary id
ALTER TABLE slope_history
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;


-- [user_id, goal_id, created_at] foreign key referencing the goals table
ALTER TABLE slope_history
ADD CONSTRAINT fk_slope_history_goals
FOREIGN KEY (user_id, goal_id, created_at)
REFERENCES goals(user_id, goal_id, created_at);


-- user_id referencing the users table
ALTER TABLE slope_history
ADD CONSTRAINT fk_slope_history_users
FOREIGN KEY (user_id)
REFERENCES users(id);


-- STRAVA AUTODATA ---------------------------------------------------------

-- create strava_autodata table
CREATE TABLE strava_autodata LIKE stg_strava_autodata;

INSERT INTO strava_autodata SELECT * FROM stg_strava_autodata;


-- create surrogate primary key
ALTER TABLE strava_autodata
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;


-- [user_id, goal_id, created_at] foreign key referencing the goals table
ALTER TABLE strava_autodata
MODIFY COLUMN created_at DATETIME;


ALTER TABLE strava_autodata
ADD CONSTRAINT fk_strava_autodata_goals
FOREIGN KEY (user_id, goal_id, created_at)
REFERENCES goals(user_id, goal_id, created_at);


-- user_id foreign key referencing the users table
ALTER TABLE strava_autodata 
ADD CONSTRAINT fk_strava_autodata_users
FOREIGN KEY (user_id)
REFERENCES users(id);


-- creating strava_metrics table
CREATE TABLE strava_metrics(
	id INT AUTO_INCREMENT,
	type VARCHAR(50),
	PRIMARY KEY (id)
);


INSERT INTO strava_metrics(type) VALUES
	('duration'),
	('number'),
	('kilometers'),
	('miles'),
	('calories'),
	('weighted_duration');


-- changing strava_autodata table's metrics column to id numbers
UPDATE strava_autodata a
LEFT JOIN strava_metrics m
	ON a.metrics = m.type
SET a.metrics = m.id;


ALTER TABLE strava_autodata
CHANGE COLUMN metrics metric_id INT AFTER activity_group;


-- metric_id foreign key references strava_metrics table
ALTER TABLE strava_autodata
ADD CONSTRAINT fk_strava_autodata_metrics
FOREIGN KEY (metric_id)
REFERENCES strava_metrics(id);


-- creating activities table 
CREATE TABLE activities( 
	id INT AUTO_INCREMENT,
	type VARCHAR(50),
	PRIMARY KEY (id)
);

INSERT INTO activities(type) SELECT DISTINCT activity_type FROM strava_autodata;


-- changing strava_autodata table's activity_type column to id numbers
UPDATE strava_autodata s
LEFT JOIN activities a
	ON s.activity_type = a.type
SET s.activity_type = a.id;


ALTER TABLE strava_autodata
CHANGE COLUMN activity_type activity_id INT;


-- activity_id foreign key references activities table
ALTER TABLE strava_autodata
ADD CONSTRAINT fk_strava_autodata_activities
FOREIGN KEY (activity_id)
REFERENCES activities(id);


-- add activity group column
ALTER TABLE activities
ADD COLUMN activity_group VARCHAR(50);

ALTER TABLE strava_autodata
DROP COLUMN activity_group;


-- categorize each of the activity types
UPDATE activities
SET 
	activity_group = CASE
        -- Foot Sports
        WHEN type IN ('run', 'hike', 'walk') THEN 'foot_sports'
        -- Cycle Sports
        WHEN type IN ('ride', 'virtual_ride') THEN 'cycle_sports'
        -- Water Sports
        WHEN type IN ('swim', 'canoeing', 'stand_up_paddling', 'surfing', 'rowing', 'windsurf', 'kayaking', 'kitesurf') THEN 'water_sports'
        -- Winter Sports
        WHEN type IN ('ice_skate', 'alpine_ski', 'backcountry_ski', 'nordic_ski', 'snowboard', 'snowshoe') THEN 'winter_sports'
        -- Adaptive Sports
        WHEN type IN ('wheel_chair', 'handcycle') THEN 'adaptive_sports'
        -- Fitness Training
        WHEN type IN ('crossfit', 'elliptical', 'stair_stepper', 'weight_training', 'workout', 'yoga') THEN 'fitness_training'
        -- Other
        WHEN type IN ('inline_skate', 'rock_climbing') THEN 'other'
	END;


-- create activity_groups table
CREATE TABLE activity_groups(
	id INT AUTO_INCREMENT,
	name VARCHAR(50),
	PRIMARY KEY (id)
);


INSERT INTO activity_groups(name) VALUES 
	('foot_sports'),
	('cycle_sports'),
	('water_sports'),
	('winter_sports'),
	('adaptive_sports'),
	('fitness_training'),
	('other');


-- change activity_group to a column of id numbers connecting to activity_groups table
UPDATE activities a
LEFT JOIN activity_groups g
	ON a.activity_group = g.name
SET a.activity_group = g.id;


ALTER TABLE activities
CHANGE COLUMN activity_group group_id INT;


-- group_id foreign key to activity_groups table
ALTER TABLE activities
ADD CONSTRAINT fk_activities_activity_groups
FOREIGN KEY (group_id)
REFERENCES activity_groups(id);


-- FITBIT AUTODATA ---------------------------------------------------------

-- create fibit_autodata table
CREATE TABLE fitbit_autodata LIKE stg_fitbit_autodata;

INSERT INTO fitbit_autodata SELECT * FROM stg_fitbit_autodata;


-- create surrogate primary key id
ALTER TABLE fitbit_autodata
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;


-- [user_id, goal_id, created_at] foreign key references goals table
ALTER TABLE fitbit_autodata
ADD CONSTRAINT fk_fitbit_autodata_goals
FOREIGN KEY (user_id, goal_id, created_at)
REFERENCES goals(user_id, goal_id, created_at);


-- user_id foreign key references users table
ALTER TABLE fitbit_autodata
ADD CONSTRAINT fk_fitbit_autodata_users
FOREIGN KEY (user_id)
REFERENCES users(id);


-- create fitbit_metrics table
CREATE TABLE fitbit_metrics(
	id INT AUTO_INCREMENT,
	type VARCHAR(50),
	PRIMARY KEY (id)
);

INSERT INTO fitbit_metrics(type) VALUES
	('steps'),
	('weight'),
	('calories_in'),
	('calories_out'),
	('hours_slept'),
	('fairly_active_time'),
	('netcalorie'),
	('activities'),
	('water'),
	('body_fat'),
	('floors');


-- change fitbit_autodata's metrics column to id numbers, referencing fitbit_metrics table
UPDATE fitbit_autodata a
LEFT JOIN fitbit_metrics m
	ON a.metrics = m.type
SET a.metrics = m.id;


ALTER TABLE fitbit_autodata
CHANGE COLUMN metrics metric_id INT;


-- metric_id foreign key references fitbit_metric table's id 
ALTER TABLE fitbit_autodata 
ADD CONSTRAINT fk_fitbit_autodata_metrics
FOREIGN KEY (metric_id)
REFERENCES fitbit_metrics(id);