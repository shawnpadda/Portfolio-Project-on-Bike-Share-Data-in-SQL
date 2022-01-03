/************************************************************************SQL Data Analysis Project on a SF Bay Area Bike Share Dataset**************************************************************************************/

/* This project is about Bay Area bike share data. It consists of four dataframes including station, status,trips, and weather. The data contains an anonymized bike trip data from August 2013 to August 2015.
The dataframes were loaded into the newly created database called WorkDB.*/

/****************************************************************************Source Of The DATA*****************************************************************************************************************************/
/* The source of the data is kaggle. The url of the data is below:
https://www.kaggle.com/benhamner/sf-bay-area-bike-share */

/****************************************************************************Objective of the Project**********************************************************************************************************************/
/* The motivation behind the project was testing the knowledge of the structured query language (SQL) on bike sharing data. Moreover, I sought to collect some information about the affect of weather on bike trips, number of bikes available according to the cities in the bay area, find out 
the geolocation of the unique bike id, etc. */

/*****************************************************************************Initialize Project Work*********************************************************************************************************************/
-- Create tables to feed data in the WorkDb database.
-- Create a Station table
create table station(id serial,name varchar(100),lat double precision,
					 long double precision ,dock_count integer,city varchar(100),
					 installation_date date)
-- Create a Status table			 
create table status (
station_id serial ,bikes_available int,docks_available int,
	time timestamp
)

-- Create a Trip table
create table trip (
id serial,duration int,start_date timestamp,
	start_station_name varchar(100),start_station_id int,
	end_date timestamp,end_station_name, varchar(100)end_station_id int, 
	bike_id int,subscription_type varchar(100),zip_code varchar(100)
)
-- Create a Weather table
create table weather(
date timestamp,max_temperature_f real,mean_temperature_f real,,min_temperature_f real,,max_dew_point_f real,,mean_dew_point_f real,,min_dew_point_f real,,max_humidity real,mean_humidity real,min_humidity real,,max_sea_level_pressure_inches real,,mean_sea_level_pressure_inches real,min_sea_level_pressure_inches real,max_visibility_miles real,mean_visibility_miles real,min_visibility_miles real,max_wind_Speed_mph real,mean_wind_speed_mph real,max_gust_speed_mph real,precipitation_inches varchar(100),cloud_cover real,events varchar(50),wind_dir_degrees real,zip_code varchar(100)



-- Load status dataframe table in the WorkDB database
command " "\\copy public.status (station_id, bikes_available, docks_available, \"time\") FROM '/Users/sirpreetpadda/Downloads/status.csv' DELIMITER ',' 
CSV HEADER QUOTE '\"' ESCAPE '''';""

-- Load station dataframe table in the WorkDB database
command " "\\copy public.station (id, name, lat, \"long\", dock_count, city, installation_date) FROM '/Users/sirpreetpadda/Downloads/station.csv' DELIMITER ','
CSV HEADER QUOTE '\"' ESCAPE '''';""

-- Load trip dataframe table in the WORKDB database
command " "\\copy public.trip (id, duration, start_date, start_station_name, start_station_id, end_date, end_station_name, end_station_id, bike_id, subscription_type, zip_code) FROM '/Users/sirpreetpadda/Downloads/trip1.csv' DELIMITER ',' 
CSV HEADER QUOTE '\"' ESCAPE '''';""

-- Load weather dataframe table in the WORKDB database
command " "\\copy public.weather (date, max_temperature_f, mean_temperature_f, min_temperature_f, max_dew_point_f, mean_dew_point_f, min_dew_point_f, max_humidity, mean_humidity, min_humidity, max_sea_level_pressure_inches, mean_sea_level_pressure_inches, min_sea_level_pressure_inches, max_visibility_miles, mean_visibility_miles, min_visibility_miles, max_wind_speed_mph, mean_wind_speed_mph, max_gust_speed_mph, precipitation_inches, cloud_cover, events, wind_dir_degrees, zip_code) FROM '/Users/sirpreetpadda/Downloads/weather.csv' DELIMITER ',' 
CSV HEADER QUOTE '\"' ESCAPE '''';""


/**********************************************************************************Verifying Columns in each Dataframe******************************************************************************************************/
select * from trip
select * from status
select * from station
select * from weather

/**********************************************************************************Querying Trip and Weather Dataframes*****************************************************************************************************/
-- Find how many trips are in the data set:
select count(*)
from trip
--The total number of trips in the datset are 1339918.


-- Find the earliest start date and end date:
select min(start_date), min(end_date)
from trip
-- The earliest start date and end date is as follows: "2013-08-29 09:08:00" and "2013-08-29 09:11:00."

-- Find the latest start date and end date:
select max(start_date), max(end_date)
from trip
-- The latest start date and end date is as follows: "2015-08-31 23:26:00" and "2015-08-31 23:39:00."

-- Find the total number of bikes in the dataset:
select count(distinct bike_id)
from trip
--The total number of bikes in the dataset are 700 bikes.

-- Find out the maximum duration along with the bike_id, start and end station name:
select duration, start_station_name, end_station_name, bike_id
from trip
group by duration,start_station_name,end_station_name,bike_id
order by duration desc
limit 1
-- The maximum duration along with the bike_id, start and end station name is as follows: 17270400,	"South Van Ness at Market", "2nd at Folsom"	and 535.

-- Find out the top five distinct bike id's which have the subscription type equal to the customers in the dataset:
select distinct bike_id
from trip
where subscription_type = 'Customer'
order by bike_id desc
limit 5
-- The top five distinct bike id's which have the subscription type equal to the customers in the dataset are as follows: 878, 877, 876, 740, and 717.

-- Count the total number of subscribers in the trip dataset:
select count(subscription_type)
from trip
where subscription_type = 'Subscriber'
-- There are total of 566746 subscribers in the trip dataset.

-- Peform a left join to see the maximum temperature along with maximum duration and bike id:
select max(w.max_temperature_f) as maximum_temperature, max(t.duration) as maximum_duration, 
t.bike_id
from trip t left join weather w
on t.zip_code = w.zip_code
group by t.bike_id
limit 1
-- The maximum temperature along with maximum duration and bike id is as follows: 102, 602338 and 9.
	
-- Use multiple datasources to find out the bike id that has the minimum geolocation in San Jose only:
select distinct t.bike_id, min(s.lat), min(s.long), s.city
from trip t, station s
where t.bike_id = '878' AND city = 'San Jose'
group by t.bike_id, s.city
-- The bike id that has the minimum geolocation in San Jose is the 878. The minimum geolocation is as follows: 37.329732 lat and -121.90573300000001 long. The city is "San Jose."

-- Create a view tw using trip and weather tables which will be called later.
Create view tw as
select duration, start_date, start_station_name, end_date, end_station_name,
	bike_id, subscription_type,t.zip_code,max_temperature_f,min_temperature_f,
	max_humidity, min_humidity,max_visibility_miles,min_visibility_miles,
	max_wind_speed_mph,precipitation_inches,events
from trip t, weather w
where t.zip_code = w.zip_code
-- A view tw has been created and saved in the WorkDB database.
	
-- Using a view called tw, find out the bike_id which has covered the maximum duration, minimum visibility and events equal to rain only.
select bike_id, max(duration)as maximum_duration, min_visibility_miles, events
from tw
where events IS NOT NULL AND events = 'Rain'
Group by bike_id, min_visibility_miles, events
Order by min_visibility_miles, maximum_duration desc
limit 1
-- The bike_id which has covered the maximum duration, minimum visibility and events equal to rain is as follows: 175, 165067, 0, and "Rain."
	
-- Using a view called tw find out the minimum duration along with total number of bikes and an event equal to fog only.
select count(bike_id), min(duration) as minimum_duration, events
from tw
where events = 'Fog' 
AND duration <= 60
group by events
order by minimum_duration 
-- The minimum duration along with total number of bikes and an event equal to fog only is as follows: 2808, 60, and "Fog."
	
/**************************************************************************Querying Trip and Station Dataframes*************************************************************************************************************/
	
select * from trip
select * from station
	
-- Create another view called ts which will be called later.
create view ts as
select duration, start_date, start_station_name, end_date, end_station_name,
	bike_id, subscription_type,t.zip_code,lat,long, installation_date, city,name 
from trip t, station s
where t.start_station_name = s.name
-- A view called ts has been created.
	
-- Using ts view, find out the old station along with name and minimum number of bikes shared at this particular station.
select distinct min(length(name)) as min_length_character, min(installation_date)as min_installation_date, 
	count(bike_id)as min_bike_id,upper(name)
from ts
where installation_date <= '2013-08-05'
group by name
order by min_installation_date, min_bike_id
limit 1
-- The old station along with name and minimum number of bikes shared at this particular station is as follows: "ADOBE ON ALMADEN" and 5028.
	
-- Find out the names of the stations along with their total, partitioned by cities in the Bay Area.
select distinct name as names_of_the_stations, city,
count(name) over(partition by city)
from station
order by names_of_the_stations, city
-- After partitioned by cities a final output is generated which indicates the name of the station along with the count.
	
-- Using a trip table, arrange duration into a lead order.
select bike_id, start_station_name, end_station_name,subscription_type,
Lead(duration) over() as next_duration
from trip
-- Using window functions lead() and over(),it will show the next lead value of a preceding row.
	
-- Using a trip table, arrange duration inti an lag order.
select bike_id, start_station_name, end_station_name,subscription_type,
Lag(duration) over() as previous_duration
from trip
-- Using window functions lag() and over(),it will show the previous lag value of a current row.
	
/***************************************************************************Querying Trip and Status Dataframes*************************************************************************************************************/

select * from status
select * from station
	
-- Find out the bikes available in city of San Francisco only.
select distinct city,(select bikes_available from status limit 1)
from station
where id IN (select station_id from status)
AND city = 'San Francisco'
-- There are total of 9 bikes available in city of San Francisco.
	
/*****************************************************************************Declaration***********************************************************************************************************************************/
/* Last but not least, the portfolio project delivered a very good understanding of the structured query language (SQL). I will be able to use my knowledge for the upcoming SQL projects.*/
	
	
/*****************************************************************************END Of The Portfolio Project******************************************************************************************************************/
	
	
	
	