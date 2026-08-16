---------------------------------------- RAW LAYER -----------------------------------------------
"""id: uuid, 
   data_json: payload comes from scrapy, 
   created_at: when data gets in the layer, 
   mode: production or testing"""
create table raw.flights_scrapy(
   id uuid primary key, 
   data_json json, 
   created_at timestamp,
   mode text --{TESTING, PRODUCTION}
)
---------------------------------------- SILVER LAYER ----------------------------------------------
"""
id: unique id idicates a user's session
flight_from: origin airport 
flight_to: destity airport 
company: company that operates the flight 
exit_hour: exit_hour from origin airport
entry_hour: entry_hour that gets into destity airport
miles_cost: cost in miles
reais_cost: cost in BRL
emission_type: flight type emission
ticket_date, 
search_date: ,
emission_link,
website,
mode: TESTING OR PRODUCTION
inserted_at: time that data getes in the table
"""


CREATE TABLE silver.flights_scrapy (
  id TEXT,
  flight_from TEXT,
  flight_to TEXT,
  company TEXT,
  exit_hour DATE,
  entry_hour DATE,
  miles_cost INT,
  reais_cost INT,
  emission_type TEXT,
  ticket_date DATE,
  search_date DATE,
  emission_link TEXT,
  website TEXT,
  mode TEXT,
  inserted_at date
);

select  * from raw.flights_scrapy fs2 

select * from silver.flights_scrapy



