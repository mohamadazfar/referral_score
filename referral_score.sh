#!/bin/bash

export PGPASSWORD="postgres_user_password"

# Create table to store referral score dataset
psql -h host -p port -U user -d dw -c "create table if not exists referral_score_initial_dataset ( user_id integer, referrer_status char);"

# Insert referral score initial dataset
psql -h host -p port -U user -d dw -c "\copy referral_score_initial_dataset FROM '/home/ubuntu/ExternalData/Referral_Status_Dataset.txt' delimiter ','"

# Create table to store referral score dataset
psql -h host -p port -U user -d dw -c "create table if not exists referral_score ( id serial primary key, user_id integer, prediction_N numeric, prediction_Y numeric, created_at timestamp not null default now(), updated_at timestamp not null default now(), decile smallint, referrer_status char, subdecile smallint);"


# Create table to store initial dataset
psql -h host -p port -U user -d dw -c "create table referral_score_initial_dataset ( user_id integer, referee_status char, referrer_status char);"

# Create staging table
psql -h host -p port -U user -d dw -c "create table if exists referral_score_staging ( id integer, user_id integer, prediction_N numeric, prediction_Y numeric);"

# Insert user from csv file into a staging table
psql -h host -p port -U user -d dw -c "\copy referral_score_staging FROM '/home/ubuntu/ExternalData/predictions_2.csv' delimiter ','"


# Command to insert new user
psql -h host -p port -U user -d dw -c "insert into referral_score (id, user_id, prediction_n, prediction_y)  select referral_score_staging.id, referral_score_staging.user_id, referral_score_staging.prediction_n, referral_score_staging.prediction_y from referral_score_staging left join referral_score on referral_score_staging.user_id = referral_score.user_id where referral_score.user_id is null;"


# Command to update existing user
psql -h host -p port -U user -d dw -c "update referral_score set prediction_n = referral_score_staging.prediction_n, prediction_y = referral_score_staging.prediction_y from referral_score_staging where referral_score.user_id = referral_score_staging.user_id;"


# Once data is loaded into referral_score table, delete all data in referral_score_staging
psql -h host -p port -U user -d dw -c "delete from referral_score_staging;"

# Add column decile in staging table
psql -h host -p port -U user -d dw -c "alter table referral_score_staging add column decile smallint"   

# Add column subdecile in staging table
psql -h host -p port -U user -d dw -c "alter table referral_score_staging add column subdecile smallint"   


# Insert decile data into staging table
psql -h host -p port -U user -d dw -c "insert into referral_score_staging (user_id, decile) select referral_score.user_id, ntile(10) over(order by prediction_y desc) from referral_score "


# Insert data into decile column in referral_score table
psql -h host -p port -U user -d dw -c "update referral_score set decile = referral_score_staging.decile from referral_score_staging where referral_score.user_id = referral_score_staging.user_id "

# Delete data from referral_staging
psql -h host -p port -U user -d dw -c "delete from referral_score_staging"


# Insert the original referrer label into referral score table
psql -h host -p port -U user -d dw -c "update referral_score set referrer_status = referral_score_initial_dataset.referrer_status from referral_score_initial_dataset where referral_score.user_id = referral_score_initial_dataset.user_id "

# Create insert subdecile data=1 into staging table
psql -h host -p port -U user -d dw -c "insert into referral_score_staging (user_id, subdecile) select referral_score.user_id,ntile(10) over(order by prediction_y desc) from referral_score where decile = 1 "


# Create insert subdecile data=2 into staging table
psql -h host -p port -U user -d dw -c "insert into referral_score_staging (user_id, subdecile) select referral_score.user_id,ntile(10) over(order by prediction_y desc) from referral_score where decile = 2 "

# Create insert subdecile data=3 into staging table
psql -h host -p port -U user -d dw -c "insert into referral_score_staging (user_id, subdecile) select referral_score.user_id,ntile(10) over(order by prediction_y desc) from referral_score where decile = 3 "

# Create insert subdecile data=4 into staging table
psql -h host -p port -U user -d dw -c "insert into referral_score_staging (user_id, subdecile) select referral_score.user_id,ntile(10) over(order by prediction_y desc) from referral_score where decile = 4 "

# Create insert subdecile data=5 into staging table
psql -h host -p port -U user -d dw -c "insert into referral_score_staging (user_id, subdecile) select referral_score.user_id,ntile(10) over(order by prediction_y desc) from referral_score where decile = 5 "

# Create insert subdecile data=6 into staging table
psql -h host -p port -U user -d dw -c "insert into referral_score_staging (user_id, subdecile) select referral_score.user_id,ntile(10) over(order by prediction_y desc) from referral_score where decile = 6 "

# Create insert subdecile data=7 into staging table
psql -h host -p port -U user -d dw -c "insert into referral_score_staging (user_id, subdecile) select referral_score.user_id,ntile(10) over(order by prediction_y desc) from referral_score where decile = 7 "

# Create insert subdecile data=8 into staging table
psql -h host -p port -U user -d dw -c "insert into referral_score_staging (user_id, subdecile) select referral_score.user_id,ntile(10) over(order by prediction_y desc) from referral_score where decile = 8 "

# Create insert subdecile data=9 into staging table
psql -h host -p port -U user -d dw -c "insert into referral_score_staging (user_id, subdecile) select referral_score.user_id,ntile(10) over(order by prediction_y desc) from referral_score where decile = 9 "

# Create insert subdecile data=10 into staging table
psql -h host -p port -U user -d dw -c "insert into referral_score_staging (user_id, subdecile) select referral_score.user_id,ntile(10) over(order by prediction_y desc) from referral_score where decile = 10 "

# Insert data into subdecile column in referral_score table
psql -h host -p port -U user -d dw -c "update referral_score set subdecile = referral_score_staging.subdecile from referral_score_staging where referral_score.user_id = referral_score_staging.user_id "

# Drop staging table
psql -h host -p port -U user -d dw -c "drop table referral_score_staging"

# Drop original referrer table
psql -h host -p port -U user -d dw -c "drop table referral_score_initial_dataset"