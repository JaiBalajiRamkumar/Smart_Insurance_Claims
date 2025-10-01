-- Databricks notebook source
-- MAGIC %md This notebook is available at https://github.com/databricks-industry-solutions/smart-claims

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Add telematics and accident data to the claims & policy data
-- MAGIC * simulate iot streaming data join with claims

-- COMMAND ----------

-- %run ./setup/initialize

-- COMMAND ----------

create table if not exists silver_claim_policy_accident as 
(
select p_c.*, t.* except(t.claim_no, t.chassis_no)
from 
silver_claim_policy_location as p_c 
join 
silver_accident as t
on p_c.claim_no=t.claim_no
)

-- COMMAND ----------

create table if not exists silver_claim_policy_telematics_avg as 
(
select p_c.*, t.telematics_latitude, t.telematics_longitude, t.telematics_speed
from 
silver_claim_policy_location as p_c 
join
(select chassis_no, avg(speed) as telematics_speed,
avg(latitude) as telematics_latitude,
avg(longitude) as telematics_longitude
from
silver_telematics
group by chassis_no) t
on p_c.chassis_no=t.chassis_no
)

-- COMMAND ----------

create table if not exists silver_claim_policy_accident as 
(
select a_c.* except(zip_code,use_of_vehicle,suspicious_activity,sum_insured,product,policytype,policy_no,pol_issue_date,pol_expiry_date,pol_eff_date,number_of_witnesses,neighborhood,premium,months_as_customer,model_year,model,incident_type,latitude,longitude,incident_severity,incident_hour,a_c.collision_number_of_vehicles_involved, a_c.claim_date, a_c.claim_amount_vehicle, a_c.claim_amount_total, a_c.claim_amount_property, a_c.claim_amount_injury, a_c.claim_no, a_c.chassis_no, a_c.address, a_c.body, a_c.borough,make,a_c.collision_type,a_c.cust_id,a_c.deductable,a_c.driver_age,a_c.driver_insured_relationship,a_c.driver_license_issue_date, drv_dob,incident_date), t.*,p_c.* except(p_c.claim_no, p_c.chassis_no)
from 
silver_claim_policy_telematics_avg as p_c 
join 
silver_accident as t
on p_c.claim_no=t.claim_no
join 
silver_claim_policy_location as a_c 
on a_c.claim_no=t.claim_no
)

-- COMMAND ----------

create table if not exists silver_claim_policy_telematics as 
(
select p_c.*, t.latitude as telematics_latitude, t.longitude as telematics_longitude, t.event_timestamp as telematics_timestamp, t.speed as telematics_speed
from 
silver_telematics as t
join 
silver_claim_policy_location as p_c 
on p_c.chassis_no=t.chassis_no
)
