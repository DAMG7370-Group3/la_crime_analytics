create user jaswanth;

create role dev_sysdadmin;

create database LA_CRIME;

grant all privileges on database LA_CRIME to role dev_sysadmin;


select * from LA_CRIME.GOLD.FACT_CRIME_INCIDENT where dr_no in(201415194,
201415195,
201415196,
201415197);

select * from LA_CRIME_2020_PRESENT;

select count(*) from LA_CRIME.bronze.la_crime_2020_present where year(DATE_OCC)='2025';
show users;

-- Check if all CRIME_TYPE_KEYs in bridge exist in dimension
SELECT 
    b.CRIME_TYPE_KEY,
    COUNT(*) as bridge_count
FROM GOLD.BRIDGE_CRIME_INCIDENT_CRIME_TYPE b
LEFT JOIN GOLD.DIM_CRIME_TYPE d 
    ON b.CRIME_TYPE_KEY = d.CRIME_TYPE_KEY
WHERE d.CRIME_TYPE_KEY IS NULL
GROUP BY b.CRIME_TYPE_KEY;


select * from LA_CRIME.SILVER.la_crime_silver where TIME_OCC is null;


select * from LA_CRIME.GOLD.FACT_CRIME_INCIDENT a left join dim_time  b on a.time_occurred_key=b.time_occ where HOUR_24 is null;

select * from dim_date where DATE_KEY=20240301;
