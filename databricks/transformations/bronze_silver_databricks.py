import dlt
from pyspark.sql.functions import (
    col, trim, upper, when, lit, regexp_replace
)


@dlt.table(
    name="la_crime_bronze",
    comment="Raw LA Crime data from 2020 to present loaded from volume"
)
def la_crime_bronze():
   
    # Read from volume in the la_crime catalog
    return (
        spark.read
        .format("csv")  
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/Volumes/la_crime/bronze/raw/la_crime_full.csv")
    )



@dlt.table(
    name="silver.la_crime_silver",
    comment="Cleaned LA Crime data with validated fields",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_all({
    "valid_dr_no": "DR_NO IS NOT NULL",
    "valid_date_occ": "DATE_OCC IS NOT NULL",
    "valid_crm_cd": "CRM_CD IS NOT NULL"
})
def la_crime_silver():
    """
    Silver layer - Data cleaning and transformation
    """
    
    return (
        dlt.read("la_crime_bronze")
        .select(
            # 1. DR_NO - Trim whitespace
            trim(col("DR_NO")).alias("DR_NO"),
            
            col("DATE_RPTD"),
            col("DATE_OCC"),
            col("TIME_OCC"),
            
            # 3. Area - Keep as is
            col("AREA"),
            
            # 4. AREA_NAME - Trim and uppercase for consistency
            upper(trim(col("AREA_NAME"))).alias("AREA_NAME"),
            
            col("RPT_DIST_NO"),
            col("PART_1_2"),
            
            # 5. Crime codes
            col("CRM_CD"),
            
            # 6. CRM_CD_DESC - Trim whitespace
            trim(col("CRM_CD_DESC")).alias("CRM_CD_DESC"),
            
            # 7. MOCODES - Trim
            trim(col("MOCODES")).alias("MOCODES"),
            
            # 8. Victim info - Clean and validate
            when(col("VICT_AGE") > 0, col("VICT_AGE")).otherwise(lit(None)).alias("VICT_AGE"),
            upper(trim(col("VICT_SEX"))).alias("VICT_SEX"),
            upper(trim(col("VICT_DESCENT"))).alias("VICT_DESCENT"),
            
            # 9. Premise info
            col("PREMIS_CD"),
            trim(col("PREMIS_DESC")).alias("PREMIS_DESC"),
            
            # 10. Weapon info
            col("WEAPON_USED_CD"),
            trim(col("WEAPON_DESC")).alias("WEAPON_DESC"),
            
            # 11. Status
            upper(trim(col("STATUS"))).alias("STATUS"),
            trim(col("STATUS_DESC")).alias("STATUS_DESC"),
            
            # 12. Additional crime codes
            col("CRM_CD_1"),
            col("CRM_CD_2"),
            col("CRM_CD_3"),
            col("CRM_CD_4"),
            
            # 13. Location - Trim and clean extra spaces
            regexp_replace(trim(col("LOCATION")), "\\s+", " ").alias("LOCATION"),
            trim(col("CROSS_STREET")).alias("CROSS_STREET"),
            
            # 14. Coordinates - Filter invalid (0,0) coordinates
            when((col("LAT") != 0) & (col("LON") != 0), col("LAT")).otherwise(lit(None)).alias("LAT"),
            when((col("LAT") != 0) & (col("LON") != 0), col("LON")).otherwise(lit(None)).alias("LON")
        )
        # Additional filters for critical data
        .filter(
            (col("DR_NO").isNotNull()) &
            (col("DATE_OCC").isNotNull()) &
            (col("CRM_CD").isNotNull())
        )
        
    )
