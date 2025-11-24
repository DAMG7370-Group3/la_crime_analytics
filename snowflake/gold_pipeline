import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, concat, lpad, year, month, dayofweek, when, lit, row_number, monotonically_increasing_id, max as max_, min as min_, to_char
from snowflake.snowpark.window import Window

def main(session: snowpark.Session):
    """Create simplified Gold dimensional model with essential columns only"""
    
    print("Creating Simplified Gold Layer Dimensional Model...")
    silver_df = session.table("SILVER.LA_CRIME_SILVER")
    print(f"Silver records: {silver_df.count()}")
    print(f"Silver columns: {silver_df.columns}")
    
    # ===== DIM_DATE =====
    print("\n1. Creating DIM_DATE (simplified)...")
    dates_distinct = silver_df.select(col("DATE_OCC")).distinct().filter(col("DATE_OCC").isNotNull())
    
    dim_date = dates_distinct.select(
        to_char(col("DATE_OCC"), "YYYYMMDD").cast("integer").alias("DATE_KEY"),
        col("DATE_OCC").alias("FULL_DATE"),
        dayofweek(col("DATE_OCC")).alias("DAY_OF_WEEK"),
        month(col("DATE_OCC")).alias("MONTH_NUM"),
        when(month(col("DATE_OCC")).in_([1,2,3]), lit("Q1")).when(month(col("DATE_OCC")).in_([4,5,6]), lit("Q2"))
        .when(month(col("DATE_OCC")).in_([7,8,9]), lit("Q3")).otherwise(lit("Q4")).alias("QUARTER_NUM"),
        year(col("DATE_OCC")).alias("YEAR_NUM")
    )
    dim_date.write.mode("overwrite").save_as_table("GOLD.DIM_DATE")
    print(f"    {dim_date.count()} records")
    
    # Create DIM_DATE for DATE_RPTD as well
    print("\n1b. Creating DIM_DATE entries for DATE_RPTD...")
    dates_rpt_distinct = silver_df.select(col("DATE_RPTD")).distinct().filter(col("DATE_RPTD").isNotNull())
    
    dim_date_rpt = dates_rpt_distinct.select(
        to_char(col("DATE_RPTD"), "YYYYMMDD").cast("integer").alias("DATE_KEY"),
        col("DATE_RPTD").alias("FULL_DATE"),
        dayofweek(col("DATE_RPTD")).alias("DAY_OF_WEEK"),
        month(col("DATE_RPTD")).alias("MONTH_NUM"),
        when(month(col("DATE_RPTD")).in_([1,2,3]), lit("Q1")).when(month(col("DATE_RPTD")).in_([4,5,6]), lit("Q2"))
        .when(month(col("DATE_RPTD")).in_([7,8,9]), lit("Q3")).otherwise(lit("Q4")).alias("QUARTER_NUM"),
        year(col("DATE_RPTD")).alias("YEAR_NUM")
    )
    
    # Union with existing date dimension to avoid duplicates
    dim_date_combined = dim_date.union(dim_date_rpt).distinct()
    dim_date_combined.write.mode("overwrite").save_as_table("GOLD.DIM_DATE")
    print(f"   ✓ Updated: {dim_date_combined.count()} records")
    
    # ===== DIM_TIME =====
    print("\n2. Creating DIM_TIME...")
    time_distinct = silver_df.select(col("TIME_OCC")).distinct().filter(col("TIME_OCC").isNotNull())
    
    dim_time = time_distinct.select(
        col("TIME_OCC").cast("integer").alias("TIME_KEY"),  # Cast TIME_OCC to integer as the key
        col("TIME_OCC").alias("TIME_OCC"),
        (col("TIME_OCC") / 100).cast("integer").alias("HOUR_24"),
        (col("TIME_OCC") % 100).cast("integer").alias("MINUTE"),
        when((col("TIME_OCC") / 100).cast("integer") < 12, lit("AM")).otherwise(lit("PM")).alias("AM_PM")
    )
    dim_time.write.mode("overwrite").save_as_table("GOLD.DIM_TIME")
    print(f"    {dim_time.count()} records")
    
    # ===== DIM_LOCATION =====
    print("\n3. Creating DIM_LOCATION...")
    location_grouped = silver_df.filter(col("AREA").isNotNull()) \
        .group_by(col("AREA"), col("RPT_DIST_NO")) \
        .agg(
            max_(col("AREA_NAME")).alias("AREA_NAME"),
            max_(col("LOCATION")).alias("LOCATION"),
            max_(col("CROSS_STREET")).alias("CROSS_STREET"),
            max_(col("LAT")).alias("LAT"),
            max_(col("LON")).alias("LON")
        )
    
    window_location = Window.order_by(col("AREA"), col("RPT_DIST_NO"))
    
    dim_location = location_grouped.select(
        row_number().over(window_location).alias("LOCATION_KEY"),
        col("AREA"), 
        col("AREA_NAME"), 
        col("RPT_DIST_NO"), 
        col("LOCATION"), 
        col("CROSS_STREET"), 
        col("LAT"), 
        col("LON")
    )
    dim_location.write.mode("overwrite").save_as_table("GOLD.DIM_LOCATION")
    print(f"    {dim_location.count()} records")
    
    # ===== DIM_CRIME_TYPE ===== (SIMPLIFIED)
    print("\n4. Creating DIM_CRIME_TYPE (simplified)...")
    crime_distinct = silver_df.select(col("CRM_CD"), col("CRM_CD_DESC"), col("PART_1_2")).distinct().filter(col("CRM_CD").isNotNull())
    window_crime = Window.order_by(col("CRM_CD"))
    
    dim_crime_type = crime_distinct.select(
        row_number().over(window_crime).alias("CRIME_TYPE_KEY"),
        col("CRM_CD").alias("CRM_CD"),
        col("CRM_CD_DESC").alias("CRM_CD_DESC"),
        col("PART_1_2").alias("PART_1_2"),
        when(col("CRM_CD_DESC").like("%THEFT%"), lit("Theft")).when(col("CRM_CD_DESC").like("%BURGLARY%"), lit("Burglary"))
        .when(col("CRM_CD_DESC").like("%ROBBERY%"), lit("Robbery")).when(col("CRM_CD_DESC").like("%ASSAULT%"), lit("Assault"))
        .when(col("CRM_CD_DESC").like("%BATTERY%"), lit("Battery")).when(col("CRM_CD_DESC").like("%VANDALISM%"), lit("Vandalism"))
        .when(col("CRM_CD_DESC").like("%VEHICLE%"), lit("Vehicle Crime")).otherwise(lit("Other")).alias("CRIME_CATEGORY"),
        when(col("PART_1_2") == 1, lit("High")).when(col("PART_1_2") == 2, lit("Medium")).otherwise(lit("Low")).alias("SEVERITY_LEVEL")
    )
    dim_crime_type.write.mode("overwrite").save_as_table("GOLD.DIM_CRIME_TYPE")
    print(f"    {dim_crime_type.count()} records")
    
    # ===== DIM_VICTIM ===== (SIMPLIFIED)
    print("\n5. Creating DIM_VICTIM (simplified)...")
    victim_distinct = silver_df.select(col("VICT_AGE"), col("VICT_SEX"), col("VICT_DESCENT")).distinct()
    window_victim = Window.order_by(col("VICT_AGE"), col("VICT_SEX"), col("VICT_DESCENT"))
    
    dim_victim = victim_distinct.select(
        row_number().over(window_victim).alias("VICTIM_KEY"),
        col("VICT_AGE"),
        when(col("VICT_AGE") < 18, lit("0-17")).when(col("VICT_AGE").between(18, 25), lit("18-25"))
        .when(col("VICT_AGE").between(26, 35), lit("26-35")).when(col("VICT_AGE").between(36, 50), lit("36-50"))
        .when(col("VICT_AGE").between(51, 65), lit("51-65")).when(col("VICT_AGE") > 65, lit("65+")).otherwise(lit("Unknown")).alias("AGE_GROUP"),
        col("VICT_SEX"),
        col("VICT_DESCENT"),
        when(col("VICT_DESCENT").in_(["A","C","J","K","V","F","Z"]), lit("Asian")).when(col("VICT_DESCENT") == "H", lit("Hispanic"))
        .when(col("VICT_DESCENT") == "B", lit("Black")).when(col("VICT_DESCENT") == "W", lit("White")).otherwise(lit("Other")).alias("DESCENT_GROUP")
    )
    dim_victim.write.mode("overwrite").save_as_table("GOLD.DIM_VICTIM")
    print(f"    {dim_victim.count()} records")
    
    # ===== DIM_PREMISE ===== (SIMPLIFIED)
    print("\n6. Creating DIM_PREMISE (simplified)...")
    premise_distinct = silver_df.select(col("PREMIS_CD"), col("PREMIS_DESC")).distinct().filter(col("PREMIS_CD").isNotNull())
    window_premise = Window.order_by(col("PREMIS_CD"))
    
    dim_premise = premise_distinct.select(
        row_number().over(window_premise).alias("PREMISE_KEY"),
        col("PREMIS_CD"), 
        col("PREMIS_DESC"),
        when(col("PREMIS_DESC").like("%STREET%"), lit("Street")).when(col("PREMIS_DESC").like("%RESIDENCE%"), lit("Residential"))
        .when(col("PREMIS_DESC").like("%APARTMENT%"), lit("Residential")).when(col("PREMIS_DESC").like("%HOUSE%"), lit("Residential"))
        .when(col("PREMIS_DESC").like("%PARKING%"), lit("Parking")).when(col("PREMIS_DESC").like("%VEHICLE%"), lit("Vehicle"))
        .when(col("PREMIS_DESC").like("%STORE%"), lit("Commercial")).when(col("PREMIS_DESC").like("%BUSINESS%"), lit("Commercial"))
        .otherwise(lit("Other")).alias("PREMISE_CATEGORY")
    )
    dim_premise.write.mode("overwrite").save_as_table("GOLD.DIM_PREMISE")
    print(f"    {dim_premise.count()} records")
    
    # ===== DIM_WEAPON ===== (SIMPLIFIED)
    print("\n7. Creating DIM_WEAPON (simplified)...")
    weapon_distinct = silver_df.select(col("WEAPON_USED_CD"), col("WEAPON_DESC")).distinct()
    window_weapon = Window.order_by(col("WEAPON_USED_CD"))
    
    dim_weapon = weapon_distinct.select(
        row_number().over(window_weapon).alias("WEAPON_KEY"),
        col("WEAPON_USED_CD"), 
        col("WEAPON_DESC"),
        when((col("WEAPON_DESC").like("%HAND GUN%")) | (col("WEAPON_DESC").like("%PISTOL%")) | (col("WEAPON_DESC").like("%REVOLVER%")), lit("Handgun"))
        .when((col("WEAPON_DESC").like("%RIFLE%")) | (col("WEAPON_DESC").like("%SHOTGUN%")), lit("Long Gun"))
        .when((col("WEAPON_DESC").like("%KNIFE%")) | (col("WEAPON_DESC").like("%CUTTING%")), lit("Knife"))
        .when((col("WEAPON_DESC").like("%BODY%")) | (col("WEAPON_DESC").like("%HAND%")), lit("Physical Force"))
        .when(col("WEAPON_DESC").isNull(), lit("None")).otherwise(lit("Other")).alias("WEAPON_CATEGORY"),
        when((col("WEAPON_DESC").like("%HAND GUN%")) | (col("WEAPON_DESC").like("%RIFLE%")) | (col("WEAPON_DESC").like("%GUN%")), lit(True)).otherwise(lit(False)).alias("IS_FIREARM")
    )
    dim_weapon.write.mode("overwrite").save_as_table("GOLD.DIM_WEAPON")
    print(f"    {dim_weapon.count()} records")
    
    # ===== DIM_CASE_STATUS =====
    print("\n8. Creating DIM_CASE_STATUS...")
    status_distinct = silver_df.select(col("STATUS"), col("STATUS_DESC")).distinct().filter(col("STATUS").isNotNull())
    window_status = Window.order_by(col("STATUS"))
    
    dim_status = status_distinct.select(
        row_number().over(window_status).alias("STATUS_KEY"),
        col("STATUS"), 
        col("STATUS_DESC")
    )
    dim_status.write.mode("overwrite").save_as_table("GOLD.DIM_CASE_STATUS")
    print(f"    {dim_status.count()} records")
    
    # ===== FACT_CRIME_INCIDENT ===== (SIMPLIFIED WITH DR_NO AND DATE_RPT)
    print("\n9. Creating FACT_CRIME_INCIDENT (simplified with DR_NO and DATE_RPT)...")
    
    # Create a base fact table with unique row numbers first
    window_fact = Window.order_by(col("DR_NO"))
    fact_base = silver_df.select(
        row_number().over(window_fact).alias("CRIME_INCIDENT_KEY"),
        col("DR_NO"),
        col("DATE_OCC"),
        col("DATE_RPTD"),
        col("TIME_OCC"),
        col("AREA"),
        col("RPT_DIST_NO"),
        col("VICT_AGE"),
        col("VICT_SEX"),
        col("VICT_DESCENT"),
        col("PREMIS_CD"),
        col("WEAPON_USED_CD"),
        col("STATUS")
    )
    
    print(f"   Base fact records: {fact_base.count()}")
    
    # Load dimension tables
    dim_date_lookup = session.table("GOLD.DIM_DATE").select(
        col("DATE_KEY"), 
        col("FULL_DATE")
    )
    
    dim_time_lookup = session.table("GOLD.DIM_TIME").select(
        col("TIME_KEY"),
        col("TIME_OCC").alias("DIM_TIME_OCC")
    )
    
    dim_location_lookup = session.table("GOLD.DIM_LOCATION").select(
        col("LOCATION_KEY"), 
        col("AREA").alias("DIM_AREA"), 
        col("RPT_DIST_NO").alias("DIM_RPT_DIST_NO")
    )
    
    dim_victim_lookup = session.table("GOLD.DIM_VICTIM").select(
        col("VICTIM_KEY"), 
        col("VICT_AGE").alias("DIM_VICT_AGE"), 
        col("VICT_SEX").alias("DIM_VICT_SEX"), 
        col("VICT_DESCENT").alias("DIM_VICT_DESCENT")
    )
    
    dim_premise_lookup = session.table("GOLD.DIM_PREMISE").select(
        col("PREMISE_KEY"), 
        col("PREMIS_CD").alias("DIM_PREMIS_CD")
    )
    
    dim_weapon_lookup = session.table("GOLD.DIM_WEAPON").select(
        col("WEAPON_KEY"), 
        col("WEAPON_USED_CD").alias("DIM_WEAPON_USED_CD")
    )
    
    dim_status_lookup = session.table("GOLD.DIM_CASE_STATUS").select(
        col("STATUS_KEY"), 
        col("STATUS").alias("DIM_STATUS")
    )
    
    # Join with dimensions - DATE_OCC
    fact_with_date_occ = fact_base.join(
        dim_date_lookup,
        to_char(fact_base["DATE_OCC"], "YYYYMMDD").cast("integer") == dim_date_lookup["DATE_KEY"],
        "left"
    ).select(
        fact_base["CRIME_INCIDENT_KEY"],
        fact_base["DR_NO"],
        dim_date_lookup["DATE_KEY"].alias("date_occurred_key"),
        fact_base["DATE_RPTD"],
        fact_base["TIME_OCC"],
        fact_base["AREA"],
        fact_base["RPT_DIST_NO"],
        fact_base["VICT_AGE"],
        fact_base["VICT_SEX"],
        fact_base["VICT_DESCENT"],
        fact_base["PREMIS_CD"],
        fact_base["WEAPON_USED_CD"],
        fact_base["STATUS"]
    )
    
    print(f"   After date_occ join: {fact_with_date_occ.count()}")
    
    # Join with dimensions - DATE_RPTD
    fact_with_date_rpt = fact_with_date_occ.join(
        dim_date_lookup,
        to_char(fact_with_date_occ["DATE_RPTD"], "YYYYMMDD").cast("integer") == dim_date_lookup["DATE_KEY"],
        "left"
    ).select(
        fact_with_date_occ["CRIME_INCIDENT_KEY"],
        fact_with_date_occ["DR_NO"],
        fact_with_date_occ["date_occurred_key"],
        dim_date_lookup["DATE_KEY"].alias("date_reported_key"),
        fact_with_date_occ["TIME_OCC"],
        fact_with_date_occ["AREA"],
        fact_with_date_occ["RPT_DIST_NO"],
        fact_with_date_occ["VICT_AGE"],
        fact_with_date_occ["VICT_SEX"],
        fact_with_date_occ["VICT_DESCENT"],
        fact_with_date_occ["PREMIS_CD"],
        fact_with_date_occ["WEAPON_USED_CD"],
        fact_with_date_occ["STATUS"]
    )
    
    print(f"   After date_rpt join: {fact_with_date_rpt.count()}")
    
    fact_with_time = fact_with_date_rpt.join(
        dim_time_lookup,
        fact_with_date_rpt["TIME_OCC"] == dim_time_lookup["DIM_TIME_OCC"],
        "left"
    ).select(
        fact_with_date_rpt["CRIME_INCIDENT_KEY"],
        fact_with_date_rpt["DR_NO"],
        fact_with_date_rpt["date_occurred_key"],
        fact_with_date_rpt["date_reported_key"],
        dim_time_lookup["TIME_KEY"].alias("time_occurred_key"),
        fact_with_date_rpt["AREA"],
        fact_with_date_rpt["RPT_DIST_NO"],
        fact_with_date_rpt["VICT_AGE"],
        fact_with_date_rpt["VICT_SEX"],
        fact_with_date_rpt["VICT_DESCENT"],
        fact_with_date_rpt["PREMIS_CD"],
        fact_with_date_rpt["WEAPON_USED_CD"],
        fact_with_date_rpt["STATUS"]
    )
    
    print(f"   After time join: {fact_with_time.count()}")
    
    fact_with_location = fact_with_time.join(
        dim_location_lookup,
        (fact_with_time["AREA"] == dim_location_lookup["DIM_AREA"]) & 
        (fact_with_time["RPT_DIST_NO"] == dim_location_lookup["DIM_RPT_DIST_NO"]),
        "left"
    ).select(
        fact_with_time["CRIME_INCIDENT_KEY"],
        fact_with_time["DR_NO"],
        fact_with_time["date_occurred_key"],
        fact_with_time["date_reported_key"],
        fact_with_time["time_occurred_key"],
        dim_location_lookup["LOCATION_KEY"].alias("location_key"),
        fact_with_time["VICT_AGE"],
        fact_with_time["VICT_SEX"],
        fact_with_time["VICT_DESCENT"],
        fact_with_time["PREMIS_CD"],
        fact_with_time["WEAPON_USED_CD"],
        fact_with_time["STATUS"]
    )
    
    print(f"   After location join: {fact_with_location.count()}")
    
    fact_with_victim = fact_with_location.join(
        dim_victim_lookup,
        (fact_with_location["VICT_AGE"] == dim_victim_lookup["DIM_VICT_AGE"]) &
        (fact_with_location["VICT_SEX"] == dim_victim_lookup["DIM_VICT_SEX"]) &
        (fact_with_location["VICT_DESCENT"] == dim_victim_lookup["DIM_VICT_DESCENT"]),
        "left"
    ).select(
        fact_with_location["CRIME_INCIDENT_KEY"],
        fact_with_location["DR_NO"],
        fact_with_location["date_occurred_key"],
        fact_with_location["date_reported_key"],
        fact_with_location["time_occurred_key"],
        fact_with_location["location_key"],
        dim_victim_lookup["VICTIM_KEY"].alias("victim_key"),
        fact_with_location["PREMIS_CD"],
        fact_with_location["WEAPON_USED_CD"],
        fact_with_location["STATUS"]
    )
    
    print(f"   After victim join: {fact_with_victim.count()}")
    
    fact_with_premise = fact_with_victim.join(
        dim_premise_lookup,
        fact_with_victim["PREMIS_CD"] == dim_premise_lookup["DIM_PREMIS_CD"],
        "left"
    ).select(
        fact_with_victim["CRIME_INCIDENT_KEY"],
        fact_with_victim["DR_NO"],
        fact_with_victim["date_occurred_key"],
        fact_with_victim["date_reported_key"],
        fact_with_victim["time_occurred_key"],
        fact_with_victim["location_key"],
        fact_with_victim["victim_key"],
        dim_premise_lookup["PREMISE_KEY"].alias("premise_key"),
        fact_with_victim["WEAPON_USED_CD"],
        fact_with_victim["STATUS"]
    )
    
    print(f"   After premise join: {fact_with_premise.count()}")
    
    fact_with_weapon = fact_with_premise.join(
        dim_weapon_lookup,
        fact_with_premise["WEAPON_USED_CD"] == dim_weapon_lookup["DIM_WEAPON_USED_CD"],
        "left"
    ).select(
        fact_with_premise["CRIME_INCIDENT_KEY"],
        fact_with_premise["DR_NO"],
        fact_with_premise["date_occurred_key"],
        fact_with_premise["date_reported_key"],
        fact_with_premise["time_occurred_key"],
        fact_with_premise["location_key"],
        fact_with_premise["victim_key"],
        fact_with_premise["premise_key"],
        dim_weapon_lookup["WEAPON_KEY"].alias("weapon_key"),
        fact_with_premise["STATUS"]
    )
    
    print(f"   After weapon join: {fact_with_weapon.count()}")
    
    fact_final = fact_with_weapon.join(
        dim_status_lookup,
        fact_with_weapon["STATUS"] == dim_status_lookup["DIM_STATUS"],
        "left"
    ).select(
        fact_with_weapon["CRIME_INCIDENT_KEY"],
        fact_with_weapon["DR_NO"],
        fact_with_weapon["date_occurred_key"],
        fact_with_weapon["date_reported_key"],
        fact_with_weapon["time_occurred_key"],
        fact_with_weapon["location_key"],
        fact_with_weapon["victim_key"],
        fact_with_weapon["premise_key"],
        fact_with_weapon["weapon_key"],
        dim_status_lookup["STATUS_KEY"].alias("status_key")
    )
    
    fact_final.write.mode("overwrite").save_as_table("GOLD.FACT_CRIME_INCIDENT")
    print(f"    {fact_final.count()} records")
    
    # ===== BRIDGE TABLE =====
    print("\n10. Creating BRIDGE_CRIME_INCIDENT_CRIME_TYPE...")
    
    fact_with_crimes = silver_df.select(
        col("DR_NO"),
        col("CRM_CD"),
        col("CRM_CD_1"),
        col("CRM_CD_2"),
        col("CRM_CD_3"),
        col("CRM_CD_4")
    )
    
    window_bridge = Window.order_by(col("DR_NO"))
    fact_keys = fact_with_crimes.select(
        row_number().over(window_bridge).alias("CRIME_INCIDENT_KEY"),
        col("DR_NO"),
        col("CRM_CD").alias("FACT_CRM_CD"),
        col("CRM_CD_1").alias("FACT_CRM_CD_1"),
        col("CRM_CD_2").alias("FACT_CRM_CD_2"),
        col("CRM_CD_3").alias("FACT_CRM_CD_3"),
        col("CRM_CD_4").alias("FACT_CRM_CD_4")
    )
    
    crime_lookup = session.table("GOLD.DIM_CRIME_TYPE")
    
    # Bridge 1 - Primary crime
    bridge_1 = fact_keys.filter(col("FACT_CRM_CD").isNotNull()) \
        .join(crime_lookup, fact_keys["FACT_CRM_CD"] == crime_lookup["CRM_CD"]) \
        .select(
            fact_keys["CRIME_INCIDENT_KEY"],
            crime_lookup["CRIME_TYPE_KEY"],
            lit(1).alias("CRIME_SEQUENCE"),
            lit(True).alias("IS_PRIMARY")
        )
    
    # Bridge 2-5 - Additional crimes
    bridge_2 = fact_keys.filter(col("FACT_CRM_CD_1").isNotNull()) \
        .join(crime_lookup, fact_keys["FACT_CRM_CD_1"] == crime_lookup["CRM_CD"]) \
        .select(
            fact_keys["CRIME_INCIDENT_KEY"],
            crime_lookup["CRIME_TYPE_KEY"],
            lit(2).alias("CRIME_SEQUENCE"),
            lit(False).alias("IS_PRIMARY")
        )
    
    bridge_3 = fact_keys.filter(col("FACT_CRM_CD_2").isNotNull()) \
        .join(crime_lookup, fact_keys["FACT_CRM_CD_2"] == crime_lookup["CRM_CD"]) \
        .select(
            fact_keys["CRIME_INCIDENT_KEY"],
            crime_lookup["CRIME_TYPE_KEY"],
            lit(3).alias("CRIME_SEQUENCE"),
            lit(False).alias("IS_PRIMARY")
        )
    
    bridge_4 = fact_keys.filter(col("FACT_CRM_CD_3").isNotNull()) \
        .join(crime_lookup, fact_keys["FACT_CRM_CD_3"] == crime_lookup["CRM_CD"]) \
        .select(
            fact_keys["CRIME_INCIDENT_KEY"],
            crime_lookup["CRIME_TYPE_KEY"],
            lit(4).alias("CRIME_SEQUENCE"),
            lit(False).alias("IS_PRIMARY")
        )
    
    bridge_5 = fact_keys.filter(col("FACT_CRM_CD_4").isNotNull()) \
        .join(crime_lookup, fact_keys["FACT_CRM_CD_4"] == crime_lookup["CRM_CD"]) \
        .select(
            fact_keys["CRIME_INCIDENT_KEY"],
            crime_lookup["CRIME_TYPE_KEY"],
            lit(5).alias("CRIME_SEQUENCE"),
            lit(False).alias("IS_PRIMARY")
        )
    
    bridge = bridge_1.union(bridge_2).union(bridge_3).union(bridge_4).union(bridge_5)
    bridge.write.mode("overwrite").save_as_table("GOLD.BRIDGE_CRIME_INCIDENT_CRIME_TYPE")
    print(f"    {bridge.count()} records")
    
    return session.table("GOLD.FACT_CRIME_INCIDENT").limit(10)
