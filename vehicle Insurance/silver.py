from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Silver Table 1: Claims Cleaned
@dp.table(
    name="vehicle_insurance_data.silver.silver_claims",
    comment="Silver layer: Cleaned claims data with fillna for optional fields"
)
@dp.expect_all({
    "valid_claim_id": "claim_id IS NOT NULL",
    "valid_policy_id": "policy_id IS NOT NULL",
    "valid_claim_amount": "claim_amount_clean >= 0"
})
def silver_claims():
    """
    Clean and transform claims data from bronze_claims.
    Uses fillna to handle missing optional fields:
    - claim_status: defaults to "UNKNOWN"
    - claim_amount_clean: defaults to 0 if null
    Deduplicates on claim_id.
    """
    return (
        spark.readStream.table("vehicle_insurance_data.bronze.bronze_claims")
        .filter("claim_id IS NOT NULL AND policy_id IS NOT NULL")
        .dropDuplicates(["claim_id"])
        .withColumn("claim_date_clean", F.to_date(F.col("claim_date")))
        .withColumn("claim_amount_clean", F.coalesce(F.col("claim_amount").cast("decimal(12,2)"), F.lit(0).cast("decimal(12,2)")))
        .withColumn("claim_status", F.coalesce(F.col("claim_status"), F.lit("UNKNOWN")))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .select(
            "claim_id", "policy_id", "claim_amount_clean", "claim_status", 
            "claim_date_clean", "ingestion_timestamp"
        )
    )


# Silver Table 2: Customers Cleaned
@dp.table(
    name="vehicle_insurance_data.silver.silver_customers",
    comment="Silver layer: Cleaned customer data with fillna for optional fields"
)
@dp.expect_all({
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_name": "name_clean IS NOT NULL"
})
def silver_customers():
    """
    Clean and transform customer data from bronze_customers.
    Uses fillna to handle missing optional fields:
    - name_clean: defaults to "UNKNOWN"
    - city_clean: defaults to "UNKNOWN"
    - state_clean: defaults to "UNKNOWN"
    - risk_category: defaults to "MEDIUM"
    Deduplicates on customer_id.
    """
    return (
        spark.readStream.table("vehicle_insurance_data.bronze.bronze_customers")
        .filter("customer_id IS NOT NULL")
        .dropDuplicates(["customer_id"])
        .withColumn("name_clean", F.coalesce(F.trim(F.col("name")), F.lit("UNKNOWN")))
        .withColumn("city_clean", F.coalesce(F.trim(F.col("city")), F.lit("UNKNOWN")))
        .withColumn("state_clean", F.coalesce(F.upper(F.trim(F.col("state"))), F.lit("UNKNOWN")))
        .withColumn("risk_category", F.coalesce(F.col("risk_category"), F.lit("MEDIUM")))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .select(
            "customer_id", "name_clean", "city_clean", "state_clean", 
            "risk_category", "ingestion_timestamp"
        )
    )


# Silver Table 3: FNOL Events Cleaned
@dp.table(
    name="vehicle_insurance_data.silver.silver_fnol",
    comment="Silver layer: Cleaned FNOL event data with fillna for optional fields"
)
@dp.expect_all({
    "valid_event_id": "event_id IS NOT NULL",
    "valid_event_time": "event_time_clean IS NOT NULL"
})
def silver_fnol():
    """
    Clean and transform FNOL event data from bronze_fnol.
    Uses fillna to handle missing optional fields:
    - event_type: defaults to "UNKNOWN"
    - description: defaults to "NO DESCRIPTION"
    Deduplicates on event_id.
    """
    return (
        spark.readStream.table("vehicle_insurance_data.bronze.bronze_fnol")
        .filter("event_id IS NOT NULL")
        .dropDuplicates(["event_id"])
        .withColumn("event_time_clean", F.to_timestamp(F.col("event_time"), "dd-MM-yyyy HH:mm"))
        .withColumn("event_type", F.coalesce(F.col("event_type"), F.lit("UNKNOWN")))
        .withColumn("description", F.coalesce(F.col("description"), F.lit("NO DESCRIPTION")))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .select(
            "event_id", "event_time_clean", "event_type", "description", 
            "ingestion_timestamp"
        )
    )


# Silver Table 4: Payments Cleaned
@dp.table(
    name="vehicle_insurance_data.silver.silver_payments",
    comment="Silver layer: Cleaned payment data with amount defaults to 0 if null"
)
@dp.expect_all({
    "valid_payment_id": "payment_id IS NOT NULL",
    "valid_amount": "amount_clean >= 0"
})
def silver_payments():
    """
    Clean and transform payment data from bronze_payments.
    Note: amount_clean defaults to 0 if null.
    Deduplicates on payment_id.
    """
    return (
        spark.readStream.table("vehicle_insurance_data.bronze.bronze_payments")
        .filter("payment_id IS NOT NULL")
        .dropDuplicates(["payment_id"])
        .withColumn("amount_clean", F.coalesce(F.col("amount").cast("decimal(10,2)"), F.lit(0).cast("decimal(10,2)")))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .select(
            "payment_id", "amount_clean", "ingestion_timestamp"
        )
    )


# Silver Table 5: Policies Cleaned
@dp.table(
    name="vehicle_insurance_data.silver.silver_policies",
    comment="Silver layer: Cleaned policy data with fillna for optional fields"
)
@dp.expect_all({
    "valid_policy_id": "policy_id IS NOT NULL",
    "valid_customer_id": "customer_id IS NOT NULL",
    "valid_premium": "premium_amount_clean >= 0"
})
def silver_policies():
    """
    Clean and transform policy data from bronze_policies.
    Uses fillna to handle missing optional fields:
    - status: defaults to "UNKNOWN"
    - premium_amount_clean: defaults to 0 if null
    Deduplicates on policy_id.
    """
    return (
        spark.readStream.table("vehicle_insurance_data.bronze.bronze_policies")
        .filter("policy_id IS NOT NULL AND customer_id IS NOT NULL")
        .dropDuplicates(["policy_id"])
        .withColumn("premium_amount_clean", F.coalesce(F.col("premium_amount").cast("decimal(10,2)"), F.lit(0).cast("decimal(10,2)")))
        .withColumn("status", F.coalesce(F.col("status"), F.lit("UNKNOWN")))
        .withColumn("last_updated_clean", F.to_timestamp(F.col("last_updated")))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .select(
            "policy_id", "customer_id", "premium_amount_clean", "status", 
            "last_updated_clean", "ingestion_timestamp"
        )
    )


# Silver Table 6: Vehicles Cleaned
@dp.table(
    name="vehicle_insurance_data.silver.silver_vehicles",
    comment="Silver layer: Cleaned vehicle data with fillna for optional fields"
)
@dp.expect_all({
    "valid_vehicle_id": "vehicle_id IS NOT NULL",
    "valid_year": "year_clean IS NULL OR (year_clean >= 1900 AND year_clean <= 2030)"
})
def silver_vehicles():
    """
    Clean and transform vehicle data from bronze_vehicles.
    Uses fillna to handle missing optional fields:
    - brand_clean: defaults to "UNKNOWN"
    - model_clean: defaults to "UNKNOWN"
    - vehicle_type: defaults to "OTHER"
    - year_clean: defaults to 0 (indicates missing year)
    Deduplicates on vehicle_id.
    """
    return (
        spark.readStream.table("vehicle_insurance_data.bronze.bronze_vehicles")
        .filter("vehicle_id IS NOT NULL")
        .dropDuplicates(["vehicle_id"])
        .withColumn("year_clean", F.coalesce(F.col("year").cast("int"), F.lit(0)))
        .withColumn("brand_clean", F.coalesce(F.trim(F.col("brand")), F.lit("UNKNOWN")))
        .withColumn("model_clean", F.coalesce(F.trim(F.col("model")), F.lit("UNKNOWN")))
        .withColumn("vehicle_type", F.coalesce(F.col("vehicle_type"), F.lit("OTHER")))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .select(
            "vehicle_id", "vehicle_type", "brand_clean", "model_clean", 
            "year_clean", "ingestion_timestamp"
        )
    )
