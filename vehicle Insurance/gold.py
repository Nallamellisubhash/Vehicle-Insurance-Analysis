from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# =============================================================================
# GOLD DIMENSION TABLES - Streaming Pass-Through from Silver
# =============================================================================

# Dimension: Customers
@dp.table(
    name="vehicle_insurance_data.gold.dim_customer",
    comment="Customer dimension - streaming pass-through from silver",
    cluster_by=["customer_id"]
)
def dim_customer():
    """
    Customer dimension table - streams directly from silver layer.
    """
    return spark.readStream.option("ignoreDeletes", "true").table("vehicle_insurance_data.silver.silver_customers")


# Dimension: Policies
@dp.table(
    name="vehicle_insurance_data.gold.dim_policy",
    comment="Policy dimension - streaming pass-through from silver",
    cluster_by=["policy_id"]
)
def dim_policy():
    """
    Policy dimension table - streams directly from silver layer.
    """
    return spark.readStream.option("ignoreDeletes", "true").table("vehicle_insurance_data.silver.silver_policies")


# Dimension: Vehicles
@dp.table(
    name="vehicle_insurance_data.gold.dim_vehicle",
    comment="Vehicle dimension - streaming pass-through from silver",
    cluster_by=["vehicle_id"]
)
def dim_vehicle():
    """
    Vehicle dimension table - streams directly from silver layer.
    """
    return spark.readStream.option("ignoreDeletes", "true").table("vehicle_insurance_data.silver.silver_vehicles")


# =============================================================================
# GOLD FACT TABLE - Hybrid Stream + Batch
# =============================================================================

@dp.table(
    name="vehicle_insurance_data.gold.fact_claims",
    comment="Fact table for claims with enriched dimensions - hybrid streaming",
    cluster_by=["claim_status", "claim_year"]
)
def fact_claims():
    """
    Claims fact table with enriched customer and policy information.
    - Claims: streaming (incremental)
    - Policies/Customers: batch (lookup for joins)
    - Time dimensions: year, month, quarter, dayofweek
    """
    # Stream claims from silver
    claims = spark.readStream.option("ignoreDeletes", "true").table("vehicle_insurance_data.silver.silver_claims")
    
    # Batch read policies and customers for efficient joins
    policies = spark.read.table("vehicle_insurance_data.silver.silver_policies")
    customers = spark.read.table("vehicle_insurance_data.silver.silver_customers")
    
    # Join claims with policies
    claims_policies = claims.join(
        policies,
        claims["policy_id"] == policies["policy_id"],
        "left"
    )
    
    # Join with customers
    enriched = claims_policies.join(
        customers,
        claims_policies["customer_id"] == customers["customer_id"],
        "left"
    )
    
    # Add time dimensions
    return (
        enriched
        .select(
            # Claim fields
            claims["claim_id"],
            claims["policy_id"],
            claims_policies["customer_id"],
            F.col("claim_amount_clean").alias("claim_amount"),
            F.col("claim_status"),
            F.col("claim_date_clean").alias("claim_date"),
            # Policy fields
            F.col("premium_amount_clean").alias("premium_amount"),
            policies["status"].alias("policy_status"),
            # Customer fields
            F.col("risk_category"),
            F.col("name_clean").alias("customer_name"),
            F.col("city_clean").alias("city"),
            F.col("state_clean").alias("customer_state"),
            # Time dimensions
            F.year("claim_date_clean").alias("claim_year"),
            F.month("claim_date_clean").alias("claim_month"),
            F.quarter("claim_date_clean").alias("claim_quarter"),
            F.dayofweek("claim_date_clean").alias("claim_dayofweek")
        )
    )


# =============================================================================
# GOLD BASE MV - Foundation for KPIs
# =============================================================================

@dp.materialized_view(
    name="vehicle_insurance_data.gold.gold_base_mv",
    comment="Foundation table for all KPIs - extends fact_claims",
    cluster_by=["claim_status"]
)
def gold_base_mv():
    """
    Foundation table for all KPIs - reads from fact_claims.
    Note: Vehicles table cannot be joined (no link to claims/policies/customers).
    Note: Payments and FNOL tables don't have claim_id, so they cannot be joined here.
    """
    return spark.read.table("vehicle_insurance_data.gold.fact_claims")


# =============================================================================
# NEW KPIs - Materialized Views (9 KPIs for Dashboard)
# Note: 4 KPIs removed due to data model limitations:
# - kpi_claims_by_vehicle_type_mv (vehicles have no link to claims)
# - avg_payment_per_claim_kpi_mv (needs claim_id in payments)
# - multiple_fnol_mv (needs claim_id in fnol)
# - fnol_conversion_kpi_mv (needs claim_id in fnol)
# =============================================================================

# KPI 1: Claims by Status 
@dp.materialized_view(
    name="vehicle_insurance_data.gold.kpi_claims_by_status_mv",
    comment="KPI: Claims metrics by status - total amounts, counts, averages",
    cluster_by=["claim_status"]
)
def kpi_claims_by_status_mv():
    """
    Claims analysis by status.
    Widgets: total_claim_amount, total_claims, avg_claim_amount, avg_claim_amount_rounded
    """
    return (
        spark.read.table("vehicle_insurance_data.gold.fact_claims")
        .groupBy("claim_status")
        .agg(
            F.sum("claim_amount").alias("total_claim_amount"),
            F.count("claim_id").alias("total_claims"),
            F.avg("claim_amount").alias("avg_claim_amount"),
            F.round(F.avg("claim_amount"), 2).alias("avg_claim_amount_rounded")
        )
        .orderBy(F.col("total_claim_amount").desc())
    )


# KPI 2: Customers by State 
@dp.materialized_view(
    name="vehicle_insurance_data.gold.kpi_customers_by_state_mv",
    comment="KPI: Customer distribution and risk categories by state",
    cluster_by=["customer_state"]
)
def kpi_customers_by_state_mv():
    """
    Customer analysis by state with risk category breakdown.
    Widgets: total_customers, high_risk, medium_risk, low_risk, state distribution
    """
    return (
        spark.read.table("vehicle_insurance_data.gold.fact_claims")
        .select("customer_id", "customer_state", "risk_category")
        .dropDuplicates(["customer_id"])
        .groupBy("customer_state")
        .agg(
            F.count("customer_id").alias("total_customers"),
            F.sum(F.when(F.col("risk_category") == "HIGH", 1).otherwise(0)).alias("high_risk_customers"),
            F.sum(F.when(F.col("risk_category") == "MEDIUM", 1).otherwise(0)).alias("medium_risk_customers"),
            F.sum(F.when(F.col("risk_category") == "LOW", 1).otherwise(0)).alias("low_risk_customers")
        )
        .orderBy(F.col("total_customers").desc())
    )


# KPI 3: Policy Status Summary (1 widget)
@dp.materialized_view(
    name="vehicle_insurance_data.gold.kpi_policy_status_summary_mv",
    comment="KPI: Policy counts and premium by status",
    cluster_by=["policy_status"]
)
def kpi_policy_status_summary_mv():
    """
    Policy status summary with premium aggregates.
    Widgets: total_policies by status
    """
    return (
        spark.read.table("vehicle_insurance_data.gold.fact_claims")
        .select("policy_id", "policy_status", "premium_amount")
        .dropDuplicates(["policy_id"])
        .groupBy("policy_status")
        .agg(
            F.count("policy_id").alias("total_policies"),
            F.sum("premium_amount").alias("total_premium"),
            F.avg("premium_amount").alias("avg_premium"),
            F.round(F.avg("premium_amount"), 2).alias("avg_premium_rounded")
        )
        .orderBy(F.col("total_policies").desc())
    )


# KPI 4: Premium by Risk Category (2 widgets)
@dp.materialized_view(
    name="vehicle_insurance_data.gold.kpi_premium_by_risk_category_mv",
    comment="KPI: Premium analysis by customer risk category",
    cluster_by=["risk_category"]
)
def kpi_premium_by_risk_category_mv():
    """
    Premium aggregates by risk category.
    Widgets: total_premium, total_policies
    """
    return (
        spark.read.table("vehicle_insurance_data.gold.fact_claims")
        .select("policy_id", "risk_category", "premium_amount")
        .dropDuplicates(["policy_id"])
        .groupBy("risk_category")
        .agg(
            F.sum("premium_amount").alias("total_premium"),
            F.count("policy_id").alias("total_policies"),
            F.avg("premium_amount").alias("avg_premium"),
            F.round(F.avg("premium_amount"), 2).alias("avg_premium_rounded")
        )
        .orderBy(F.col("total_premium").desc())
    )


# KPI 5: High Claim Outliers (3 widgets)
@dp.materialized_view(
    name="vehicle_insurance_data.gold.high_claim_outliers_mv",
    comment="KPI: High claim outliers with z-score analysis",
    cluster_by=["outlier_category"]
)
def high_claim_outliers_mv():
    """
    Identifies claim outliers using z-score statistical analysis.
    Widgets: outlier_category distribution, z_score, claim amounts
    """
    gold_clean = spark.read.table("vehicle_insurance_data.gold.gold_base_mv").filter("claim_amount IS NOT NULL")
    
    # Calculate mean and standard deviation
    stats_df = gold_clean.agg(
        F.avg("claim_amount").alias("mean"),
        F.stddev("claim_amount").alias("stddev")
    )
    
    return (
        gold_clean
        .crossJoin(stats_df)
        .withColumn("z_score", (F.col("claim_amount") - F.col("mean")) / F.col("stddev"))
        .withColumn(
            "outlier_category",
            F.when(F.col("z_score") > 3, "high")
            .when(F.col("z_score") > 2, "medium")
            .otherwise("low")
        )
        .select("claim_id", "policy_id", "claim_amount", "customer_id", 
               "z_score", "outlier_category")
    )


# KPI 6: Repeat Claims (1 widget)
@dp.materialized_view(
    name="vehicle_insurance_data.gold.repeat_claims_mv",
    comment="KPI: Customers with multiple claims",
    cluster_by=["customer_id"]
)
def repeat_claims_mv():
    """
    Identifies customers with multiple claims.
    Widgets: claim_count
    """
    return (
        spark.read.table("vehicle_insurance_data.gold.fact_claims")
        .groupBy("customer_id")
        .agg(F.count("claim_id").alias("claim_count"))
        .filter(F.col("claim_count") > 1)
        .orderBy(F.col("claim_count").desc())
    )


# KPI 7: Policy Claim Ratio (2 widgets)
@dp.materialized_view(
    name="vehicle_insurance_data.gold.policy_kpi_mv",
    comment="KPI: Policy claim ratio - total claims vs premium by policy",
    cluster_by=["policy_id"]
)
def policy_kpi_mv():
    """
    Policy-level claim to premium ratio.
    Widgets: policy_claim_ratio, total_claim_amount
    """
    return (
        spark.read.table("vehicle_insurance_data.gold.gold_base_mv")
        .groupBy("policy_id")
        .agg(
            F.sum("claim_amount").alias("total_claim_amount"),
            F.first("premium_amount").alias("premium_amount")
        )
        .withColumn(
            "policy_claim_ratio",
            F.when(F.col("premium_amount") > 0, 
                   F.round(F.col("total_claim_amount") / F.col("premium_amount"), 4))
            .otherwise(0)
        )
        .orderBy(F.col("policy_claim_ratio").desc())
    )


# KPI 8: Settlement Rate
@dp.materialized_view(
    name="vehicle_insurance_data.gold.settlement_rate_kpi_mv",
    comment="KPI: Claims settlement rate percentage",
    cluster_by=["metric"]
)
def settlement_rate_kpi_mv():
    """
    Settlement rate calculation.
    Widgets: settlement_rate_percentage
    """
    claims = spark.read.table("vehicle_insurance_data.gold.fact_claims")
    
    total = claims.count()
    settled = claims.filter(F.col("claim_status") == "SETTLED").count()
    
    return spark.createDataFrame([
        ("Total Claims", float(total)),
        ("Settled Claims", float(settled)),
        ("Settlement Rate %", float(round((settled / total * 100) if total > 0 else 0, 2)))
    ], ["metric", "value"])


# KPI 9: High Risk Customers
@dp.materialized_view(
    name="vehicle_insurance_data.gold.high_risk_customers_kpi_mv",
    comment="KPI: High-risk customers with claim analysis",
    cluster_by=["customer_id"]
)
def high_risk_customers_kpi_mv():
    """
    Customers with high risk category and their claim summary.
    Widgets: total_claims, total_claim_amount, customer details
    """
    return (
        spark.read.table("vehicle_insurance_data.gold.fact_claims")
        .filter(F.col("risk_category") == "HIGH")
        .groupBy("customer_id", "customer_name", "customer_state", "risk_category")
        .agg(
            F.count("claim_id").alias("total_claims"),
            F.sum("claim_amount").alias("total_claim_amount"),
            F.avg("claim_amount").alias("avg_claim_amount"),
            F.round(F.avg("claim_amount"), 2).alias("avg_claim_amount_rounded")
        )
        .orderBy(F.col("total_claim_amount").desc())
    )


# =============================================================================
# GOLD HISTORY TABLE - SCD Type 2 with Auto CDC
# =============================================================================

# Step 1: Create target streaming table
dp.create_streaming_table(
    name="vehicle_insurance_data.gold.gold_customer_history_scd2",
    comment="SCD Type 2 table tracking customer history changes"
)

# Step 2: Define Auto CDC flow
dp.create_auto_cdc_flow(
    source="vehicle_insurance_data.silver.silver_customers",
    target="vehicle_insurance_data.gold.gold_customer_history_scd2",
    keys=["customer_id"],
    sequence_by="ingestion_timestamp",
    stored_as_scd_type="2",
    track_history_column_list=["name_clean", "city_clean", "state_clean", "risk_category"]
)


# =============================================================================
# GOLD MASKED TABLE - Column Masking with Python Helpers
# =============================================================================

def mask_pii_column(col_name):
    """
    Mask PII by showing first character + '***'.
    Example: 'John Doe' -> 'J***'
    """
    return F.when(
        F.col(col_name).isNotNull(),
        F.concat(
            F.substring(F.col(col_name), 1, 1),
            F.lit("***")
        )
    ).otherwise(F.col(col_name))


def mask_pii_column_proportional(col_name):
    """
    Mask PII proportional to original length.
    Example: 'John Doe' -> 'J*******' (first char + asterisks for remaining)
    """
    return F.when(
        F.col(col_name).isNotNull(),
        F.concat(
            F.substring(F.col(col_name), 1, 1),
            F.expr(f"repeat('*', length({col_name}) - 1)")
        )
    ).otherwise(F.col(col_name))


def mask_pii_with_group_check(col_name):
    """
    Mask PII unless user is in account_group.
    Uses is_account_group_member() function.
    """
    return F.when(
        F.expr("is_account_group_member('account_users')"),
        F.col(col_name)
    ).otherwise(mask_pii_column_proportional(col_name))


@dp.materialized_view(
    name="vehicle_insurance_data.gold.gold_customer_masked",
    comment="Customer data with PII masking applied to name and city"
)
def gold_customer_masked():
    """
    Customer table with masked PII fields.
    Applies proportional masking to customer_name and city.
    """
    customers = spark.read.table("vehicle_insurance_data.gold.dim_customer")
    
    return (
        customers
        .withColumn("customer_name", mask_pii_column_proportional("name_clean"))
        .withColumn("city", mask_pii_column_proportional("city_clean"))
        .select(
            "customer_id",
            "customer_name",
            "city",
            F.col("state_clean").alias("state"),
            "risk_category",
            "ingestion_timestamp"
        )
    )
