from pyspark import pipelines as dp

# Bronze Table 1: Claims
@dp.table(
    name="vehicle_insurance_data.bronze.bronze_claims",
    comment="Bronze layer: Raw claims data from S3"
)
def bronze_claims():
    """
    Ingest claims.csv and incremental claims data using Auto Loader.
    Auto Loader handles:
    - Initial load from claims.csv
    - Incremental load from incremental load folder
    - Schema inference and evolution
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", "*.csv")
        .load("s3://s3-vehicle-insurance/")
        .filter("_metadata.file_name LIKE '%claims%'")
    )


# Bronze Table 2: Customers
@dp.table(
    name="vehicle_insurance_data.bronze.bronze_customers",
    comment="Bronze layer: Raw customer data from S3"
)
def bronze_customers():
    """
    Ingest customrs.csv and incremental customer data.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", "*.csv")
        .load("s3://s3-vehicle-insurance/")
        .filter("_metadata.file_name LIKE '%customrs%' OR _metadata.file_name LIKE '%customers%'")
    )


# Bronze Table 3: First Notice of Loss (FNOL)
@dp.table(
    name="vehicle_insurance_data.bronze.bronze_fnol",
    comment="Bronze layer: Raw FNOL event data from S3"
)
def bronze_fnol():
    """
    Ingest fnol.csv and incremental FNOL data.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", "*.csv")
        .load("s3://s3-vehicle-insurance/")
        .filter("_metadata.file_name LIKE '%fnol%'")
    )


# Bronze Table 4: Payments
@dp.table(
    name="vehicle_insurance_data.bronze.bronze_payments",
    comment="Bronze layer: Raw payment data from S3"
)
def bronze_payments():
    """
    Ingest paymnts.csv and incremental payment data.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", "*.csv")
        .load("s3://s3-vehicle-insurance/")
        .filter("_metadata.file_name LIKE '%paymnts%' OR _metadata.file_name LIKE '%payments%'")
    )


# Bronze Table 5: Policies
@dp.table(
    name="vehicle_insurance_data.bronze.bronze_policies",
    comment="Bronze layer: Raw policy data from S3"
)
def bronze_policies():
    """
    Ingest policies.csv and incremental policy data.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", "*.csv")
        .load("s3://s3-vehicle-insurance/")
        .filter("_metadata.file_name LIKE '%policies%'")
    )


# Bronze Table 6: Vehicles
@dp.table(
    name="vehicle_insurance_data.bronze.bronze_vehicles",
    comment="Bronze layer: Raw vehicle data from S3"
)
def bronze_vehicles():
    """
    Ingest vehiclees.csv and incremental vehicle data.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("pathGlobFilter", "*.csv")
        .load("s3://s3-vehicle-insurance/")
        .filter("_metadata.file_name LIKE '%vehiclees%' OR _metadata.file_name LIKE '%vehicles%'")
    )
