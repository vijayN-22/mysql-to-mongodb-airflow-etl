from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat, rand, floor, expr
from pyspark.sql.types import StringType

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("DataGenerator") \
    .config("spark.jars.packages",
            "mysql:mysql-connector-java:8.0.33") \
    .getOrCreate()

N = 100000  # 1 lakh

policy_df = spark.range(N) \
    .withColumn("policy_no", col("id").cast("int")) \
    .withColumn("owning_location",
                expr("CASE WHEN id % 4 = 0 THEN 'CHN' "
                     "WHEN id % 4 = 1 THEN 'BLR' "
                     "WHEN id % 4 = 2 THEN 'DEL' "
                     "ELSE 'MUM' END")) \
    .withColumn("policy_type",
                expr("CASE WHEN id % 2 = 0 THEN 'ULIP' ELSE 'TERM' END")) \
    .withColumn("issue_date", expr("date('2022-01-01')")) \
    .withColumn("maturity_date", expr("date('2042-01-01')")) \
    .withColumn("sum_assured", (rand()*1000000).cast("int")) \
    .withColumn("policy_type", lit("Active")) \
    .drop("id")

policy_df.show(5)

from pyspark.sql.functions import explode, array

nominee_df = policy_df \
    .withColumn("numNominees", (floor(rand()*2) + 1)) \
    .withColumn("nomineeIndex",
                explode(expr("sequence(1, numNominees)"))) \
    .select(
        "policy_no",
        "owning_location",
        lit("Spouse").alias("nominee_name"),
        lit("Wife").alias("relationship"),
        (floor(rand()*60) + 18).alias("nominee_age"),
        col("nomineeIndex").alias("key_tail")
    )

nominee_df.show(5)

rider_df = policy_df \
    .withColumn("numRiders", (floor(rand()*3) + 1)) \
    .withColumn("riderIndex",
                explode(expr("sequence(1, numRiders)"))) \
    .select(
        "policy_no",
        "owning_location",
        col("riderIndex").alias("key_tail"),
        expr("CASE WHEN riderIndex % 2 = 0 "
             "THEN 'Accidental Death' "
             "ELSE 'Critical Illness' END").alias("rider_type"),
        (rand()*500000).cast("int").alias("rider_sum_assured"),
        (rand()*5000).cast("int").alias("rider_premium"),
    )

rider_df.show(5)

bank_df = policy_df.select(
    "policy_no",
    "owning_location",
    lit("HDFC").alias("bank_name"),
    concat(lit("AC"), col("policy_no")).alias("account_number"),
    lit("HDFC0001234").alias("ifsc_code"),
    lit("PolicyHolder").alias("account_holder_name")
)

customer_df = policy_df.select(
    "policy_no",
    "owning_location"
).withColumn(
    "customer_name",
    concat(lit("Customer_"), col("policy_no"))
).withColumn(
    "gender",
    expr("CASE WHEN policy_no % 2 = 0 THEN 'Male' ELSE 'Female' END")
).withColumn(
    "age",
    (floor(rand()*42) + 18)  # Age between 18–60
).withColumn(
    "dob", expr("date('2022-01-01')")
    )

annuity_df = policy_df.select(
    "policy_no",
    "owning_location",
    lit("Monthly").alias("annuity_frequency"),
    lit("Fixed").alias("annuity_type"),
    (rand()*20000).cast("int").alias("annuity_amount")
)

jdbc_url = "jdbc:mysql://localhost:3306/LIC"

properties = {
    "user": "root",
    "host": "localhost",
    "password": "22jan1998",
    "driver": "com.mysql.cj.jdbc.Driver"
}

policy_df.write.jdbc(jdbc_url, "pmo02000", mode="append", properties=properties)
nominee_df.write.jdbc(jdbc_url, "pmc04000", mode="append", properties=properties)
rider_df.write.jdbc(jdbc_url, "pmc05000", mode="append", properties=properties)
bank_df.write.jdbc(jdbc_url, "pmc07000", mode="append", properties=properties)
annuity_df.write.jdbc(jdbc_url, "pmc06000", mode="append", properties=properties)
# customer_df.write.jdbc(jdbc_url, "pmc03000", mode="append", properties=properties)