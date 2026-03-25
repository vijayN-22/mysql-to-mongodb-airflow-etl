from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_set, struct


from config.config_loader import load_config


def create_spark_session():

    config = load_config()
    mongo_conf = config["mongo"]

    if mongo_conf.get("username"):
        connection_uri = (
            f"mongodb://{mongo_conf['username']}:{mongo_conf['password']}"
            f"@{mongo_conf['host']}:{mongo_conf['port']}"
        )
    else:
        connection_uri = (
            f"mongodb://{mongo_conf['host']}:{mongo_conf['port']}"
        )

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("PolicyMigration") \
        .config("spark.jars.packages",
                "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,"
                "mysql:mysql-connector-java:8.0.33") \
        .config("spark.mongodb.write.connection.uri", connection_uri) \
        .config("spark.mongodb.write.database", mongo_conf["database"]) \
        .config("spark.mongodb.write.collection", mongo_conf["collection"]) \
        .getOrCreate()

    return spark
def read_from_mysql(spark):

    config = load_config()
    mysql_conf = config["mysql"]

    jdbc_url = f"jdbc:mysql://{mysql_conf['host']}:{mysql_conf['port']}/{mysql_conf['database']}"

    properties = {
        "user": mysql_conf["user"],
        "password": mysql_conf["password"],
        "driver": "com.mysql.cj.jdbc.Driver"
    }

    query = """
        (SELECT p.policy_no as policyNo,
                p.owning_location as owningLocation,
                p.policy_type as policyType,
                p.issue_date as issueDate,
                p.maturity_date as maturityDate,
                p.sum_assured as sumAssured,
                n.nominee_name as nomineeName,
                n.relationship as relationship,
                n.nominee_age as nomineeAge,
                n.key_tail as nkeyTail,
                r.key_tail as rkeyTail,
                r.rider_type as riderType,
                r.rider_sum_assured as riderSumAssured,
                r.rider_premium as riderPremium,
                p6.annuity_frequency as annuityFrequency,
                p6.annuity_type as annuityType,
                p6.annuity_amount as annuityAmount,
                b.bank_name as bankName,
                b.account_number as accountNumber,
                b.ifsc_code as ifscCode,
                b.account_holder_name as accountHolderName
         FROM pmo02000 p
         LEFT JOIN pmc04000 n ON p.policy_no = n.policy_no and p.owning_location = n.owning_location
         LEFT JOIN pmc05000 r ON p.policy_no = r.policy_no and p.owning_location = r.owning_location
         LEFT JOIN pmc06000 p6 ON p.policy_no = p6.policy_no and p.owning_location = p6.owning_location
         LEFT JOIN pmc07000 b ON p.policy_no = b.policy_no and p.owning_location = b.owning_location
        ) as policy_data
    """

    df = spark.read.jdbc(
        url=jdbc_url,
        table=query,
        column="policyNo",   # partition column
        lowerBound=1,
        upperBound=100000,
        numPartitions=8,
        properties=properties
    )

    return df

def transform(df):

    transformed_df = df.groupBy(
        "policyNo",
        "owningLocation",
        "policyType",
        "issueDate",
        "maturityDate",
        "sumAssured",
        "bankName",
        "accountNumber",
        "ifscCode",
        "accountHolderName",
        "annuityFrequency",
        "annuityType",
        "annuityAmount"
    ).agg(
        collect_set(
            struct(
                col("nomineeName"),
                col("relationship"),
                col("nomineeAge"),
                col("nkeyTail").alias("keyTail")
            )
        ).alias("nomineeDetails"),

        collect_set(
            struct(
                col("rkeyTail").alias("riderKeyTail"),
                col("riderType"),
                col("riderSumAssured"),
                col("riderPremium")
            )
        ).alias("riderDetails")
    )

    transformed_df = transformed_df \
        .withColumn(
            "neftDetails",
            struct(
                col("bankName"),
                col("accountNumber"),
                col("ifscCode"),
                col("accountHolderName")
            )
        ) \
        .withColumn(
            "annuityDetails",
            struct(
                col("annuityFrequency"),
                col("annuityType"),
                col("annuityAmount")
            )
        ) \
        .drop(
            "bankName",
            "accountNumber",
            "ifscCode",
            "accountHolderName",
            "annuityFrequency",
            "annuityType",
            "annuityAmount"
        )

    return transformed_df

def write_to_mongo(df):

    df.write \
      .format("mongodb") \
      .option("operationType", "update") \
      .option("upsertDocument", "true") \
      .option("idFieldList", "policyNo,owningLocation") \
      .mode("append") \
      .save()
    
if __name__ == "__main__":

    spark = create_spark_session()
    print(spark.version)
    df = read_from_mysql(spark)

    transformed_df = transform(df)

    write_to_mongo(transformed_df)
    spark.stop()
