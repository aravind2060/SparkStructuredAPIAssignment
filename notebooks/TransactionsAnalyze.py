import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, sum, avg
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType

def create_spark_session(master_node_url):
    return SparkSession.builder.appName('ECommerceAnalysis').master(master_node_url).getOrCreate()

def read_data(spark, file_path, schema):
    return spark.read.csv(file_path, schema=schema, header=True)

def define_schema():
    return StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("purchase_amount", DoubleType(), True),
        StructField("product_category", StringType(), True),
        StructField("transaction_date", TimestampType(), True)
    ])

def perform_basic_analysis(df):
    distinct_customers = df.select(countDistinct("customer_id"))
    total_revenue = df.select(sum("purchase_amount"))
    average_purchase = df.select(avg("purchase_amount"))
    return distinct_customers, total_revenue, average_purchase

def perform_advanced_analysis(df):
    popular_categories = df.groupBy("product_category").count().orderBy('count', ascending=False)
    high_value_customers = df.groupBy("customer_id").sum("purchase_amount").orderBy('sum(purchase_amount)', ascending=False)
    low_value_customers = df.groupBy("customer_id").sum("purchase_amount").orderBy('sum(purchase_amount)', ascending=True)
    return popular_categories, high_value_customers, low_value_customers

def write_to_file(df, output_path):
    df.coalesce(1).write.option("header", True).csv(output_path)

def main(master_nodel_url,input_file_path, output_dir):
    spark = create_spark_session(master_node_url)
    schema = define_schema()
    df = read_data(spark, input_file_path, schema)
    df.printSchema()

    distinct_customers, total_revenue, average_purchase = perform_basic_analysis(df)
    popular_categories, high_value_customers, low_value_customers = perform_advanced_analysis(df)

    os.makedirs(output_dir, exist_ok=True)
    
    write_to_file(distinct_customers, os.path.join(output_dir, 'distinct_customers'))
    write_to_file(total_revenue, os.path.join(output_dir, 'total_revenue'))
    write_to_file(average_purchase, os.path.join(output_dir, 'average_purchase'))
    write_to_file(popular_categories, os.path.join(output_dir, 'popular_categories'))
    write_to_file(high_value_customers, os.path.join(output_dir, 'high_value_customers'))
    write_to_file(low_value_customers, os.path.join(output_dir, 'low_value_customers'))

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: program.py <input_file_path> <output_directory>")
        sys.exit(1)
    input_file_path = sys.argv[1]
    output_dir = sys.argv[2]
    master_node_url="spark://spark-master:7077";
    main(master_node_url,input_file_path, output_dir)
