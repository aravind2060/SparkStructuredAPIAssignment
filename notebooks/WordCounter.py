import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

def create_spark_session(master_node_url,app_name):
    return SparkSession.builder.appName(app_name).master(master_node_url).getOrCreate()

def load_data(spark, file_path):
    return spark.read.text(file_path)

def process_data(df):
    return df.select(explode(split(col("value"), "\s+")).alias("word"))

def count_words(df):
    return df.groupBy("word").count()

def sort_words(df):
    return df.orderBy("count", ascending=False)

def main(master_node_url,input_file, output_dir):
    # Create a Spark session
    spark = create_spark_session(master_node_url,"WordCount")

    # Load and process the data
    text_df = load_data(spark, input_file)
    words_df = process_data(text_df)
    word_counts = count_words(words_df)
    sorted_word_counts = sort_words(word_counts)

    # Output the results
    sorted_word_counts.write.mode("overwrite").csv(output_dir)

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: /spark/bin/spark-submit WordCounter.py /data/input/random_words.txt /data/output/wordcount")
        sys.exit(-1)

    input_file = sys.argv[1]
    output_dir = sys.argv[2]
    master_node_url="spark://spark-master:7077";
    main(master_node_url,input_file, output_dir)