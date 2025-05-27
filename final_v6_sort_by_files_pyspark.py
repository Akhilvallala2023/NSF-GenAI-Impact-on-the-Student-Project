from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lower, regexp_replace
from pyspark.sql.types import BooleanType
from pathlib import Path
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KeywordBasedCSVProcessor") \
    .getOrCreate()

# Define keywords
edu_keywords = [
    "curriculum", "tutoring", "education", "educational", "school", "university",
    "universities", "higher education", "k-12", "kindergarten", "preschool",
    "elementary school", "middle school", "junior high", "high school", "highschool",
    "homeschool", "homeschooling", "teacher", "student", "teachers", "students"
]
ai_keywords = [
    "generative ai", "gen ai", "chatgpt", "ai", "midjourney", "chatgpt4o"
]

# Broadcast keywords
edu_keywords_bc = spark.sparkContext.broadcast(edu_keywords)
ai_keywords_bc = spark.sparkContext.broadcast(ai_keywords)

# Define UDF for filtering
def contains_keywords_udf(text):
    if text is None:
        return False
    text = text.lower()
    return any(edu in text for edu in edu_keywords_bc.value) and any(ai in text for ai in ai_keywords_bc.value)

from pyspark.sql.functions import udf
contains_keywords = udf(contains_keywords_udf, BooleanType())

# Helper to process each folder recursively
def process_folder(folder, base_directory, output_path):
    folder_path = f"{base_directory}/{folder}"
    all_csv_paths = list(Path(folder_path).rglob("*.csv"))
    csv_paths = [str(p) for p in all_csv_paths]

    if not csv_paths:
        print(f"No CSV files found in: {folder_path}")
        return

    # Read all CSVs into a DataFrame
    df = spark.read.option("header", True).option("inferSchema", True).csv(csv_paths)

    # Combine text and filter
    df = df.withColumn("combined_text", concat_ws(" ", col("post_title"), col("post_description")))
    df = df.withColumn("combined_text", lower(col("combined_text")))
    df_filtered = df.filter(contains_keywords(col("combined_text")))

    # Clean post DataFrame
    post_df = df_filtered.select(
        "post_id", "post_title", "post_description", "subreddit",
        "post_score", "post_upvotes", "post_upvote_ratio", "post_created_utc"
    ).dropna(subset=["post_id"]) \
     .withColumn("post_title", regexp_replace("post_title", r"^https?:\/\/.*[\r\n]*", "")) \
     .withColumn("post_description", regexp_replace("post_description", r"^https?:\/\/.*[\r\n]*", "")) \
     .orderBy("post_id", col("post_upvotes").desc()) \
     .dropDuplicates(["post_id"])

    # Clean comment DataFrame
    comment_df = df_filtered.select(
        "post_id", "comment_id", "parent_id", "comment_body",
        "comment_author", "comment_upvotes", "comment_created_utc", "comment_depth"
    ).withColumn("comment_body", regexp_replace("comment_body", r"^https?:\/\/.*[\r\n]*", "")) \
     .orderBy("comment_id", col("comment_upvotes").desc()) \
     .dropDuplicates(["comment_id"])

    # Save outputs
    post_output_path = f"{output_path}/posts/{folder}_post.csv"
    comment_output_path = f"{output_path}/comments/{folder}_comment.csv"

    post_df.write.mode("overwrite").option("header", True).csv(post_output_path)
    comment_df.write.mode("overwrite").option("header", True).csv(comment_output_path)

    print(f"✅ Finished processing folder: {folder}")

# Main execution
#list_of_folder = ["controversial", "gilded", "hot", "new", "random_rising", "rising", "top"]
list_of_folder = ["top"]
base_directory = "/home/avallala2023/data"
output_path = "/home/avallala2023/Desktop/Combined/collected_files"

for folder in list_of_folder:
    print(f"🔍 Processing folder: {folder}")
    process_folder(folder, base_directory, output_path)

# Stop Spark session
spark.stop()
