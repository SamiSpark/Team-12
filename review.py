# from pyspark.sql import SparkSession
# from pyspark.sql.functions import year, count

# spark = SparkSession.builder \
#     .appName("Yearly Review Count") \
#     .getOrCreate()

# json_file_path = "yelp_academic_dataset_review.json"
# reviews_df = spark.read.json(json_file_path)

# yearly_review_count = reviews_df.withColumn("year", year(reviews_df["review_id"])) \
#     .groupBy("year") \
#     .agg(count("review_id").alias("review_count")) \
#     .orderBy("year")
# yearly_review_count.show()

# spark.stop()

#2
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import year, sum

# spark = SparkSession.builder \
#     .appName("Review Summary") \
#     .getOrCreate()

# json_file_path = "yelp_academic_dataset_review.json"
# reviews_df = spark.read.json(json_file_path)

# review_summary = reviews_df.withColumn("year", year(reviews_df["review_id"])) \
#     .groupBy("year") \
#     .agg(sum("useful").alias("total_helpful_reviews"),
#          sum("funny").alias("total_funny_reviews"),
#          sum("cool").alias("total_cool_reviews")) \
#     .orderBy("year")

# review_summary.show()

# spark.stop()

#3

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import year, count, rank
# from pyspark.sql.window import Window

# spark = SparkSession.builder \
#     .appName("User Ranking") \
#     .getOrCreate()

# json_review_file_path = "yelp_academic_dataset_review.json"
# json_user_file_path = "yelp_academic_dataset_user.json"
# reviews_df = spark.read.json("D:\Intern 2024\Project review'\yelp_academic_dataset_review.json")
# users_df = spark.read.json("D:\Intern 2024\Project review'\yelp_academic_dataset_user.json")

# reviews_df = reviews_df.withColumn("year", year(reviews_df["date"]))

# joined_df = reviews_df.join(users_df, reviews_df["user_id"] == users_df["user_id"])

# user_yearly_review_count = joined_df.groupBy("year", reviews_df["user_id"]).agg(count(reviews_df["review_id"]).alias("total_reviews"))

# window_spec = Window.partitionBy("year").orderBy(user_yearly_review_count["total_reviews"].desc())
# ranked_users = user_yearly_review_count.withColumn("rank", rank().over(window_spec))

# ranked_users.show()

# spark.stop()

4#
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import explode, split, lower
# from pyspark.sql.types import ArrayType, StringType

# spark = SparkSession.builder \
#     .appName("Top 20 Common Words") \
#     .getOrCreate()

# json_file_path = "yelp_academic_dataset_review.json"
# reviews_df = spark.read.json(json_file_path)

# words_df = reviews_df.select(explode(split(lower(reviews_df["text"]), "\s+")).alias("word"))

# clean_words_df = words_df.filter(words_df["word"].rlike("[a-zA-Z]"))

# word_count_df = clean_words_df.groupBy("word").count()

# top_20_common_words = word_count_df.orderBy("count", ascending=False).limit(20)

# top_20_common_words.show()

# spark.stop()
#5

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import explode, split, lower
# from pyspark.ml.feature import RegexTokenizer, StopWordsRemover
# import numpy as np
# import nltk
# from nltk.corpus import stopwords

# spark = SparkSession.builder \
#     .appName("Word Cloud Analysis") \
#     .getOrCreate()

# json_file_path = "yelp_academic_dataset_review.json"
# reviews_df = spark.read.json(json_file_path)

# tokenizer = RegexTokenizer(inputCol="text", outputCol="words", pattern=r"\W")
# reviews_df = tokenizer.transform(reviews_df)

# stop_words = stopwords.words("english")
# stop_words_remover = StopWordsRemover(inputCol="words", outputCol="filtered_words", stopWords=stop_words)
# reviews_df = stop_words_remover.transform(reviews_df)

# filtered_words_df = reviews_df.select(explode("filtered_words").alias("word"))
# word_cloud_data = filtered_words_df.groupBy("word").count()

# word_cloud_data.show()

# spark.stop()
