import pyspark
from pyspark import SparkContext as sc
from pyspark.sql import SparkSession
spark1 = SparkSession.builder.appName('Basics').getOrCreate()
df = spark1.read.json('yelp_academic_dataset_user.json')
#1
from pyspark.sql import SparkSession
from pyspark.sql.functions import year

spark = SparkSession.builder \
    .appName("Yearly Growth Analysis") \
    .getOrCreate()

json_file_path = "yelp_academic_dataset_user.json"

df = df.withColumn("signup_year", year(df["yelping_since"]))

yearly_growth = df.groupBy("signup_year").count().orderBy("signup_year")

yearly_growth.show()

#2
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("User Review Count Analysis") \
    .getOrCreate()

json_file_path = "yelp_academic_dataset_user.json"
df = spark.read.json(json_file_path)

user_review_count = df.groupBy("user_id").sum("review_count")

user_review_count.show()


#3
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

spark = SparkSession.builder \
    .appName("Popular Users Analysis") \
    .getOrCreate()

json_file_path = "yelp_academic_dataset_user.json"
df = spark.read.json(json_file_path)

popular_users = df.select("user_id", "fans").orderBy(desc("fans"))

popular_users.show()

#4
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, when, col

spark = SparkSession.builder \
     .appName("Elite Users Ratio Analysis") \
     .getOrCreate()

json_file_path = "yelp_academic_dataset_user.json"
df = spark.read.json(json_file_path)

df = df.withColumn("elite_year", year(col("elite")))
df = df.withColumn("user_type", when(col("elite_year") > 0, "elite").otherwise("regular"))

yearly_user_type_count = df.groupBy("yelping_since", "user_type").count().orderBy("yelping_since")

yearly_user_type_count.show()

elite_count = df.filter(df["user_type"] == "elite").count()
regular_count = df.filter(df["user_type"] == "regular").count()
elite_to_regular_ratio = elite_count / regular_count


print(f"Ratio of Elite Users to Regular Users: {elite_to_regular_ratio}")


#5

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, when, count, sum

spark = SparkSession.builder \
    .appName("Yearly User Proportions Analysis") \
    .getOrCreate()

json_file_path = "yelp_academic_dataset_user.json"
df = spark.read.json(json_file_path)

df = df.withColumn("signup_year", year("yelping_since"))
df = df.withColumn("silent_user", when(df["review_count"] == 0, 1).otherwise(0))

yearly_user_stats = df.groupBy("signup_year") \
    .agg(count("user_id").alias("total_users"),
         sum("silent_user").alias("silent_users")) \
    .orderBy("signup_year")

yearly_user_stats.show()

#6

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, count, sum, when

spark = SparkSession.builder \
    .appName("Yearly Statistics Summary") \
    .getOrCreate()

json_file_path = "yelp_academic_dataset_user.json"
df = spark.read.json(json_file_path)

df = df.withColumn("signup_year", year("yelping_since"))
df = df.withColumn("elite_year", year("elite"))

yearly_stats_summary = df.groupBy("signup_year") \
    .agg(count("user_id").alias("new_users"),
         sum("review_count").alias("total_review_count"),
         sum(when(df["elite_year"] > 0, 1).otherwise(0)).alias("elite_users")) \
    .orderBy("signup_year")

yearly_stats_summary.show()

#7
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, count, sum, when
import matplotlib.pyplot as plt

spark = SparkSession.builder \
    .appName("Yearly Statistics Summary") \
    .getOrCreate()

json_file_path = "yelp_academic_dataset_user.json"

df = spark.read.json(json_file_path)

df = df.withColumn("signup_year", year("yelping_since"))
df = df.withColumn("elite_year", year("elite"))

yearly_stats_summary = df.groupBy("signup_year") \
    .agg(count("user_id").alias("new_users"),
         sum("review_count").alias("total_review_count"),
         sum(when(df["elite_year"] > 0, 1).otherwise(0)).alias("elite_users")) \
    .orderBy("signup_year")

yearly_stats_pd = yearly_stats_summary.toPandas()

print("Yearly Statistics Summary:")
print(yearly_stats_pd)

plt.figure(figsize=(10, 5))
plt.plot(yearly_stats_pd["signup_year"], yearly_stats_pd["new_users"], marker='o', label='New Users')
plt.plot(yearly_stats_pd["signup_year"], yearly_stats_pd["total_review_count"], marker='o', label='Total Review Count')
plt.plot(yearly_stats_pd["signup_year"], yearly_stats_pd["elite_users"], marker='o', label='Elite Users')
plt.xlabel('Year')
plt.ylabel('Count')
plt.title('Yearly Statistics Summary')
plt.legend()
plt.grid(True)
plt.show()





