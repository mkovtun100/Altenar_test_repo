from pyspark.sql import SparkSession
import os
from pyspark.sql.functions import *

current_dir = os.path.dirname(__file__)
relative_path = "..\\sources\\*.json"
absolute_file_path = os.path.join(current_dir, relative_path)

spark = SparkSession.builder.appName("Altenar_batch_load") \
    .master("local[*]").getOrCreate()

df = spark.read.format("json") \
        .option("inferSchema", "true")\
        .load(absolute_file_path)

#df.printSchema()

destinct_events = df.select(col("type")).distinct().sort("type")

owners_columns = df.withColumn("owner_id", col("payload.pull_request.head.repo.owner.id")) \
       .withColumn("owner_login", col("payload.pull_request.head.repo.owner.login"))

owners_columns_non_null = owners_columns.select("owner_id", "owner_login").na.drop()

counted_owners_task_1 = owners_columns_non_null.groupby("owner_id", "owner_login").count().sort(col("count").desc())

#result of the first task
print("Result of the first task")
counted_owners_task_1.show()

committers = df.withColumn("committers", col("payload.commits")).na.drop()\
    .withColumn("committers_cutted", col("committers.author.name"))\
    .withColumn("committers_exploded", explode(col("committers_cutted")))\
    .select("committers_exploded")

committers_counted_task_2 = committers.select(col("committers_exploded")).groupby("committers_exploded").count()\
    .withColumnRenamed("committers_exploded", "name")\
    .withColumnRenamed("count", "commits_quantity")\
    .filter(col("commits_quantity") > 1)\
    .sort(col("commits_quantity").desc(), "name")

#result of the second task
print("Result of the second task")
committers_counted_task_2.show()

total_users = df.withColumn("user", col("actor.login"))\
    .select("user").distinct()

users_with_more_commits = committers_counted_task_2.select("name")

users_with_less_commits = total_users.subtract(users_with_more_commits)

#result of the third task
print("Result of the third task")
users_with_less_commits.show()

spark.stop()