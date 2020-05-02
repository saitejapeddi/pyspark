import findspark
findspark.init()
import sys
from operator import add
from pyspark import SparkContext
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("q3") \
        .getOrCreate()
    sc = SparkContext.getOrCreate()

    business = spark.read.format("csv").option("delimiter", ":").load("C:/Users/psait/Desktop/bda/business.csv").toDF("business_id", "tempCol1", "full_address", "tempCol3", "categories").drop("tempCol1", "tempCol3")
    review = spark.read.format("csv").option("delimiter", ":").load("C:/Users/psait/Desktop/bda/review.csv").toDF("review_id", "tempCol1", "user_id", "tempCol3", "business_id", "tempCol5", "rating").drop("tempCol1", "tempCol3", "tempCol5")
    
    joined = business.join(review, "business_id")
    stan_frame = joined.filter(joined["full_address"].contains("Stanford")).distinct()
    stan_frame = stan_frame["user_id", "rating"]
    usr = spark.read.format("csv").option("delimiter", ":").load("C:/Users/psait/Desktop/bda/user.csv").toDF("user_id", "tempCol1", "name", "tempCol3", "tempCol4", "url").drop("tempCol1", "tempCol3", "tempCol4")
    j = usr.join(stan_frame, "user_id")
    res = j["user_id", "name", "rating"].select("user_id", "rating")
    res.rdd.map(lambda x: str(x[0]) + "\t" + str(x[1])).repartition(1).saveAsTextFile("C:/Users/psait/Desktop/bda/q3.txt")
