import findspark
findspark.init()
from operator import add
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql import *


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("q4") \
        .getOrCreate()
    sc = SparkContext.getOrCreate()

    business = spark.read.format("csv").option("delimiter", ":").load("C:/Users/psait/Desktop/bda/business.csv").toDF("business_id", "tempCol1", "full_address", "tempCol3", "categories").drop("tempCol1", "tempCol3")
    review = spark.read.format("csv").option("delimiter", ":").load("C:/Users/psait/Desktop/bda/review.csv").toDF("review_id", "tempCol1", "user_id", "tempCol3", "business_id", "tempCol5", "stars").drop("tempCol1", "tempCol3", "tempCol5", "review_id")

    review = review.withColumn("stars", review["stars"].cast(IntegerType()))
    jf = business.join(review, "business_id").select("business_id", "full_address", "categories", "stars").groupBy("business_id",
                                                                                                       "full_address",
                                                                                                       "categories").avg(
        "stars");
    opframe = jf.toDF("business_id", "full_address", "categories", "avg_rating").sort("avg_rating",ascending = False).take(10)
    op = sc.parallelize(list(opframe)).toDF()
    final = op.rdd.map(lambda x :str(x[0]) + "\t" + str(x[1]) + "\t" + str(x[2]) + "\t" + str(x[3]))
    final.repartition(1).saveAsTextFile("C:/Users/psait/Desktop/bda/q4.txt")
