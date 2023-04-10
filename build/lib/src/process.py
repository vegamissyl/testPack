import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, mean, col
from os.path import dirname, join
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from decorator import checkTime

current_dir = dirname(__file__)

class Process:

    def __init__(self, file):
        
        self.file = file
        self.spark = SparkSession.builder \
                                 .master('local[1]') \
                                 .appName("SparkTest") \
                                 .getOrCreate()
    @checkTime
    def run_pandas(self):
        print("\npandas:\n")
        data = pd.read_csv(join(current_dir, self.file))
        # df_mean_salary_by_gender = data.groupby(["Gender"])[["EstimatedSalary", "Purchased"]].agg(["mean","count"])
        df_analyse_sal_purchased = data.groupby(["Gender"]).agg({"Gender" : "count",
                                                                  "EstimatedSalary" : "mean",
                                                                  "Purchased" : ["mean", "sum"]})
        print(df_analyse_sal_purchased, "\n")
        return df_analyse_sal_purchased

    @checkTime
    def run_pyspark(self):
        print("\nSpark:\n")
        df = self.spark.read.option("header", True).option("inferSchema",True).csv(join(current_dir, self.file))
        #df.printSchema()
        df_analyse_sal_purchased = df.groupBy("Gender").agg(count(col("Gender")).alias("count"),
                                                            mean(df["EstimatedSalary"]).alias("meanSalary"),
                                                            mean("Purchased").alias("meanPurchased"),
                                                            sum("Purchased").alias("sumPurchased"))
        df_analyse_sal_purchased.show()
        return df_analyse_sal_purchased

    def stop_spark(self):
        self.spark.stop()