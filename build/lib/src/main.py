from process import Process

if __name__=="__main__":
    proc = Process("Social_Network_Ads.csv")
    proc.run_pandas()
    proc.run_pyspark()
    proc.stop_spark()