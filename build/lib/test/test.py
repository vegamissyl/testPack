import unittest
# import sys
# from pathlib import Path
# sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import os, sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from src.process import Process

proc = Process("Social_Network_Ads.csv")
class TestDataProcess(unittest.TestCase):

    def  test_pandas(self):
        expected = 204
        df = proc.run_pandas()
        self.assertEqual(expected, df.iloc[0,0], "test failed")

    def test_spark(self):
        expected = 71759.803922
        df = proc.run_pyspark()
        self.assertAlmostEqual(expected, df.select("meanSalary").filter(df["Gender"]=="Female").collect()[0][0],None,"test failed",0.00001)
        proc.stop_spark()
        
if __name__ == "__main__":
    unittest.main()