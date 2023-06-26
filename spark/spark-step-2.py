from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StringType, StructType, StructField
from pyspark import SparkConf
import functools

spark = SparkSession \
        .builder \
        .getOrCreate()

loan_base_parquet = spark.read.parquet("file:///home/sub/cong/Library-Data-Analysis/data/base/loan_base")
loan_base_parquet.createOrReplaceTempView("loan_base")

lbrry_base_parquet = spark.read.parquet("file:///home/sub/cong/Library-Data-Analysis/data/base/lbrry_base")
lbrry_base_parquet.createOrReplaceTempView("lbrry_base")

book_base_parquet = spark.read.parquet("file:///home/sub/cong/Library-Data-Analysis/data/base/book_base")
book_base_parquet.createOrReplaceTempView("book_base")

print('Start merging tables')

tb3_merge = spark.sql("""SELECT tb1.CTRL_NO, tb1.AUTHR_NM, tb1.TITLE_NM, tb1.ISBN_THIRTEEN_NO,
                            tb2.LBRRY_CD, tb2.LBRRY_NM, tb2.BOOK_KEY_NO, tb2.MBER_SEQ_NO_VALUE,
                            tb2.LON_YEAR, tb2.LON_MONTH, tb2.LON_DAY,
                            tb2.RTURN_YEAR, tb2.RTURN_MONTH, tb2.RTURN_DAY, tb2.RTURN_PREARNGE_DE
                            FROM book_base tb1 RIGHT OUTER JOIN
                              (SELECT 
                              LBRRY_CD, LBRRY_NM, BOOK_KEY_NO, MBER_SEQ_NO_VALUE,
                              LON_YEAR, LON_MONTH, LON_DAY,
                              RTURN_YEAR, RTURN_MONTH, RTURN_DAY, RTURN_PREARNGE_DE
                              FROM (SELECT * FROM loan_base a
                                    LEFT OUTER JOIN (SELECT LBRRY_CD as tmp_cd, LBRRY_NM FROM lbrry_base) b
                                    ON a.LBRRY_CD = b.tmp_CD)) tb2
                              ON tb1.CTRL_NO = tb2.BOOK_KEY_NO""")


print('Start saving merged_table')

tb3_merge.write.format("parquet").mode("overwrite").partitionBy("LBRRY_CD").save("file:///home/sub/cong/Library-Data-Analysis/data/partition")

print('clear')

