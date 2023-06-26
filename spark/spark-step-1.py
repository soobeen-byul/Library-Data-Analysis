from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StringType, StructType, StructField
from pyspark import SparkConf
import functools

spark = SparkSession \
        .builder \
        .getOrCreate()

def unionAll(dfs):
    return functools.reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)

# 대여정보
loan_23_01 = "file:///home/sub/cong/Library-Data-Analysis/data/loan/2023/202301/"
loan_23_02 = "file:///home/sub/cong/Library-Data-Analysis/data/loan/2023/202302/"
loan_23_03 = "file:///home/sub/cong/Library-Data-Analysis/data/loan/2023/202303/"
loan_23_04 = "file:///home/sub/cong/Library-Data-Analysis/data/loan/2023/202304/"
loan_23_05 = "file:///home/sub/cong/Library-Data-Analysis/data/loan/2023/202305/"

csv_path=[loan_23_01,loan_23_02,loan_23_03,loan_23_04,loan_23_05]
 
loan_csv = []

for path in csv_path:
    tmp_csv = spark.read.option("header", True).option("encoding", "utf-8").csv(path)
    loan_csv.append(tmp_csv)
    
loan_csv_df=unionAll(loan_csv)
loan_csv_df.createOrReplaceTempView("loan_raw")

loan_base_df = spark.sql("""SELECT LBRRY_CD, BOOK_KEY_NO, MBER_SEQ_NO_VALUE,
                            LEFT(LON_DE,4) as LON_YEAR, SUBSTRING(LON_DE,6,2) as LON_MONTH, SUBSTRING(LON_DE,9,2) as LON_DAY,
                            LEFT(RTURN_DE,4) as RTURN_YEAR, SUBSTRING(RTURN_DE,6,2) as RTURN_MONTH, SUBSTRING(RTURN_DE,9,2) as RTURN_DAY,
                            SUBSTRING_INDEX(RTURN_PREARNGE_DE,' ',1) as RTURN_PREARNGE_DE
                            FROM loan_raw""")

loan_base_df.createOrReplaceTempView("loan_base")
print('load loan_base_df')


# 도서관 정보
lbrry_csv = spark.read.option("header", True).option("encoding", "utf-8").csv("file:///home/sub/cong/Library-Data-Analysis/data/NL_CD_LIBRARY_202305.csv")
lbrry_csv.createOrReplaceTempView("lbrry_raw")

lbrry_base_df = spark.sql("""SELECT 
                            LBRRY_CD, LBRRY_NM, LBRRY_NO, LBRRY_TY_CD, 
                            LBRRY_TY_NM, LBRRY_NCM_NM, ETC_LBRRY_CD, 
                            AREA_CD, ONE_AREA_NM, TWO_AREA_NM,LBRRY_ADDR, ETC_LBRRY_ADDR, WETHR_AREA_CD FROM lbrry_raw""")

lbrry_base_df.createOrReplaceTempView("lbrry_base")
print('load lbrry_base_df')


# 도서 정보
book_csv = spark.read.option("header", True).option("encoding", "utf-8").csv("file:///home/sub/cong/Library-Data-Analysis/data/NL_BO_BOOK_PUB_202305-1.csv")
book_csv.createOrReplaceTempView("book_raw")

book_base_df = spark.sql("""SELECT
                            LBRRY_CD, CTRL_NO, AUTHR_NM, TITLE_NM, TITLE_SBST_NM, KDC_NM,
                            ISBN_THIRTEEN_NO,SGVL_ISBN_NO,SGVL_ISBN_ADTION_SMBL_NM,ISBN_THIRTEEN_ORGT_NO,
                            CL_SMBL_NO,CL_SMBL_FLAG_NM,MEDIA_FLAG_NM,UTILIIZA_LMTT_FLAG_NM, UTILIIZA_TRGET_FLAG_NM
                            FROM book_raw""")

book_base_df.createOrReplaceTempView("book_base")
print('load book_base_df')

#base table 저장
loan_base_df.write.format("parquet").mode("overwrite").option("encoding", "euc-kr").save("file:///home/sub/cong/Library-Data-Analysis/data/base/loan_base")
print('Save loan_base_parquet')

lbrry_base_df.write.format("parquet").mode("overwrite").option("encoding", "euc-kr").save("file:///home/sub/cong/Library-Data-Analysis/data/base/lbrry_base")
print('Save lbrry_base_parquet')

book_base_df.write.format("parquet").mode("overwrite").option("encoding", "euc-kr").save("file:///home/sub/cong/Library-Data-Analysis/data/base/book_base")
print('Save book_base_parquet')

print('clear')
