# PySpark Shell 주피터 노트북에서 열 수 있도록 설정
export PYSPARK_DRIVER_PYTHON="jupyter"
export PYSPARK_DRIVER_PYTHON_OPTS="notebook"

## 파이썬 3를 사용한다면 아래 설정 추가
export PYSPARK_PYTHON=python3

## local[n] 는 로컬 코어 n개를 사용한다는 의미
alias spark-jupyter='$SPARK_HOME/bin/pyspark --master local[2]'

spark-jupyter
