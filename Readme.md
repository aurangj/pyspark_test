
1) python version used is 3.8.5. I use anaconda env to create virtual env and used
conda env create -f py38.yml
source activate py38
pip install -r requirements.txt to install  all python packages
I just gave all of them but code does not require most of them


to run code locally

2) $SPARK_HOME/../bin/spark-submit --driver-memory 1g  --master local[4] movies.py

3) to run on server ( executor memory will change base on data size on cluster)

 $SPARK_HOME/../bin/spark-submit --driver-memory 1g  --executor-memory 2g --master local[4] movies.py




 3) output of 4 test cases

(py38) users-MacBook-Pro:my_spark user$ python -m pytest
============================================================================================================================= test session starts ==============================================================================================================================
platform darwin -- Python 3.8.5, pytest-7.1.0, pluggy-1.0.0
rootdir: /Users/user/PycharmProjects/DSA/my_spark
plugins: dash-1.20.0
collected 4 items

tests/test_movies.py ....                                                                                                                                                                                                                                                [100%]

============================================================================================================================== 4 passed in 29.84s ==============================================================================================================================
(py38) users-MacBook-Pro:my_spark user$
