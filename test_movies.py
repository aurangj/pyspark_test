import pytest
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from my_spark.movies import *


schema2 = StructType([
    StructField("tconst", StringType()),
    StructField("averageRating", DoubleType()),
    StructField("numVotes", IntegerType()),
    StructField("score",  DoubleType())
])

schema1 = StructType([
    StructField("tconst", StringType()),
    StructField("averageRating", DoubleType()),
    StructField("numVotes", IntegerType())
])


@pytest.mark.usefixtures("spark_session")
def test_filter_and_calculate_score(spark_session):
    test_df = spark_session.createDataFrame(
        [
            ('tt0000001',1.0, 50),
            ('tt0000002',2.0, 100),
            ('tt0000003', 3.0 ,100),
            ('tt0000004', 4.0 ,150),
            ('tt0000005', 4.0, 40),
        ],
        schema=schema1
    )
    new_df = filter_and_calculate_score(test_df)
    expected_df = spark_session.createDataFrame(
        [
            ('tt0000001', 1.0, 50, 50.0),
            ('tt0000002', 2.0, 100, 200.0),
            ('tt0000003', 3.0, 100, 300.00),
            ('tt0000004', 4.0, 150,600.00),
        ],
        schema=schema2
    )

    assert new_df.count() == 4
    diff_df = expected_df.subtract(new_df)
    assert diff_df.count()  == 0

schema4 = StructType([
    StructField("tconst", StringType()),
    StructField("titleType", StringType()),
    StructField("score", DoubleType()),
    StructField("rank",  IntegerType())
])

schema3 = StructType([
    StructField("tconst", StringType()),
    StructField("titleType", StringType()),
    StructField("score", DoubleType())
])


@pytest.mark.usefixtures("spark_session")
def test_rank_filter_top_20_movies(spark_session):
    test_df = spark_session.createDataFrame(
        [
            ('tt0000001',"movie", 100.0),
            ('tt0000002',"movie", 200.0),
            ('tt0000003', "movie" ,600.0),
            ('tt0000004', "movie" ,400.0),
            ('tt0000005', "movie", 300.0),
        ],
        schema=schema3
    )
    new_df = calculate_rank_filter_top_20_movies(test_df)
    expected_df = spark_session.createDataFrame(
        [
            ('tt0000001', "movie", 100.0, 5 ),
            ('tt0000002', "movie", 200.0, 4),
            ('tt0000003', "movie", 600.0, 1),
            ('tt0000004', "movie", 400.0, 2 ),
            ('tt0000005', "movie", 300.0, 3),
        ],
        schema=schema4
    )
    new_df.show()
    assert new_df.count() == 5
    diff_df = expected_df.subtract(new_df)
    assert diff_df.count()  == 0

@pytest.mark.usefixtures("spark_session")
def test_rank_filter_top_20_movies_same_rank(spark_session):
    test_df = spark_session.createDataFrame(
        [
            ('tt0000001',"movie", 100.0),
            ('tt0000002',"movie", 200.0),
            ('tt0000003', "movie" ,600.0),
            ('tt0000004', "movie" ,400.0),
            ('tt0000005', "movie", 400.0),
        ],
        schema=schema3
    )
    new_df = calculate_rank_filter_top_20_movies(test_df)
    expected_df = spark_session.createDataFrame(
        [
            ('tt0000001', "movie", 100.0, 4),
            ('tt0000002', "movie", 200.0, 3),
            ('tt0000003', "movie", 600.0, 1),
            ('tt0000004', "movie", 400.0, 2 ),
            ('tt0000005', "movie", 400.0, 2),
        ],
        schema=schema4
    )
    new_df.show()
    assert new_df.count() == 5
    diff_df = expected_df.subtract(new_df)
    assert diff_df.count()  == 0

@pytest.mark.usefixtures("spark_session")
def test_rank_filter_top_3_movies_same_rank(spark_session):
    test_df = spark_session.createDataFrame(
        [
            ('tt0000001',"movie", 100.0),
            ('tt0000002',"movie", 200.0),
            ('tt0000003', "movie" ,600.0),
            ('tt0000004', "movie" ,400.0),
            ('tt0000005', "movie", 400.0),
        ],
        schema=schema3
    )
    new_df = calculate_rank_filter_top_20_movies(test_df,3)
    expected_df = spark_session.createDataFrame(
        [
            ('tt0000002', "movie", 200.0, 3),
            ('tt0000003', "movie", 600.0, 1),
            ('tt0000004', "movie", 400.0, 2),
            ('tt0000005', "movie", 400.0, 2),
        ],
        schema=schema4
    )
    new_df.show()
    assert new_df.count() == 4
    diff_df = expected_df.subtract(new_df)
    assert diff_df.count()  == 0