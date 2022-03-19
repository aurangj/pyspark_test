from pyspark.sql import SparkSession
from pyspark.sql import Window,DataFrame
import pyspark.sql.functions as psf


number_of_cores = 4

def create_spark_session()->SparkSession:
    return SparkSession.builder.appName("Python Spark get top 20 movies ").master("local[*]").getOrCreate()


def read_rating_file(spark:SparkSession) -> DataFrame:
    df_title_ratings = spark.read.csv(r'/Users/user/Downloads/title.ratings.tsv', sep=r'\t', header=True,
                                       inferSchema=True)

    df_title_ratings = df_title_ratings.repartition(4 * number_of_cores)

    return df_title_ratings

def filter_and_calculate_score(df_title_ratings:DataFrame) -> DataFrame:
    df_title_ratings = df_title_ratings.filter(df_title_ratings.numVotes >= 50)

    df_title_ratings = df_title_ratings.withColumn("score", df_title_ratings.numVotes * df_title_ratings.averageRating)

    return df_title_ratings


def read_basics_file(spark:SparkSession)->DataFrame:

    df_title_basic = spark.read.csv(r'/Users/user/Downloads/title.basics.tsv', sep=r'\t', header=True,
                                     inferSchema=True).select('tconst', 'titleType', 'primaryTitle', 'originalTitle')

    df_title_basic = df_title_basic.repartition(4 * number_of_cores)

    return df_title_basic

def filter_only_movies(df_title_basic:DataFrame) -> DataFrame:

    df_title_basic = df_title_basic.filter(df_title_basic.titleType == 'movie')

    return df_title_basic


def join_df_basic_title(df_title_basic : DataFrame,df_title_ratings:DataFrame) -> DataFrame:
    df_score = df_title_basic.join(df_title_ratings, df_title_basic.tconst == df_title_ratings.tconst). \
        select(df_title_ratings['tconst'], 'titleType', 'primaryTitle', 'originalTitle', 'averageRating', 'numVotes',
               'score')

    return df_score


def calculate_rank_filter_top_20_movies(df_score:DataFrame,rank_value=20) -> DataFrame:
    wA = Window.partitionBy(df_score["titleType"]).orderBy(df_score["score"].desc())

    df_score = df_score.withColumn("rank", psf.dense_rank().over(wA))
    df_score = df_score.filter(df_score.rank <= rank_value)
    return df_score


def read_title_principal(spark:SparkSession) ->DataFrame:
    df_title_principals = spark.read.csv(r'/Users/user/Downloads/title.principals.tsv', sep=r'\t', header=True,
                                          inferSchema=True)
    df_title_principals = df_title_principals.repartition(4 * number_of_cores)
    return df_title_principals

def join_df_score_principals(df_score: DataFrame, df_title_principals : DataFrame) -> DataFrame:
    df_final = df_score.join(df_title_principals, df_score.tconst == df_title_principals.tconst). \
        select(df_score['tconst'], 'titleType', 'primaryTitle', 'originalTitle', 'averageRating', 'numVotes', 'score',
               'rank', 'ordering', 'nconst', 'category', 'job', 'characters')

    return df_final


if __name__ == '__main__':
    spark = create_spark_session()
    df_title_ratings = read_rating_file(spark)
    df_title_ratings = filter_and_calculate_score(df_title_ratings)
    df_title_ratings.cache()
    df_title_basic = read_basics_file(spark)
    df_title_basic = filter_only_movies(df_title_basic)
    df_score = join_df_basic_title(df_title_basic,df_title_ratings)
    df_score.cache()
    df_score = calculate_rank_filter_top_20_movies(df_score)
    df_title_principal = read_title_principal(spark)
    df_final = join_df_score_principals(df_score,df_title_principal)
    df_final.write.option("header",True).csv(r'/Users/user/PycharmProjects/DSA/my_spark/final_output')












