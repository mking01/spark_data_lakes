import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType, TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
    Purpose: Create spark session
    :return: spark session
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    Purpose: Process song data files, create songs and artists tables
    :param spark: spark session
    :param input_data: first part of songs file path
    :param output_data: first part of output data file path
    :return:
    '''
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # create schema
    schema = StructType([StructField('num_songs', IntegerType(), True),
                         StructField('artist_id', StringType(), False),
                         StructField('artist_latitude', DoubleType(), True),
                         StructField('artist_longitude', DoubleType(), True),
                         StructField('artist_location', StringType(), True),
                         StructField('artist_name', StringType(), True),
                         StructField('song_id', StringType(), False),
                         StructField('title', StringType(), True),
                         StructField('duration', DoubleType(), True),
                         StructField('year', IntegerType(), True)])
    
    # read song data file
    df = spark.read.json(song_data, schema = schema)

    # extract columns to create songs table, then drop duplicates and assign IDs
    songs_table = df.select(["title","artist_id","year","duration"]) \
                    .dropDuplicates() \
                    .withColumn("song_id", monotonically_increasing_id())
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year","artist_id").parquet(output_data + "songs/")

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name as name", "artist_location as location", \
                              "artist_latitude as latitude", "artist_longitude as longitude") \
                        .dropDuplicates() \
                        .withColumn("artist_id", monotonically_increasing_id())
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists/")


def process_log_data(spark, input_data, output_data):
    '''
    Purpose:  Process log data, create users, time, and songplays tables
    :param spark: Spark session
    :param input_data: Input data file path
    :param output_data: Output data file path
    :return:
    '''
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'
    
    # create schema
    schema = StructType([StructField('artist', StringType(), True),
                         StructField('auth', StringType(), True),
                         StructField('firstName', StringType(), True),
                         StructField('gender', StringType(), True),
                         StructField('itemInSession', IntegerType(), True),
                         StructField('lastName', StringType(), True),
                         StructField('length', DoubleType(), True),
                         StructField('level', StringType(), True),
                         StructField('location', StringType(), True),
                         StructField('method', StringType(), True),
                         StructField('page', StringType(), True),
                         StructField('registration', StringType(), True),
                         StructField('sessionId', IntegerType(), True),
                         StructField('song', StringType(), True),
                         StructField('status', IntegerType(), True),
                         StructField('ts', TimestampType(), True),
                         StructField('userAgent', StringType(), True),
                         StructField('userId', IntegerType(), False)])

    # read log data file
    df = spark.read.json(log_data, schema = schema)
    
    # filter by actions for song plays
    df = df.filter(df.page=="NextSong")

    # extract columns for users table    
    users_table = df.select("userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level") \
                    .dropDuplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users/")

    # create timestamp column from original timestamp column
    get_datetime = udf(date_conversion, TimestampType())
    df = df.withColumn("start_time", get_datetime('ts'))
    
    # extract columns to create time table
    time_table = df.select("start_time") \
                    .dropDuplicates() \
                    .withColumn("hour", hour(col("start_time"))) \
    .withColumn("day", day(col("start_time"))) \
    .withColumn("week", week(col('start_time'))) \
    .withColumn("month", month(col('start_time'))) \
    .withColumn("year", year(col('start_time'))) \
    .withColumn("weekday", date_format(col('start_time')))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").parquet(output_data + "time/")

    # read in song data to use for songplays table
    songs_df = spark.read.parquet(output_data + 'song_data/*/*/*')
    
    artists_df = spark.read.parquet(output_data + 'artists/*')

    # extract columns from joined song and log datasets to create songplays table 
    songs_logs_joined_df = df.join(songs_df, df.song == song_df.title)
    artists_songs_logs_joined_df = songs_logs_joined_df.join(artists_df, songs_logs_joined_df.artist == artists_df.name)
    songplays_table = artists_songs_logs_joined_df.join(time_table, \
                                                        artists_songs_logs_joined_df.ts == time_table.start_time, "left") \
                                                  .drop(artists_songs_logs_joined_df.year)

     songplays_table = songplays_table.select("start_time", "userId as user_id", "level", "song_id", "artist_id", \
                                               "sessionId as session_id", "location", "userAgent as user_agent", "year", \
                                               "month")                                                  
                                                        
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").parquet(output_data + "songplays/")

def main():
    '''
    Execute above functions in one program

    :return:
    '''
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
