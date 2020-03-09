import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType, DoubleType, FloatType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = "s3n://udacity-dend/song_data/A/W/I/"
    
    # read song data file
    songs_data=spark.read.json(song_data)

    # create spark table view for songs data
    songs_data.createOrReplaceTempView("songs_data_table")

    # extract columns to create songs table (song_id, title, artist_id, year, duration)
    songs_table = spark.sql('''SELECT song_id, title, artist_id, year, duration
                               FROM songs_data_table''')

    # write songs table to parquet files
    songs_table.write.parquet(output_data + 'songs_table.parquet')

    # write a local copy to be used for creating songplays table
    songs_table.write.parquet('songs_table.parquet')

    # extract columns to create artists table (artist_id, name, location, lattitude, longitude)
    artists_table = spark.sql('''SELECT artist_id, artist_name AS name, artist_location AS location,
                                        artist_latitude AS latitude, artist_longitude AS longitude
                                 FROM songs_data_table''')
   
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists_table.parquet')

    # write a local copy to be used for creating songplays table
    artists_table.write.parquet('artists_table.parquet')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = 's3a://udacity-dend/log_data/*/*/*.json'

    # define schema for log data (spark had difficulty defining a consistent schema on all logs)
    schema = StructType([
        StructField('artist',StringType(),True),
        StructField('auth',StringType(),True),
        StructField('firstName',StringType(),True),
        StructField('gender',StringType(),True),
        StructField('itemInSession',LongType(),True),
        StructField('lastName',StringType(),True),
        StructField('length',DoubleType(),True),
        StructField('level',StringType(),True),
        StructField('location',StringType(),True),
        StructField('method',StringType(),True),
        StructField('page',StringType(),True),
        StructField('registration',DoubleType(),True),
        StructField('sessionId',LongType(),True),
        StructField('song',StringType(),True),
        StructField('status',LongType(),True),
        StructField('ts',LongType(),True),
        StructField('userAgent',StringType(),True),
        StructField('userId',StringType(),True)
    ])

    # read log data file
    log_data = spark.read.json(log_data, schema) 

    # create spark table view for the log data
    log_data.createOrReplaceTempView("log_table")
    
    # filter by actions for song plays
    log_data = spark.sql('''SELECT *
             FROM log_table
             WHERE page = "NextSong"''')
    
    # update spark table view with new filtered data
    log_data.createOrReplaceTempView("log_table")

    # extract columns for users table (user_id, first_name, last_name, gender, level)
    users_table = spark.sql('''SELECT DISTINCT userId, firstName, lastName, gender, level
                               FROM log_table''')
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users_table.parquet')

    # convert timestamp to seconds from milliseconds and put in column 'start_time'
    timestamp_to_sec = udf(lambda x: x/1000, FloatType())
    log_data = log_data.withColumn('start_time', timestamp_to_sec('ts'))

    # create datetime column from unix timestamp that was converted to seconds 
    log_data = log_data.withColumn('datetime', from_unixtime('start_time'))
    
    # update spark table view
    log_data.createOrReplaceTempView("log_table")
    
    # extract columns to create time table (start_time, hour, day, week, month, year, weekday)
    time_table = spark.sql('''SELECT DISTINCT start_time,
                        datetime, 
                        date_format(datetime, 'HH') AS hour,
                        date_format(datetime, 'dd') AS day,
                        date_format(datetime, 'w') AS week,
                        date_format(datetime, 'MM') AS month,
                        date_format(datetime, 'yyyy') AS year,
                        date_format(datetime, 'u') AS weekday 
                FROM log_table
            ''')

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + 'time_table.parquet')

    # read in song data to use for songplays table
    song_df = spark.read.parquet("songs_table.parquet")
    song_df.createOrReplaceTempView("songs_table")

    # read in artist data to use for songplays table
    artist_df = spark.read.parquet("artists_table.parquet")
    artist_df.createOrReplaceTempView("artists_table")

    # extract columns from joined song and log datasets to create songplays table 
    # (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    songplays_table = spark.sql('''SELECT CONCAT(e.ts, '-', e.sessionId) AS songplay_id, e.ts AS start_time, e.userId AS user_id,
                                        e.level AS level, sa.song_id AS song_id, 
                                        sa.artist_id AS artist_id, e.sessionId AS session_id, 
                                        e.location AS location, e.userAgent AS user_agent
                                   FROM log_table AS e
                                   JOIN (
                                       SELECT s.song_id, a.artist_id, s.title, a.name, s.duration
                                       FROM songs_table AS s
                                       INNER JOIN artists_table AS a
                                       ON s.artist_id = a.artist_id) AS sa
                                   ON sa.title = e.song 
                                   AND sa.name = e.artist 
                                   AND sa.duration = e.length
                                ''')


    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + 'songplays_table.parquet')


def main():
    spark = create_spark_session()
    
    # set efficient file output algorithm
    spark.sparkContext._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2")

    input_data = "s3a://udacity-dend/"
    output_data = "s3a://de-nano-project-4/output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
