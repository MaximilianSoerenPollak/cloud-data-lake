import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_unixtime
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek


config = configparser.ConfigParser()
config.read('dl.cfg')

AWS_SECRET_KEY = config.get("KEYS", "AWS_SECRET_KEY")

AWS_ACCESS_KEY = config.get("KEYS","AWS_ACCESS_KEY")


def create_spark_session():
    spark = SparkSession \
        .builder \
        .master('local')\
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1")\
        .config('spark.hadoop.fs.s3a.access.key', AWS_ACCESS_KEY)\
        .config('spark.hadoop.fs.s3a.secret.key', AWS_SECRET_KEY)\
        .getOrCreate()
    return spark


def process_song_data(spark, output_data):
    # get filepath to song data file
    song_data = config.get("DATA", "SONG_DATA") 
    
    # read song data file
    # Just testing with the Sub-category files. Change if you want ALL song data to "/*/*/*/*.json" 
    df = spark.read.json((song_data + "/A/A/A/*.json")) 

    # extract columns to create songs table
    # songstable has the columns: song_id, title, artist_id, year, duration
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates()
    songs_table.createOrReplaceTempView("songs")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data, "overwrite")

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")\
                        .withColumnRenamed("artist_name", "name")\
                        .withColumnRenamed("artist_location","location")\
                        .withColumnRenamed("artist_latitude", "latiude")\
                        .withColumnRenamed("artist_longitude", "longitude")\
                        .dropDuplicates()

    artists_table.createOrReplaceTempView("artists")
    # write artists table to parquet files
    artists_table.write.parquet(output_data, "overwrite")


def process_log_data(spark, output_data):
    # get filepath to log data file
    log_data = config.get("DATA", "LOG_DATA")

    # read log data file
    df = spark.read.json(log_data + "/*/*/*.json") 
    
    # filter by actions for song plays
    df = df.select().where(col("page") == "NextSong")

    # extract columns for users table    
    users_table = df.select("user_id", "fist_name", "last_name", "gender", "level").dropDuplicates()
    users_table.createOrReplaceTempView("users")
    
    # write users table to parquet files
    users_table.write.parquet((output_data + "users/users.parquet"), "overwrite")

    # create timestamp column from original timestamp column
    # ----????--- The Timestamp column is already in the original DF?
    #get_timestamp = udf()
    #df = 
    
    # create datetime column from original timestamp column
    #get_datetime = udf()
    df = df.withColumn("Datetime", from_unixtime((col("ts")/1000), "yyyy-MM-dd HH:mm:ss"))
    
    # extract columns to create time table
    time_table = df.select("Datetime").withColumnRenamed("Datetime", "start_time") 
    time_table = time_table.withColumn("hour", hour("start_time"))\
                            .withColumn("day", dayofmonth("start_time"))\
                            .withColumn("week", weekofyear("start_time"))\
                            .withColumn("month", month("start_time"))\
                            .withColumn("year", year("start_time"))\
                            .withColumn("weekday", dayofweek("start_time"))
    time_table.createOrReplaceTempView("timetable")
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("month", "year").parquet((output_data + "timetable/timetable.parquet"), "overwrite")

    # read in song data to use for songplays table
    song_df = spark.read.parquet((output_data + "songs/songs.parquet"))
    # read in user data to use for songplays tableV
    artists_df = spark.read.parquet((output_data + "artists/artists.parquet"))

    # extract columns from joined song and log datasets to create songplays table 
    # songplay_id, #start_time, #user_id, #level, #song_id, #artist_id, session_id, location, user_agent
    songplays_table = song_df.select("artist_id", "song_id")\
                        .withColumn("start_time", time_table.select("start_time"))\
                        .withColumn("user_id", users_table.select("user_id"))\
                        .withColumn("level", df.select("level"))\
                        .withColumn("session_id", df.select("session_id"))\
                        .withColumn("user_agent", df.select("user_agent"))\
                        .withColumn("location", artists_df.select("location").where("artist_id" == artists_df.artist_id))
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet((output_data + "songsplays/songplays.parquet"), "overwrite")

        
def main():
    spark = create_spark_session()
    output_data = "~/Developing/nanodegree/aws_dl_project/data/"
    
    process_song_data(spark, output_data)    
    process_log_data(spark,  output_data)


if __name__ == "__main__":
    main()
