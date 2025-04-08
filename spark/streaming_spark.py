import os
import time
import argparse

from pyspark.sql import SparkSession

from applied.TILES import TILES


# Acts as the client, connecting to an open socket ready to receive data and process it
def create_streaming_session(host='localhost', port=9999):
    # global spark
    while True:
        try:
            # Initialize SparkSession
            spark = (SparkSession.builder.appName("StreamingCOTILES")
                     .config("spark.jars.packages","graphframes:graphframes:0.8.3-spark3.4-s_2.12")
            .config("spark.sql.shuffle.partitions", "3")
            .config("spark.executor.memory", "7g")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.driver.memory", "7g").getOrCreate())

            spark.conf.set("spark.streaming.backpressure.enabled", "true")

            basePath = os.getcwd()
            spark.sparkContext.setCheckpointDir(f"{basePath}/checkpoint")
            spark.sparkContext.setLogLevel("error")
            # spark.sparkContext.setLogLevel("TRACE")

            # Enabled by properties file located in /home/bigdata/spark/conf    spark-defaults.cond
            streamingDataFrame = spark.readStream.format('socket').option('host', host).option('port', port).load()
            streamingDF = (streamingDataFrame.selectExpr(
                "split(value, '\\t')[0] as action",
                "cast(split(value, '\\t')[1] as int) as nodeU",
                "cast(split(value, '\\t')[2] as int) as nodeV",
                "cast(split(value, '\\t')[3] as int) as timestamp",
                "split(split(value, '\\t')[4], ',') as tags"
            ))
            # Example of a line tab-delimited
            # "+    29	45503	1280970074	linux,arch-linux,dns,cache"
            # 1st: action, "+" add or "-" remove edge
            # 2nd: edge nodeU (u)
            # 3rd: edge nodeV (v)
            # 4th: timestamp - event time
            # 5th: tags (comma-separated)
            # The tags are applied on both edges

            tiles_instance = TILES(spark=spark)
            streamingDF.printSchema()

            print("streamingDF IsStreaming: " + streamingDF.isStreaming.__str__())
            tiles_instance.clear_directory(directory_path=f"{basePath}/checkpoint")
            # In the writeStream:
            (streamingDF.writeStream.foreachBatch(tiles_instance.execute)
                        .outputMode("append")
                        # .option("checkpointLocation", f"{basePath}/checkpoint")
                        .trigger(processingTime="5 seconds")
                        .start()
                        .awaitTermination())
            break

        except Exception as e:
            print(f"Failed to connect to socket: {e}")
            print("Retrying in 5 seconds...")
            time.sleep(1)




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start COTILES streaming algorithm.")

    parser.add_argument("--host", type=str, default="localhost", help="Host to listen the streaming data (default: localhost).")
    parser.add_argument("--port", type=int, default=9999, help="Port to too listen the streaming data (default: 9999).")

    create_streaming_session()