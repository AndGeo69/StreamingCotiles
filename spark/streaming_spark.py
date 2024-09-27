from pyspark.sql import SparkSession
import subprocess
import sys

from pyspark.sql.functions import explode, split
from pyspark.streaming import StreamingContext


if __name__ == "__main__":

    spark = SparkSession.builder.appName("StreamingCOTILES").getOrCreate()

    # Streaming Context Spark's DStream API
    # sc = spark.sparkContext
    # ssc = StreamingContext(sc, 1)
    # lines = ssc.socketTextStream("localhost", 9999)
    # lines.pprint()
    # ssc.start()
    # ssc.awaitTermination()

    # Structured Streaming API
    lines = spark.readStream.format('socket').option('host', 'localhost').option('port','9999').load()

    words = lines.select(
        explode(
            split(lines.value, '\t')
        ).alias('word')
    )
    wordCounts = words.groupBy('word').count()
    query = wordCounts.writeStream.outputMode('complete').format('console').trigger(processingTime='5 seconds').start()

    query.awaitTermination()


    def run_tiles_algorithm(mode, obs, ttl, path, filename):
        try:
            process = subprocess.Popen([
                sys.executable, '/home/bigdata/PycharmProjects/StreamingCotiles/__main__.py',
                filename,
                '-m', mode,
                '-o', str(obs),
                '-t', str(ttl),
                '-p', path
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)

            for line in process.stdout:
                print(line, end="")  # Output the stdout to the console in real-time

            process.wait()

            for line in process.stderr:
                print(f"Error: {line}", end="")

            if process.returncode != 0:
                print(f"COTILES algorithm failed with exit code {process.returncode}")
            else:
                print("COTILES algorithm finished successfully.")

        except Exception as e:
            print(f"Error running the COTILES algorithm: {e}")

    # Set the mode for running the algorithm (TTL or Explicit)
    mode = 'Explicit'  # Or 'TTL'
    obs = 7            # Observation window in days
    ttl = 5            # Time-to-live for edges
    path = 'results'   # Directory to store output results (adjust as needed)

    # Instead of saving the DataFrame, just call the COTILES algorithm
    # run_tiles_algorithm(mode, obs, ttl, path, input_file)

    # dont stop when dealing with streaming data
    # spark.stop()
