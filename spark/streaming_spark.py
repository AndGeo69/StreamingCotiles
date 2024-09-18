from pyspark.sql import SparkSession
import subprocess
import sys


if __name__ == "__main__":

    spark = SparkSession.builder.appName("TILES and eTILES Runner").getOrCreate()
    sc = spark.sparkContext

    # Define file path (assuming it's in the same directory)
    input_file = "NetworkSegmentETiles30.tsv"

    # Read the TSV file into a DataFrame
    df = spark.read.csv(input_file, sep="\t", header=False)

    # Show some rows (for debugging purposes)
    df.show(5)

    # Optionally, you can perform transformations on the data here if needed
    # For example, filtering rows or adding columns.

    # Define a function to run the external Python script using subprocess
    def run_tiles_algorithm(mode, obs, ttl, path, filename):
        try:
            process = subprocess.Popen([
                sys.executable, '/home/bigdata/PycharmProjects/StreamingCotiles/__main__.py',  # Replace this with the full path to your __main__.py if necessary
                filename,
                '-m', mode,
                '-o', str(obs),
                '-t', str(ttl),
                '-p', path
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)

            # Read stdout and stderr in real-time
            for line in process.stdout:
                print(line, end="")  # Output the stdout to the console in real-time

            # Wait for the process to finish and get the exit code
            process.wait()

            # Check if any error occurred (stderr)
            for line in process.stderr:
                print(f"Error: {line}", end="")  # Output the stderr to the console in real-time

            if process.returncode != 0:
                print(f"TILES/eTILES algorithm failed with exit code {process.returncode}")
            else:
                print("TILES/eTILES algorithm finished successfully.")

        except Exception as e:
            print(f"Error running the TILES/eTILES algorithm: {e}")

    # Set the mode for running the algorithm (TTL or Explicit)
    mode = 'Explicit'  # Or 'TTL'
    obs = 7            # Observation window in days
    ttl = 5            # Time-to-live for edges
    path = 'results'   # Directory to store output results (adjust as needed)

    # Instead of saving the DataFrame, just call the TILES or eTILES algorithm
    run_tiles_algorithm(mode, obs, ttl, path, input_file)

    # Stop the Spark session
    spark.stop()
