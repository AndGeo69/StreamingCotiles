# Requirements
- Apache Spark 3.5.4
- Python 3.10

# Step 1:
stream_file.py
-> Opens a socket at localhost:9999 and starts sending data reading them from file_path, default to <path>/Small_NetowrkData2.txt
Execute via python3.10 stream_file.py <path> --host <ip> --port <port>

Default values apply if args not provided.

# Step 2:
streaming_spark.py
-> Main spark application.

-> Connects to the socket at localhost:9999, creates a streaming dataframe, pre-process incoming text data -> applies foreachBatch method for further processing.


Example of a line tab-delimited

 "+    29	45503	1280970074	linux,arch-linux,dns,cache"
 
 1st: action, "+" add or "-" remove edge
 
 2nd: edge nodeU (u)
 
 3rd: edge nodeV (v)
 
 4th: timestamp - event time
 
 5th: tags (comma-separated)
 

NOTE: If streming_spark sits idle after initial launch, restart and it should start working.


Execute via python3.10 streaming_spark.py --host <ip> --port <port>

Default values apply if args not provided.

