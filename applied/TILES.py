import datetime
import os
import sys
import time

from graphframes import GraphFrame
from networkx.classes import Graph
from pyspark.sql import functions as F
from pyspark.sql.functions import min

if sys.version_info > (2, 7):
    from queue import PriorityQueue
else:
    from Queue import PriorityQueue


__author__ = "Giulio Rossetti"
__contact__ = "giulio.rossetti@gmail.com"
__website__ = "about.giuliorossetti.net"
__license__ = "BSD"

from pyspark.sql import DataFrame


class TILES:
    def __init__(self, stream=None, spark=None,  ttl=float('inf'), obs=7, path="", start=None, end=None):
        self.path = path
        self.ttl = ttl
        self.cid = 0
        self.actual_slice = 0
        self.g: GraphFrame = None
        self.spark = spark
        self.base = os.getcwd()
        self.status = open(f"{self.base}/{path}/extraction_status.txt", "w")
        self.representation = open(f"{self.base}/{path}/representation.txt", "w")
        self.removed = 0
        self.added = 0
        self.start = start
        self.end = end
        self.obs = obs
        self.communities = {}

        self.stream = stream

        self.edge_buffer = []

        self.last_break = None  # Initialize last_break as None to indicate first batch
        self.actual_time = None  # Initialize actual_time to be set during first batch
        self.sliceCount = 0


    def execute(self, batch_df: DataFrame, batch_id):
        """
        Execute TILES algorithm on streaming data using foreachBatch.
        """
        self.status.write(u"Started! (%s) \n\n" % str(time.asctime(time.localtime(time.time()))))
        self.status.flush()

        # Priority queue for TTL handling
        qr = PriorityQueue()

        if batch_df.isEmpty():
            return

        batch_df.sort(batch_df.timestamp)

        batch_df = batch_df \
            .withColumn("e_u", F.col("nodeU")) \
            .withColumn("e_v", F.col("nodeV")) \
            .withColumn("e_weight", F.lit(1)) \
            .withColumn("e_t", F.col("tags"))


        # creating slices - logic needed
        if self.actual_time is None and self.last_break is None:
            min_timestamp_df = batch_df.select(min("timestamp").alias("min_timestamp"))
            first_min_timestamp = min_timestamp_df.first()["min_timestamp"]
            if first_min_timestamp is not None:
                self.actual_time = datetime.datetime.fromtimestamp(float(first_min_timestamp))
                self.last_break = self.actual_time
                print(f"First timestamp received {self.actual_time}, timestamp {first_min_timestamp.__str__}")

        self.added += 1

        batch_df = batch_df.withColumn("dt", F.col("timestamp"))

        if self.last_break is not None:
            batch_df = batch_df.withColumn("gap", (F.col("timestamp") - F.lit(self.last_break.timestamp())))
            batch_df = batch_df.withColumn("dif", (F.col("gap") / 86400).cast("int"))  # Convert seconds to days

        if F.col("dif").isNotNull:
            new_slice_df = batch_df.filter(F.col("dif") >= self.obs)

            # print("new_slice_df:")
            # new_slice_df.show()
            if not new_slice_df.isEmpty():
                # new_slice_df.show(truncate=False)  # debug print

                # Update last_break and actual_time to the latest timestamp in this slice
                max_timestamp = new_slice_df.agg(F.max("timestamp")).collect()[0][0]
                self.last_break = datetime.datetime.fromtimestamp(max_timestamp)
                # print(f"~ old actual_time = {self.actual_time}")
                self.actual_time = self.last_break
                self.sliceCount += 1
                # print(f"New slice detected starting from {self.actual_time}. Processed batch {batch_id}. Slice Count: {self.sliceCount}")
                print(f"~ NEW actual_time = {self.actual_time}")
                # print("~~ do something now that a slice is detected\n")

        theDataframe = batch_df.filter(F.col("nodeU") != F.col("nodeV"))

        #1 TODO remove_edge
        # to_remove_df = theDataframe.filter(F.col("action") == "-")

        new_nodes_u = theDataframe.select(F.col("nodeU").alias("id"))
        new_nodes_v = theDataframe.select(F.col("nodeV").alias("id"))

        new_nodes = new_nodes_u.union(new_nodes_v).withColumn("c_coms", F.array()).distinct()
        new_edges = theDataframe.select(F.col("nodeU").alias("src"),
                                        F.col("nodeV").alias("dst"),
                                        F.col("timestamp"),
                                        F.col("tags"),
                                        F.lit(1).alias("weight"))


        # Check if the GraphFrame already exists (initialized in the driver)
        if not self.g:
            self.g: GraphFrame = GraphFrame(new_nodes, new_edges)
        else:
            # TODO Do the following only on "+"
            # Get existing vertices from the current graph
            existing_vertices = self.g.vertices.alias("existing")

            # Perform a left anti-join to find new nodes not already in the graph
            new_nodes_to_add = new_nodes.alias("new").join(
                existing_vertices,
                on="id",
                how="left_anti"
            )

            # If there are new nodes to add, update the GraphFrame vertices
            if not new_nodes_to_add.isEmpty():
                print("new_nodes_to_add - nodes that are new:")
                new_nodes_to_add.show()
                updated_vertices = self.g.vertices.union(new_nodes_to_add)
                self.g = GraphFrame(updated_vertices, self.g.edges)

            print("new data (edges)")
            new_edges.show(truncate= False)
            print("new data (nodes)")
            new_nodes.show(truncate= False)

        print("before theDataframe")
        print("graph edges:")
        self.g.edges.show(truncate=False)
        print("graph vertices:")
        self.g.vertices.show(truncate=False)

        # Add or update the edges in the GraphFrame
        if hasattr(self, 'g'):
            self.g = update_graph_with_edges(self.g, new_edges)

        # Show the updated edges (debugging)
        print("graph edges:")
        self.g.edges.show(truncate=False)
        print("graph nodes:")
        self.g.vertices.show(truncate=False)

        to_add_df = theDataframe.filter(F.col("action") != "-")

def update_graph_with_edges(graph, new_edges):
    # Current edges in the graph
    existing_edges = graph.edges.alias("existing")

    # Alias the new edges DataFrame
    new_edges = new_edges.alias("new")

    print("existing_edges:")
    existing_edges.show()
    print("new_edges:")
    new_edges.show()
    # Update existing edges: sum weights and update timestamps/tags
    updated_edges = existing_edges.join(
        new_edges,
        (F.col("existing.src") == F.col("new.src")) & (F.col("existing.dst") == F.col("new.dst")),
        "inner"
    ).select(
        F.col("existing.src").alias("src"),
        F.col("existing.dst").alias("dst"),
        (F.col("existing.weight") + F.col("new.weight")).alias("weight"),
        F.col("new.timestamp").alias("timestamp"),
        F.col("new.tags").alias("tags")
    )

    # Add new edges: those not present in the current edges
    added_edges = new_edges.join(
        existing_edges,
        (F.col("new.src") == F.col("existing.src")) & (F.col("new.dst") == F.col("existing.dst")),
        "anti"
    ).select(
        F.col("new.src").alias("src"),
        F.col("new.dst").alias("dst"),
        F.col("new.weight").alias("weight"),
        F.col("new.timestamp").alias("timestamp"),
        F.col("new.tags").alias("tags")
    )

    print('updated_edges:')
    updated_edges.show()

    print('added_edged:')
    added_edges.show()
    # Combine updated and added edges
    final_edges = updated_edges.unionByName(added_edges)

    print('final_edges:')
    final_edges.show()

    # Return a new GraphFrame with updated edges
    return GraphFrame(graph.vertices, final_edges)