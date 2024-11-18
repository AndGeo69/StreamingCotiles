import datetime
import os
import sys
import time

from graphframes import GraphFrame
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
        self.g = None
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
        # batch_df.show()


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
        e = {}

        batch_df = batch_df.withColumn("dt", F.col("timestamp"))

        if self.last_break is not None:
            batch_df = batch_df.withColumn("gap", (F.col("timestamp") - F.lit(self.last_break.timestamp())))
            batch_df = batch_df.withColumn("dif", (F.col("gap") / 86400).cast("int"))  # Convert seconds to days


        # print("batch_df:")
        # batch_df.show()

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
            self.g = GraphFrame(new_nodes, new_edges)
        else:

            # TODO Do the following only on "+"
            # Merge with existing vertices and edges
            # existing_vertices = self.g.vertices
            # existing_edges = self.g.edges

            # new_nodes_to_add = new_nodes.join(existing_vertices, on="id", how="left_anti")
            # new_edges_to_add = new_edges.join(existing_edges, on=["src", "dst"], how="left_anti")

            # Update the graph if there are new nodes or edges
            if not new_nodes.isEmpty() or not new_edges.isEmpty():
                self.g = GraphFrame(
                    self.g.vertices.union(new_nodes),
                    self.g.edges.union(new_edges)
                )

        self.g.vertices.show(truncate=False)

        # 3. Process edges (Add new edges or update existing edges)
        new_edges = theDataframe.select(F.col("nodeU").alias("src"),
                                        F.col("nodeV").alias("dst"),
                                        F.col("timestamp"), F.col("tags"),
                                        F.lit(1).alias("weight"))
        new_edges.show()
        # Add or update the edges in the GraphFrame
        if hasattr(self, 'g'):
            self.g = update_graph_with_edges(self.g, new_edges)
            # Join new edges with existing edges to find updates
            # existing_edges = self.g.edges
            #
            # # Fix column naming ambiguity after the join
            # updated_edges = existing_edges.join(new_edges,
            #     (existing_edges.src == new_edges.src) & (existing_edges.dst == new_edges.dst),
            #     "left"
            # ).withColumn("weight",
            #     F.when(F.col("new_edges.weight").isNotNull(),
            #            F.col("existing_edges.weight") + F.col("new_edges.weight"))
            #     .otherwise(F.col("existing_edges.weight"))
            # ).select(
            #     F.col("src"), F.col("dst"), F.col("timestamp"), F.col("tags"), F.col("weight")
            # )
            #
            # # Add edges that don't exist in the graph yet
            # added_edges = new_edges.join(
            #     existing_edges,
            #     (new_edges.src == existing_edges.src) & (new_edges.dst == existing_edges.dst),
            #     "anti"
            # )
            #
            # # Update the graph with new edges (ensure the schema is consistent)
            # self.g = GraphFrame(self.g.vertices, updated_edges.union(added_edges))

        # Show the updated edges (debugging)
        print("graph edges:")
        self.g.edges.show(truncate=False)
        print("graph nodes:")
        self.g.vertices.show(truncate=False)

        to_add_df = theDataframe.filter(F.col("action") != "-")

def update_graph_with_edges(graph, new_edges):
    """
    Update the GraphFrame with the new edges, adding or updating as necessary.

    :param graph: The current GraphFrame (self.g)
    :param new_edges: DataFrame containing new edges (src, dst, timestamp, tags, weight)
    :return: Updated GraphFrame
    """
    # Current edges in the graph
    existing_edges = graph.edges.alias("existing")

    # Alias the new edges DataFrame
    new_edges = new_edges.alias("new")

    # Update existing edges: sum weights and update timestamps/tags
    updated_edges = existing_edges.join(
        new_edges,
        (F.col("existing.src") == F.col("new.src")) & (F.col("existing.dst") == F.col("new.dst")),
        "left_outer"
    ).select(
        F.col("existing.src"),
        F.col("existing.dst"),
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
        F.col("new.src"),
        F.col("new.dst"),
        F.col("new.weight").alias("weight"),
        F.col("new.timestamp"),
        F.col("new.tags")
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

def has_edge(self, u, v):
    """
    Checks if an edge exists between node `u` and node `v` in a GraphFrame.

    :param u: Source node ID
    :param v: Destination node ID
    :return: True if the edge exists, False otherwise
    """
    edge_exists = self.g.edges.filter((F.col("src") == u) & (F.col("dst") == v)).count() > 0
    return edge_exists