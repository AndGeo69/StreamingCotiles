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
        new_edges = theDataframe.select(F.col("nodeU").alias("src"), F.col("nodeV").alias("dst"), F.col("timestamp"), F.col("tags"), F.lit(1).alias("weight"))


        # Check if the GraphFrame already exists (initialized in the driver)
        if not self.g:
            self.g = GraphFrame(new_nodes, new_edges)
        else:
            # Merge with existing vertices and edges
            existing_vertices = self.g.vertices
            existing_edges = self.g.edges

            new_nodes_to_add = new_nodes.join(existing_vertices, on="id", how="left_anti")
            new_edges_to_add = new_edges.join(existing_edges, on=["src", "dst"], how="left_anti")

            # Update the graph if there are new nodes or edges
            if not new_nodes_to_add.isEmpty() or not new_edges_to_add.isEmpty():
                self.g = GraphFrame(
                    self.g.vertices.union(new_nodes_to_add),
                    self.g.edges.union(new_edges_to_add)
                )

        self.g.vertices.show(truncate=False)

        # 3. Process edges (Add new edges or update existing edges)
        new_edges = theDataframe.select(F.col("nodeU").alias("src"),
                                        F.col("nodeV").alias("dst"),
                                        F.col("timestamp"), F.col("tags"),
                                        F.lit(1).alias("weight"))

        # Add or update the edges in the GraphFrame
        if hasattr(self, 'g'):
            # Join new edges with existing edges to find updates
            existing_edges = self.g.edges

            # Fix column naming ambiguity after the join
            updated_edges = existing_edges.join(new_edges,
                (existing_edges.src == new_edges.src) & (existing_edges.dst == new_edges.dst),
                "left"
            ).withColumn("weight",
                F.when(F.col("new_edges.weight").isNotNull(),
                       F.col("existing_edges.weight") + F.col("new_edges.weight"))
                .otherwise(F.col("existing_edges.weight"))
            ).select(
                F.col("src"), F.col("dst"), F.col("timestamp"), F.col("tags"), F.col("weight")
            )

            # Add edges that don't exist in the graph yet
            added_edges = new_edges.join(
                existing_edges,
                (new_edges.src == existing_edges.src) & (new_edges.dst == existing_edges.dst),
                "anti"
            )

            # Update the graph with new edges (ensure the schema is consistent)
            self.g = GraphFrame(self.g.vertices, updated_edges.union(added_edges))

        # Show the updated edges (debugging)
        self.g.edges.show(truncate=False)

        to_add_df = theDataframe.filter(F.col("action") != "-")

        # new_slice_df.foreach(lambda row: handle_new_slice(row, self.status))
        #
        # max_dt = batch_df.agg(F.max("dt")).collect()[0][0]
        # if max_dt:
        #     self.last_break = max_dt

        # u = batch_df.select("nodeU")
        # v = batch_df.select("nodeV")
        # dt = datetime.datetime.fromtimestamp(float(batch_df['timestamp']))
        # tags = batch_df['tags']
        # e['weight'] = 1
        # e["u"] = u
        # e["v"] = v
        # e["t"] = tags
        #
        # # Observations
        # gap = dt - self.last_break
        # dif = gap.days
        #
        # # Create a new slice if the observation window has passed
        # if dif >= self.obs:
        #     self.last_break = dt
        #     print("New slice. Starting Day: %s" % dt)
        #     self.status.write(u"Saving Slice %s: Starting %s ending %s - (%s)\n" %
        #                       (self.actual_slice, self.actual_time, dt,
        #                        str(time.asctime(time.localtime(time.time())))))
        #
        #     self.status.write(u"Edge Added: %d\tEdge removed: %d\n" % (self.added, self.removed))
        #     self.added = 0
        #     self.removed = 0
        #
        #     self.actual_time = dt
        #     self.status.flush()
        #
        #     self.splits = gzip.open("%s/%s/Draft3/splitting-%d.gz" % (self.base, self.path, self.actual_slice),
        #                             "wt", 3)
        #     self.splits.write(self.spl.getvalue())
        #     self.splits.flush()
        #     self.splits.close()
        #     self.spl = StringIO()
        #
        #     # self.print_communities()
        #     self.status.write(
        #         u"\nStarted Slice %s (%s)\n" % (self.actual_slice, str(datetime.datetime.now().time())))
        #
        # if u == v:
        #     return
        #
        # # Edge removal based on TTL
        # if self.ttl != float('inf'):
        #     qr.put((dt, (int(e['u']), int(e['v']), int(e['weight']), e['t'])))
        #     # self.remove(dt, qr)
        #
        # # Add nodes if they don’t exist in the graph
        # if not self.g.has_node(u):
        #     self.g.add_node(u)
        #     self.g.nodes[u]['c_coms'] = {}  # central
        #
        # if not self.g.has_node(v):
        #     self.g.add_node(v)
        #     self.g.nodes[v]['c_coms'] = {}
        #
        # # Update or add the edge in the graph
        # if self.g.has_edge(u, v):
        #     w = self.g.adj[u][v]["weight"]
        #     self.g.adj[u][v]["weight"] = w + e['weight']
        #     self.g.adj[u][v]["t"] = e['t']
        # else:
        #     self.g.add_edge(u, v)
        #     self.g.adj[u][v]["weight"] = e['weight']
        #     self.g.adj[u][v]["t"] = e['t']
        #
        # # Neighbor analysis
        # u_n = list(self.g.neighbors(u))
        # v_n = list(self.g.neighbors(v))
        #
        # # Evolution analysis
        # if len(u_n) > 1 and len(v_n) > 1:
        #     common_neighbors = set(u_n) & set(v_n)
        #     # self.common_neighbors_analysis(u, v, common_neighbors, e['t'])
        #
        # # Final slice update
        # self.status.write(u"Slice %s: Starting %s ending %s - (%s)\n" %
        #                   (self.actual_slice, self.actual_time, self.actual_time,
        #                    str(time.asctime(time.localtime(time.time())))))
        # self.status.write(u"Edge Added: %d\tEdge removed: %d\n" % (self.added, self.removed))
        # self.added = 0
        # self.removed = 0
        #
        # # self.print_communities()
        # self.status.write(u"Finished! (%s)" % str(time.asctime(time.localtime(time.time()))))
        # self.status.flush()
        # self.status.close()


