import datetime
import os
import string
import sys
import time

from graphframes import GraphFrame
from networkx.classes import Graph, common_neighbors
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
        self.communityTagsDf: DataFrame = None
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
        # qr = PriorityQueue()

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

        to_remove_df = theDataframe.filter(F.col("action") == "-")

        # 1 TODO remove_edge
        # remove_edge_from_graph(self.g, to_remove_df)

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
            # new_edges.show(truncate= False)
            print("new data (nodes)")
            # new_nodes.show(truncate= False)

        print("before theDataframe")
        print("graph edges:")
        # self.g.edges.show(truncate=False)
        print("graph vertices:")
        # self.g.vertices.show(truncate=False)

        # Add or update the edges in the GraphFrame
        if self.g:
            self.g = update_graph_with_edges(self.g, new_edges)

        # common_neighbors = evolution(self.g, new_edges)
        print(time.ctime() + " Before detect_common_neighbors")
        common_neighbors = detect_common_neighbors(self, new_edges)
        print(time.ctime() + " After detect_common_neighbors")
        # printTrace("Common Neighbors:", common_neighbors)

        common_neighbors_analysis(self, common_neighbors)


        # Show the updated edges (debugging)
        print("graph edges:")
        # self.g.edges.show(truncate=False)
        print("graph nodes:")
        # self.g.vertices.show(truncate=False)


def common_neighbors_analysis(self, common_neighbors: DataFrame):
    """
    Analyze and update communities for nodes with common neighbors in a streaming manner.

    :param common_neighbors: DataFrame with columns ["src", "dst", "common_neighbors"]
    """
    if common_neighbors.isEmpty():
        return

    # Step 1: Filter out null common_neighbors and explode to one row per common_neighbor
    common_neighbors = (
        common_neighbors
        .filter(F.col("common_neighbors").isNotNull())
        .withColumn("common_neighbor", F.explode(F.col("common_neighbors")))
    )

    # Step 2: Reshape common_neighbors for a single join (avoiding double join)
    common_neighbors_reshaped = (
        common_neighbors
        .select(
            F.col("src").alias("node_id"),
            F.col("dst").alias("other_node_id"),
            F.col("timestamp"),
            F.col("tags"),
            F.col("weight"),
            F.col("common_neighbors")
        )
        .union(
            common_neighbors
            .select(
                F.col("dst").alias("node_id"),
                F.col("src").alias("other_node_id"),
                F.col("timestamp"),
                F.col("tags"),
                F.col("weight"),
                F.col("common_neighbors")
            )
        )
    )

    # Rename node_id and other_node_id back to src and dst for later use
    common_neighbors_reshaped = (
        common_neighbors_reshaped
        .withColumnRenamed("node_id", "src")
        .withColumnRenamed("other_node_id", "dst")
    )

    # Step 3: Join with vertices DataFrame once (avoiding double join)
    vertices = self.g.vertices.select("id", "c_coms")
    # Perform the join once and get community information for both src and dst
    shared_coms = (
        common_neighbors_reshaped
        .join(vertices, (F.col("src") == F.col("id")) | (F.col("dst") == F.col("id")), how="left")
        .select(
            F.col("src"),
            F.col("dst"),
            F.col("timestamp"),
            F.col("tags"),
            F.col("weight"),
            F.col("common_neighbors"),
            F.when(F.col("src") == F.col("id"), F.col("c_coms")).alias("src_coms"),
            F.when(F.col("dst") == F.col("id"), F.col("c_coms")).alias("dst_coms")
        )
    )

    # Optional: Cache the resulting DataFrame if it's reused later in streaming processing
    shared_coms.cache()

    print(time.ctime() + "common_neighbors_analysis: AFTER single join with vertices")

    # # Step 1: Filter out null common_neighbors and explode to one row per common_neighbor
    # common_neighbors = (
    #     common_neighbors
    #     .filter(F.col("common_neighbors").isNotNull())
    #     .withColumn("common_neighbor", F.explode(F.col("common_neighbors")))
    # )
    #
    # print(time.ctime() + "common_neighbors_analysis: BEFORE common_neighbors double join w/ vertices")
    # # Step 2: Join with `self.g.vertices` to get community information for src and dst
    # vertices = self.g.vertices.select("id", "c_coms")
    # shared_coms = (
    #     common_neighbors
    #     .join(vertices.alias("vertices_u"), F.col("src") == F.col("vertices_u.id"), how="left")
    #     .join(vertices.alias("vertices_v"), F.col("dst") == F.col("vertices_v.id"), how="left")
    #     .select(
    #         F.col("src"),
    #         F.col("dst"),
    #         F.col("timestamp"),
    #         F.col("tags"),
    #         F.col("weight"),
    #         F.col("common_neighbors"),
    #         F.col("vertices_u.c_coms").alias("src_coms"),
    #         F.col("vertices_v.c_coms").alias("dst_coms")
    #     )
    # )
    print(time.ctime() + "common_neighbors_analysis: AFTER common_neighbors double join w/ vertices")


    print(time.ctime() + "common_neighbors_analysis: BEFORE intersecting")
    # Step 3: Compute derived columns - shared_coms, only_u, and only_v
    shared_coms = (
        shared_coms
        .withColumn("shared_coms", F.array_intersect(F.col("src_coms"), F.col("dst_coms")))
        .withColumn("only_u", F.array_except(F.col("src_coms"), F.col("dst_coms")))
        .withColumn("only_v", F.array_except(F.col("dst_coms"), F.col("src_coms")))
    )
    print(time.ctime() + "common_neighbors_analysis: AFTER intersecting")

    # Step 4: Rename and reselect columns in the final DataFrame
    print(time.ctime() + "common_neighbors_analysis: BEFORE shared_coms selecting")
    shared_coms = (
        shared_coms
        .select(
            F.col("src").alias("shared_coms_src"),
            F.col("dst").alias("shared_coms_dst"),
            F.col("timestamp"),
            F.col("tags").alias("shared_coms_tags"),
            F.col("weight"),
            F.col("common_neighbors"),
            F.col("src_coms"),
            F.col("dst_coms"),
            F.col("shared_coms"),
            F.col("only_u"),
            F.col("only_v")
        )
    )
    print(time.ctime() + "common_neighbors_analysis: AFTER shared_coms selecting")


    print("shared_coms")
    # printTrace("shared_coms", shared_coms)

    # Propagate 'src' logic: Propagate src if 'common_neighbor' is in 'only_v'
    print(time.ctime() + "common_neighbors_analysis: BEFORE shared_coms propagated expression ")
    propagated = shared_coms.withColumn(
        "propagated",
        F.when(
            F.expr(
                "size(array_intersect(common_neighbors, only_v)) > 0 OR"
                " size(array_intersect(common_neighbors, only_u)) > 0"),
            True
        ).otherwise(False)
    )
    print(time.ctime() + "common_neighbors_analysis: AFTER shared_coms propagated expression ")

    #  TODO check common_neighbors list might need an exploded one

    print("propagated")
    # printTrace("propagated", propagated)

    propagated = propagated.withColumn("community_id", F.monotonically_increasing_id())

    # printTrace("new_communities(propagated)", propagated)

    print(time.ctime() + "common_neighbors_analysis: BEFORE shared_coms nodesToBeAddedToCom = expression ")

    nodesToBeAddedToCom = (propagated.filter("propagated = false")
                               .select(
                                    F.col("shared_coms_src"),
                                    F.col("shared_coms_dst"),
                                    F.col("timestamp"),
                                    F.col("shared_coms_tags"),
                                    F.col("community_id"),
                            )
                           )
    prexistingCommunities = (propagated.filter("propagated = true")
                                .select(
                                    F.col("shared_coms_src"),
                                    F.col("shared_coms_dst"),
                                    F.col("timestamp"),
                                    F.col("shared_coms_tags"),
                                    F.col("community_id"),
                                )
                            )

    # printTrace("node propagted FASLE", nodesToBeAddedToCom)
    # printTrace("node propagted TRUE", prexistingCommunities)

    print(time.ctime() + " BEFORE add to community")
    add_to_community(self, nodesToBeAddedToCom)
    print(time.ctime() + " AFTER add to community")

    # if prexistingCommunities is not None:
    #     printTrace("prexistingCommunities", prexistingCommunities)

    # Step 4: Handle new communities if no propagation occurs TODO this
    # new_communities = common_neighbors_exploded.join(
    #     shared_coms, on=["src", "dst"], how="left_anti"
    # ).withColumn("community_id", F.monotonically_increasing_id())


def add_to_community(self, toBeAddedDf: DataFrame):
    """
    Adds a node to a community in a streaming-friendly way using DataFrames.

    Args:
        node (str): Node to be added to the community.
        community_id (str): ID of the community.
        tags (str): Tags associated with the community, comma-separated.
    """
    src_tags = toBeAddedDf.select(
        F.col("shared_coms_src").alias("nodeId"),
        F.explode(F.split(F.col("shared_coms_tags"), ",")).alias("tag"),
        F.col("community_id")
    )

    # Explode the tags for shared_coms_dst
    dst_tags = toBeAddedDf.select(
        F.col("shared_coms_dst").alias("nodeId"),
        F.explode(F.split(F.col("shared_coms_tags"), ",")).alias("tag"),
        F.col("community_id")
    )

    new_entries = src_tags.union(dst_tags)

    # If communityTagsDf is empty, initialize it; otherwise, append the new entries
    if self.communityTagsDf is None:
        self.communityTagsDf = new_entries
    else:
        self.communityTagsDf = self.communityTagsDf.union(new_entries)

    # Join communityTagsDf with vertices
    updated_vertices = self.g.vertices.join(
        self.communityTagsDf.withColumnRenamed("nodeId", "id"),
        on="id",
        how="left"
    ).withColumn(
        "c_coms",
        F.when(
            F.col("community_id").isNotNull(),  # If there is a community_id to add
            F.concat(F.col("c_coms"), F.array(F.col("community_id")))  # Append to c_coms array
        ).otherwise(F.col("c_coms"))  # Keep the original c_coms if no community_id
    ).drop("community_id", "tag")  # Drop extra columns from join if not needed

    updated_vertices = updated_vertices.withColumn(
        "c_coms", F.array_distinct(F.col("c_coms"))
    )
    # Step 3: Deduplicate and remove redundant entries
    # updated_vertices = (
    #     updated_vertices
    #     .withColumn("c_coms", F.array_distinct(F.col("c_coms")))  # Remove duplicates within c_coms
    #     .dropDuplicates(["id"])  # Ensure no duplicate rows for the same id
    # )

    # Update the graph
    self.g = GraphFrame(updated_vertices, self.g.edges)

def detect_common_neighbors(self, edge_updates):
    """
    Detect common neighbors for edges in the batch using GraphFrames.
    """

    # Step 1: Extract neighbors for each node in the graph using a simpler approach
    edges = self.g.edges
    neighbors = (
        edges.select(F.col("src").alias("node"), F.col("dst").alias("neighbor"))
        .union(edges.select(F.col("dst").alias("node"), F.col("src").alias("neighbor")))
        .groupBy("node")
        .agg(F.collect_list("neighbor").alias("neighbors"))
    )

    # Step 2: Join edge_updates with neighbors for both src and dst nodes
    edge_neighbors = (
        edge_updates
        .join(neighbors.withColumnRenamed("node", "src"), on="src", how="left")
        .withColumnRenamed("neighbors", "src_neighbors")
        .join(neighbors.withColumnRenamed("node", "dst"), on="dst", how="left")
        .withColumnRenamed("neighbors", "dst_neighbors")
    )

    # Step 3: Compute common neighbors
    edge_neighbors = edge_neighbors.withColumn(
        "common_neighbors", F.array_intersect(F.col("src_neighbors"), F.col("dst_neighbors"))
    )

    # Step 4: Filter edges with common neighbors
    common_neighbors_filtered = edge_neighbors.filter(
        F.col("common_neighbors").isNotNull() & (F.size(F.col("common_neighbors")) > 0)
    )
    common_neighbors_filtered.select(F.col("dst"), F.col("src"),
                                     F.col("timestamp"), F.col("tags"),
                                     F.col("weight"), F.col("common_neighbors"))

    return common_neighbors_filtered.select(F.col("dst"), F.col("src"),
                                     F.col("timestamp"), F.col("tags"),
                                     F.col("weight"), F.col("common_neighbors"))


# def detect_common_neighbors(self, edge_updates):
#     """
#     Detect common neighbors for edges in the batch using GraphFrames.
#     """
#
#     # TODO Check performance of these, seems slow
#     # Extract neighbors for each node in the graph
#     neighbors = self.g.find("(a)-[]->(b)") \
#         .select(F.col("a.id").alias("node"), F.col("b.id").alias("neighbor")) \
#         .groupBy("node") \
#         .agg(F.collect_list("neighbor").alias("neighbors"))
#
#     # Join edge_updates with neighbors twice (for src and dst nodes)
#     edge_neighbors = edge_updates \
#         .join(neighbors.withColumnRenamed("node", "src"), on="src", how="left") \
#         .withColumnRenamed("neighbors", "src_neighbors") \
#         .join(neighbors.withColumnRenamed("node", "dst"), on="dst", how="left") \
#         .withColumnRenamed("neighbors", "dst_neighbors")
#
#     # Compute common neighbors
#     common_neighbors_df = edge_neighbors.withColumn(
#         "common_neighbors", F.array_intersect(F.col("src_neighbors"), F.col("dst_neighbors"))
#     )
#
#     # Filter edges with common neighbors
#     common_neighbors_filtered = common_neighbors_df.filter(
#         F.col("common_neighbors").isNotNull() & (F.size(F.col("common_neighbors")) > 0)
#     )
#
#     return common_neighbors_filtered

def evolution(graph: GraphFrame, edge_updates: DataFrame):
    """
    Analyze common neighbors for nodes in updated edges and handle evolution.
    Args:
        graph (GraphFrame): Current GraphFrame representing the graph.
        edge_updates (DataFrame): New edges added in the current batch, with columns ['src', 'dst'].
    Returns:
        DataFrame: Common neighbors for the given edges.
    """

    # Extract neighbors for both source (u) and destination (v)
    neighbors = graph.edges.select(
        F.col("src").alias("node"),
        F.col("dst").alias("neighbor")
    ).union(
        graph.edges.select(
            F.col("dst").alias("node"),
            F.col("src").alias("neighbor")
        )
    )

    # Calculate neighbor counts for all nodes
    neighbor_counts = neighbors.groupBy("node").agg(F.count("neighbor").alias("neighbor_count"))

    # Filter for nodes with more than one neighbor (u_n and v_n conditions)
    valid_neighbors = neighbor_counts.filter(F.col("neighbor_count") > 1).select("node")

    # Join edges with valid nodes to ensure u and v both meet the condition
    valid_edges = (
        edge_updates.alias("edges")
        .join(valid_neighbors.alias("valid_u"), F.col("edges.src") == F.col("valid_u.node"))
        .join(valid_neighbors.alias("valid_v"), F.col("edges.dst") == F.col("valid_v.node"))
    )

    # Find common neighbors for valid (u, v) pairs
    common_neighbors = (
        valid_edges
        .join(neighbors.alias("u_neighbors"), F.col("edges.src") == F.col("u_neighbors.node"), "inner")
        .join(neighbors.alias("v_neighbors"), F.col("edges.dst") == F.col("v_neighbors.node"), "inner")
        .filter(F.col("u_neighbors.neighbor") == F.col("v_neighbors.neighbor"))
        .select(
            F.col("edges.src").alias("node_u"),
            F.col("edges.dst").alias("node_v"),
            F.col("u_neighbors.neighbor").alias("common_neighbor")
        )
        .groupBy("node_u", "node_v")
        .agg(F.collect_set("common_neighbor").alias("common_neighbors"))
    )

    return common_neighbors


def remove_edge_from_graph(graph:GraphFrame, df_to_remove: DataFrame):
# TODO revisit
    existing_edges = graph.edges.alias("existing")
    edges_to_remove = df_to_remove.alias("remove")
    printTrace("existing edge", existing_edges)
    printTrace("edges to remove", edges_to_remove)

    # has_edge but in dataframes
    removed_edges = existing_edges.join(
        edges_to_remove,
        (F.col("existing.src") == F.col("remove.nodeU")) &
        (F.col("existing.dst") == F.col("remove.nodeV")),
        "inner"
    ).select("existing.src", "existing.dst")

    printTrace("removed edges:", removed_edges)

    printTrace("graph.vertices:", graph.vertices)

    shared_communities = graph.vertices.alias("u").join(
        graph.vertices.alias("v"),
        (F.col("u.id") == removed_edges["src"]) & (F.col("v.id") == removed_edges["dst"])
    ).select(
        F.array_intersect(F.col("u.c_coms"), F.col("v.c_coms")).alias("shared_coms"),
        F.col("u.id").alias("src"),
        F.col("v.id").alias("dst")
    )

    printTrace("shared_coms", shared_communities)

    neighbors_u = existing_edges.filter(F.col("src") == removed_edges["src"]).select("dst")
    neighbors_v = existing_edges.filter(F.col("src") == removed_edges["dst"]).select("dst")

    common_neighbors = neighbors_u.intersect(neighbors_v)

    coms_to_change = shared_communities.withColumn(
        "affected_nodes",
        F.array_union(F.array(F.col("src"), F.col("dst")), common_neighbors)
    )

    return


def printTrace(str: string, df: DataFrame):
    print(str)
    df.show()

def update_graph_with_edges(graph, new_edges):
    # Current edges in the graph
    existing_edges = graph.edges.alias("existing")

    # Alias the new edges DataFrame
    new_edges = new_edges.alias("new")

    print("existing_edges:")
    # existing_edges.show()
    print("new_edges:")
    # new_edges.show()
    # Update existing edges: sum weights and update timestamps/tags
    updated_edges = existing_edges.join(
        new_edges,
        (F.col("existing.src") == F.col("new.src")) & (F.col("existing.dst") == F.col("new.dst")),
        "left_outer"
        # "inner"
    ).select(
        F.col("existing.src").alias("src"),
        F.col("existing.dst").alias("dst"),
        # Update weight if new value exists; otherwise, keep the old weight
        F.when(F.col("new.weight").isNotNull(), F.col("existing.weight") + F.col("new.weight"))
        .otherwise(F.col("existing.weight")).alias("weight"),
        # Use the latest timestamp where available
        F.coalesce(F.col("new.timestamp"), F.col("existing.timestamp")).alias("timestamp"),
        # Combine tags if new tags are available; otherwise, keep old tags
        F.coalesce(F.col("new.tags"), F.col("existing.tags")).alias("tags")
#           F.col("existing.src").alias("src"),
    #     F.col("existing.dst").alias("dst"),
    #     (F.col("existing.weight") + F.col("new.weight")).alias("weight"),
    #     F.col("new.timestamp").alias("timestamp"),
    #     F.col("new.tags").alias("tags")
    )

    print('updated_edges:')
    # updated_edges.show()

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

    print('added_edged:')
    # added_edges.show()
    # Combine updated and added edges
    final_edges = updated_edges.unionByName(added_edges)

    print('final_edges:')
    # final_edges.show()

    # Return a new GraphFrame with updated edges
    return GraphFrame(graph.vertices, final_edges)