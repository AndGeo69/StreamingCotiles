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
        self.community_memberships: DataFrame = None
        self.communityTagsDf: DataFrame = None
        self.communitiesDf: DataFrame = None
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
            new_edges.show(truncate= False)
            print("new data (nodes)")
            new_nodes.show(truncate= False)

        print("before theDataframe")
        print("graph edges:")
        self.g.edges.show(truncate=False)
        print("graph vertices:")
        self.g.vertices.show(truncate=False)

        # Add or update the edges in the GraphFrame
        if self.g:
            self.g = update_graph_with_edges(self.g, new_edges)

        # common_neighbors = evolution(self.g, new_edges)
        common_neighbors = detect_common_neighbors(self, new_edges)
        printTrace("Common Neighbors:", common_neighbors)

        common_neighbors_analysis(self, new_edges, common_neighbors)


        # Show the updated edges (debugging)
        print("graph edges:")
        self.g.edges.show(truncate=False)
        print("graph nodes:")
        self.g.vertices.show(truncate=False)




def common_neighbors_analysi(self, new_edges: DataFrame, common_neighbors: DataFrame):
    """
    Analyze and update communities for nodes with common neighbors in a streaming manner.

    :param new_edges: DataFrame with columns ["src", "dst"]
    :param common_neighbors: DataFrame with columns ["src", "dst", "common_neighbor"]
    """

    # Ensure common_neighbors is not empty
    if common_neighbors.isEmpty():
        return

    # Step 1: Extract communities for src and dst
    src_communities = new_edges.alias("edges") \
        .join(self.communitiesDf.alias("coms_u"), F.array_contains(F.col("coms_u.nodes"), F.col("edges.src")), "left") \
        .select(F.col("edges.src"), F.col("coms_u.cid").alias("src_cid"))

    dst_communities = new_edges.alias("edges") \
        .join(self.communitiesDf.alias("coms_v"), F.array_contains(F.col("coms_v.nodes"), F.col("edges.dst")), "left") \
        .select(F.col("edges.dst"), F.col("coms_v.cid").alias("dst_cid"))

    # Step 2: Derive shared and exclusive communities
    shared_coms = src_communities.join(dst_communities, (src_communities["src"] == dst_communities["dst"]), "inner") \
        .select("src_cid", "dst_cid").distinct()

    only_u = src_communities.subtract(shared_coms)
    only_v = dst_communities.subtract(shared_coms)

    # Step 3: Analyze communities for common neighbors
    common_neighbor_coms = common_neighbors.alias("cn") \
        .join(self.communitiesDf.alias("cn_coms"), F.array_contains(F.col("cn_coms.nodes"), F.col("cn.common_neighbor")), "left") \
        .select("cn.src", "cn.dst", "cn.common_neighbor", "cn_coms.cid").distinct()

    # Step 4: Propagate communities
    def propagate_community(row):
        src = row["src"]
        dst = row["dst"]
        common_neighbor = row["common_neighbor"]
        community_id = row["cid"]

        if community_id in only_v:
            self.addToCommunity(src, community_id, tags=None)
        if community_id in only_u:
            self.addToCommunity(dst, community_id, tags=None)
        if community_id in shared_coms and community_id not in self.communitiesDf:
            self.addToCommunity(common_neighbor, community_id, tags=None)

    common_neighbor_coms.foreach(propagate_community)

    # Step 5: Handle propagation failures
    new_coms = common_neighbors.filter("cid IS NULL").distinct()
    new_coms.foreach(lambda row: self.addToCommunity(row["src"], self.new_community_id, tags=None))
    new_coms.foreach(lambda row: self.addToCommunity(row["dst"], self.new_community_id, tags=None))
    new_coms.foreach(lambda row: self.addToCommunity(row["common_neighbor"], self.new_community_id, tags=None))


def common_neighbors_analysis(self, new_edges:DataFrame, common_neighbors:DataFrame):
    """
    Analyze and update communities for nodes with common neighbors in a streaming manner.

    :param common_neighbors: DataFrame with columns ["src", "dst", "common_neighbor"]
    """

    if common_neighbors.isEmpty() or common_neighbors.count() < 1:
        return

    #  NEW IMPL. VVVV

    common_neighbors = common_neighbors.filter(F.col("common_neighbors").isNotNull()).withColumn(
        "common_neighbor", F.explode(F.col("common_neighbors")))

    shared_coms = (common_neighbors.alias("common_neighbors")
                   .join(self.g.vertices.alias("vertices_u"),
                         on=F.col("common_neighbors.src") == F.col("vertices_u.id"), how="left")
                   .join(self.g.vertices.alias("vertices_v"),
                         on=F.col("common_neighbors.dst") == F.col("vertices_v.id"), how="left"))

    # Might not need new_edges at all since i have the information in common_neighbors !!! <------
    # shared_coms = (new_edges.alias("new_edges")
    #                .join(self.g.vertices.alias("vertices_u"),
    #                      on=F.col("new_edges.src") == F.col("vertices_u.id"), how="left")
    #                .join(self.g.vertices.alias("vertices_v"),
    #                      on=F.col("new_edges.dst") == F.col("vertices_v.id"), how="left"))

    # Step 1: Extract the communities for each node
    # We need to explicitly select the 'c_coms' from both vertices_u and vertices_v
    shared_coms = shared_coms.withColumn("src_coms", F.col("vertices_u.c_coms"))
    shared_coms = shared_coms.withColumn("dst_coms", F.col("vertices_v.c_coms"))

    # Step 2: Calculate "shared_coms" - communities that are shared between `src` and `dst`
    shared_coms = shared_coms.withColumn("shared_coms", F.array_intersect("src_coms", "dst_coms"))
    # Step 3: Calculate "only_u" - communities that are unique to `u`
    shared_coms = shared_coms.withColumn("only_u", F.array_except("src_coms", "dst_coms"))

    # Step 4: Calculate "only_v" - communities that are unique to `v`
    # |src|dst| timestamp| tags|weight| id|c_coms| id|c_coms|src_coms|dst_coms|shared_coms|only_u|only_v|
    shared_coms = shared_coms.withColumn("only_v", F.array_except("dst_coms", "src_coms"))

    shared_coms = (shared_coms.withColumnRenamed("src", "shared_coms_src")
                   .withColumnRenamed("dst", "shared_coms_dst")
                   .withColumnRenamed("tags", "shared_coms_tags")
                   .select(
                        F.col("shared_coms_src").alias("shared_coms_src"),
                        F.col("shared_coms_dst").alias("shared_coms_dst"),
                        F.col("timestamp").alias("timestamp"),
                        F.col("shared_coms_tags").alias("shared_coms_tags"),
                        F.col("weight").alias("weight"),
                        F.col("src_neighbors").alias("src_neighbors"),
                        F.col("dst_neighbors").alias("dst_neighbors"),
                        F.col("common_neighbors").alias("common_neighbors"),
                        F.col("src_coms").alias("src_coms"),
                        F.col("dst_coms").alias("dst_coms"),
                        F.col("shared_coms").alias("shared_coms"),
                        F.col("only_u").alias("only_u"),
                        F.col("only_v").alias("only_v"),
                   ))

    printTrace("shared_coms", shared_coms)

    # # Join common neighbors with src_coms and dst_coms
    # shared_coms_with_neighbors = shared_coms.join(
    #     common_neighbors_exploded,
    #     (F.col("shared_coms_src") == F.col("common_neighbor")) |
    #     (F.col("shared_coms_dst") == F.col("common_neighbor")),
    #     how="inner"
    # ).filter(
    #     (F.col("shared_coms_src") == F.col("common_neighbor")) | (F.col("shared_coms_dst") == F.col("common_neighbor"))
    # )

    # Propagate 'src' logic: Propagate src if 'common_neighbor' is in 'only_v'
    propagated_src = shared_coms.withColumn(
        "propagated_src",
        F.when(F.array_contains(F.col("only_v"), F.col("common_neighbors")), True).otherwise(False)
    )
    #  TODO check common_neighbors list might need an exploded one

    # Propagate 'dst' logic: Propagate dst if 'common_neighbor' is in 'only_u'
    propagated_dst = shared_coms.withColumn(
        "propagated_dst",
        F.when(F.array_contains(F.col("only_u"), F.col("common_neighbors")), True).otherwise(False)
    )

    printTrace("propagated_src", propagated_src)
    printTrace("propagated_dst", propagated_dst)

    # Combine the propagated community updates
    propagated = propagated_src.union(propagated_dst).distinct()

    printTrace("propagated", propagated)

    new_communities = propagated.withColumn("community_id", F.monotonically_increasing_id())

    # Step 4: Handle new communities if no propagation occurs TODO this
    # new_communities = common_neighbors_exploded.join(
    #     shared_coms, on=["src", "dst"], how="left_anti"
    # ).withColumn("community_id", F.monotonically_increasing_id())

    new_communities_updates = new_communities.select(
        F.col("src").alias("node_id"), "community_id"
    ).union(new_communities.select(
        F.col("dst").alias("node_id"), "community_id"
    ))

    # Combine all updates
    all_updates = propagated.union(new_communities_updates).distinct()

    # Step 5: Update the global community memberships DataFrame
    self.community_memberships = self.community_memberships.union(all_updates).distinct()

    # OLD IMPL. VVV

    # Use g.vertices A OR g.nodes V
    self.community_memberships = self.g.vertices.select(
        F.col("id").alias("node_id"),
        F.explode(F.col("c_coms")).alias("community_id") # Each community as a separate row
    )

    common_neighbors_exploded =\
        common_neighbors.withColumn("common_neighbor", F.explode(F.col("common_neighbors")))

    # Join to find the communities of src, dst, and common neighbors
    cn_coms = self.community_memberships.alias("cn_coms").join(
        common_neighbors_exploded, F.col("cn_coms.node_id") == F.col("common_neighbor"), how="inner"
    )

    # Alias the DataFrames for clarity
    src_coms = self.community_memberships.alias("src_coms").join(
        common_neighbors_exploded, F.col("src_coms.node_id") == F.col("node_u"), how="inner"
    )
    dst_coms = self.community_memberships.alias("dst_coms").join(
        common_neighbors_exploded, F.col("dst_coms.node_id") == F.col("node_v"), how="inner"
    )

    # Join the DataFrames to identify shared communities, explicitly reference column names
    shared_coms = src_coms.join(dst_coms, on="community_id", how="inner").select(
        F.col("src_coms.node_id").alias("node_u"), F.col("dst_coms.node_id").alias("node_v"), "community_id"
    )

    # Identify unique communities for src and dst
    only_src_coms = src_coms.join(dst_coms, on="community_id", how="left_anti").select(
        F.col("src_coms.node_id").alias("node_u"), "community_id"
    )
    only_dst_coms = dst_coms.join(src_coms, on="community_id", how="left_anti").select(
        F.col("dst_coms.node_id").alias("node_v"), "community_id"
    )

    # Add nodes to communities based on propagation rules
    propagated_src = only_dst_coms.join(cn_coms, on="community_id", how="inner").select(
        F.col("common_neighbor").alias("node_id"), "community_id"
    )
    printTrace("propagated_src", propagated_src)

    propagated_dst = only_src_coms.join(cn_coms, on="community_id", how="inner").select(
        F.col("common_neighbor").alias("node_id"), "community_id"
    )

    # Combine propagated updates
    printTrace("propagated_dst", propagated_dst)
    propagated = propagated_src.union(propagated_dst).distinct()

    # Handle new communities if no propagation occurs
    new_communities = common_neighbors_exploded.join(
        shared_coms, on=["node_u", "node_v"], how="left_anti"
    ).withColumn("community_id", F.monotonically_increasing_id())

    new_communities_updates = new_communities.select(
        F.col("node_u").alias("node_id"), "community_id"
    )

    printTrace("propagated", propagated)

    # Combine all updates
    all_updates = propagated.union(new_communities_updates)
    printTrace("all_updates", all_updates)

    # Update the global community memberships DataFrame
    self.community_memberships = self.community_memberships.union(all_updates).distinct()


def add_to_community(self, node, community_id, tags):
    """
    Adds a node to a community in a streaming-friendly way using DataFrames.

    Args:
        node (str): Node to be added to the community.
        community_id (str): ID of the community.
        tags (str): Tags associated with the community, comma-separated.
    """
    # Create a DataFrame for the new community tag entry
    if not self.communityTagsDf:
        new_tags = self.communityTagsDf.sparkSession.createDataFrame(
            [(community_id, tags.split(','))],
            schema=["community_id", "tags"]
    )

    # Update communityTagsDf by unioning new tags and deduplicating
    updated_communityTagsDf = (
        self.communityTagsDf.unionByName(new_tags)
        .groupBy("community_id")
        .agg(F.flatten(F.collect_list("tags")).alias("tags"))
    )

    # Create a DataFrame for the new community-node mapping
    new_community = self.communitiesDf.sparkSession.createDataFrame(
        [(community_id, node)],
        schema=["community_id", "node"]
    )

    # Update communitiesDf by unioning new entries and deduplicating
    updated_communitiesDf = self.communitiesDf.unionByName(new_community).distinct()

    if not self.communityTagsDf:
        self.communityTagsDf = updated_communityTagsDf
    else:
        self.communityTagsDf.unionByName(updated_communityTagsDf)

    printTrace("communityTagsDf:", self.communityTagsDf)

    if not self.communitiesDf:
        self.communitiesDf = updated_communitiesDf
    else:
        self.communitiesDf.unionByName(updated_communitiesDf)

    printTrace("communitiesDf:", self.communitiesDf)


def detect_common_neighbors(self, edge_updates):
    """
    Detect common neighbors for edges in the batch using GraphFrames.
    """
    # Extract neighbors for each node in the graph
    neighbors = self.g.find("(a)-[]->(b)") \
        .select(F.col("a.id").alias("node"), F.col("b.id").alias("neighbor")) \
        .groupBy("node") \
        .agg(F.collect_list("neighbor").alias("neighbors"))

    # Join edge_updates with neighbors twice (for src and dst nodes)
    edge_neighbors = edge_updates \
        .join(neighbors.withColumnRenamed("node", "src"), on="src", how="left") \
        .withColumnRenamed("neighbors", "src_neighbors") \
        .join(neighbors.withColumnRenamed("node", "dst"), on="dst", how="left") \
        .withColumnRenamed("neighbors", "dst_neighbors")

    # Compute common neighbors
    common_neighbors_df = edge_neighbors.withColumn(
        "common_neighbors", F.array_intersect(F.col("src_neighbors"), F.col("dst_neighbors"))
    )

    # Filter edges with common neighbors
    common_neighbors_filtered = common_neighbors_df.filter(
        F.col("common_neighbors").isNotNull() & (F.size(F.col("common_neighbors")) > 0)
    )

    return common_neighbors_filtered

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