import datetime
import os
import string
import sys
import time
from typing import Tuple, Iterator, Any, Iterable

from graphframes import GraphFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import min
from pyspark.sql.streaming.state import GroupState
from pyspark.sql.types import StructType, StringType, StructField, ArrayType, IntegerType

import pandas as pd
import pyarrow

if sys.version_info > (2, 7):
    pass
else:
    pass


__author__ = "Giulio Rossetti"
__contact__ = "giulio.rossetti@gmail.com"
__website__ = "about.giuliorossetti.net"
__license__ = "BSD"

from pyspark.sql import DataFrame


class TILES:
    def __init__(self, ttl=float('inf'), obs=7, path="", start=None, end=None, spark=None):
        self.path = path
        self.ttl = ttl
        self.cid = 0
        self.actual_slice = 0
        self.vertices: DataFrame = None
        self.edges: DataFrame = None
        self.g: GraphFrame = None
        self.communityTagsDf: DataFrame = None
        self.base = os.getcwd()
        self.status = open(f"{self.base}/{path}/extraction_status.txt", "w")
        self.representation = open(f"{self.base}/{path}/representation.txt", "w")
        self.removed = 0
        self.added = 0
        self.start = start
        self.end = end
        self.obs = obs
        self.communities = {}

        self.edge_buffer = []

        self.last_break = None  # Initialize last_break as None to indicate first batch
        self.actual_time = None  # Initialize actual_time to be set during first batch
        self.sliceCount = 0

        self.vertices_path = "/home/bigdata/PycharmProjects/SparkStreamingCotiles/parquet/vertices"
        self.edges_path = "/home/bigdata/PycharmProjects/SparkStreamingCotiles/parquet/edges"

        self.spark = spark

        self.vertices_schema = StructType([
            StructField("id", StringType(), True),
            StructField("c_coms", ArrayType(StringType()), True)
        ])
        self.edges_schema = StructType([
            StructField("src", StringType(), True),
            StructField("dst", StringType(), True),
            StructField("timestamp", IntegerType(), True),
            StructField("tags", ArrayType(StringType()), True),
            StructField("weight", IntegerType(), True)
        ])

    def execute(self, batch_df: DataFrame, batch_id):
        """
        Execute TILES algorithm on streaming data using foreachBatch.
        """
        self.status.write(u"Started! (%s) \n\n" % str(time.asctime(time.localtime(time.time()))))
        self.status.flush()

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
            first_min_timestamp =  batch_df.select(min("timestamp").alias("min_timestamp")).first()["min_timestamp"]
            if first_min_timestamp is not None:
                self.actual_time = datetime.datetime.fromtimestamp(float(first_min_timestamp))
                self.last_break = self.actual_time
                printMsg(f"First timestamp received {self.actual_time}, timestamp {first_min_timestamp.__str__}")

        # self.added += 1

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
                # self.sliceCount += 1
                # print(f"New slice detected starting from {self.actual_time}. Processed batch {batch_id}. Slice Count: {self.sliceCount}")
                printMsg(f"~ NEW actual_time = {self.actual_time}")
                # print("~~ do something now that a slice is detected\n")



        # to_remove_df = theDataframe.filter(F.col("action") == "-")
        # 1 TODO remove_edge
        # remove_edge_from_graph(self.g, to_remove_df)

        # Filter valid edges
        theDataframe = batch_df.filter(F.col("nodeU") != F.col("nodeV"))

        new_nodes = (
            theDataframe.select(F.col("nodeU").alias("id"))
            .union(theDataframe.select(F.col("nodeV").alias("id")))
            .distinct()
            .withColumn("c_coms", F.array().cast(ArrayType(StringType())))  # Add empty array column for communities
        )

        new_edges = theDataframe.select(F.col("nodeU").alias("src"),
                                             F.col("nodeV").alias("dst"),
                                             F.col("timestamp"),
                                             F.col("tags"),
                                             F.lit(1).alias("weight"))

        # Load previous state
        try:
            vertices_state = self.spark.read.format("parquet").load(self.vertices_path)
        except Exception:
            vertices_state = self.spark.createDataFrame([], self.vertices_schema)

        try:
            edges_state = self.spark.read.format("parquet").load(self.edges_path)
        except Exception:
            edges_state = self.spark.createDataFrame([], self.edges_schema)

        new_nodes_filtered = new_nodes.join(vertices_state.select("id"), on="id", how="left_anti").dropDuplicates()
        new_edges_filtered = new_edges.join(edges_state.select("src", "dst"), on=["src", "dst"], how="left_anti").dropDuplicates()

        # Update state
        updated_vertices = vertices_state.unionByName(new_nodes_filtered).dropDuplicates()
        updated_edges = edges_state.unionByName(new_edges_filtered).dropDuplicates()

        # Persist updated state back to storage
        updated_vertices.write.mode("overwrite").format("parquet").save(self.vertices_path)
        updated_edges.write.mode("overwrite").format("parquet").save(self.edges_path)

        # Reload updated state from Parquet to ensure it includes all persisted data
        all_vertices = self.spark.read.format("parquet").load(self.vertices_path)
        all_edges = self.spark.read.format("parquet").load(self.edges_path)

        # # Print all vertices and edges
        # print("All vertices processed so far:")
        # all_vertices.show()
        #
        # print("All edges processed so far:")
        # all_edges.show()

        # if self.vertices is None or self.edges is None:
        #     if not self.vertices:
        #         self.vertices = new_nodes
        #     if not self.edges:
        #         self.edges = new_edges
        # else:
        # Filter new vertices
        # new_nodes_filtered = new_nodes.join(self.vertices.select("id"), on="id", how="left_anti").dropDuplicates()
        #
        # # Filter new edges
        # new_edges_filtered = new_edges.join(self.edges.select("src", "dst"), on=["src", "dst"],
        #                                     how="left_anti").dropDuplicates()
        #
        # printTrace("new_nodes_filtered", new_nodes_filtered)
        # printTrace("new_edges_filtered", new_edges_filtered)

#         def process_vertices(group_key: Any, rows: Iterable[pd.DataFrame]) -> pd.DataFrame:
#             """
#             This function processes the incoming rows and returns the updated vertices state as a Pandas DataFrame.
#             It combines the state from the current microbatch and the previous state.
#             """
#             # Convert incoming rows to a single Pandas DataFrame
#             pdf = pd.concat(rows)
#
#             # Retrieve the current state if it exists
#             if hasattr(process_vertices, 'state'):
#                 current_state = process_vertices.state
#             else:
#                 current_state = pd.DataFrame(columns=["id", "c_coms"])
#
#             # Combine the current state with the new data (new nodes)
#             updated_state = pd.concat([current_state, pdf]).drop_duplicates(subset=["id"])
#
#             # Save the updated state back to the function for future batches
#             process_vertices.state = updated_state  # Keep the state for the next batch
#
#             return updated_state
#
#         def process_edges(group_key: Any, rows: Iterable[pd.DataFrame]) -> pd.DataFrame:
#             """
#             This function processes the incoming rows and returns the updated edges state as a Pandas DataFrame.
#             It combines the state from the current microbatch and the previous state.
#             """
#             # Convert incoming rows to a single Pandas DataFrame
#             pdf = pd.concat(rows)
#
#             # Retrieve the current state if it exists
#             if hasattr(process_edges, 'state'):
#                 current_state = process_edges.state
#             else:
#                 current_state = pd.DataFrame(columns=["src", "dst", "timestamp", "tags", "weight"])
#
#             # Combine the current state with the new data (new edges)
#             updated_state = pd.concat([current_state, pdf]).drop_duplicates(subset=["src", "dst"])
#
#             # Save the updated state back to the function for future batches
#             process_edges.state = updated_state  # Keep the state for the next batch
#
#             return updated_state
#
#         # def process_vertices(group_key: Any, rows: Iterable[pd.DataFrame], state: GroupState) -> Iterable[pd.DataFrame]:
#         #     # Convert rows (Iterable of Pandas DataFrames) into a single Pandas DataFrame
#         #     pdf = pd.concat(rows)
#         #     # Retrieve the current state
#         #     if state.exists:
#         #         current_state = pd.DataFrame(state, columns=["id", "c_coms"])
#         #     else:
#         #         current_state = pd.DataFrame(columns=["id", "c_coms"])
#         #     # Combine the current state with the incoming data
#         #     updated_state = pd.concat([current_state, pdf]).drop_duplicates(subset=["id"])
#         #     # Update the state
#         #     state.update((updated_state.to_dict(orient="records")))
#         #     # Return the updated state as an iterable (e.g., a list containing a single DataFrame)
#         #     yield updated_state
#         #
#         # def process_edges(group_key: Any, rows: Iterable[pd.DataFrame], state: GroupState) -> Iterable[pd.DataFrame]:
#         #     # Convert rows (Iterable of Pandas DataFrames) into a single Pandas DataFrame
#         #     pdf = pd.concat(rows)
#         #     # Retrieve the current state
#         #     if state.exists:
#         #         current_state = pd.DataFrame(state, columns=["src", "dst", "timestamp", "tags", "weight"])
#         #     else:
#         #         current_state = pd.DataFrame(columns=["src", "dst", "timestamp", "tags", "weight"])
#         #     # Combine the current state with the incoming data
#         #     updated_state = pd.concat([current_state, pdf]).drop_duplicates(subset=["src", "dst"])
#         #     # Update the state
#         #     state.update((updated_state.to_dict(orient="records")))
#         #     # Return the updated state as an iterable (e.g., a list containing a single DataFrame)
#         #     yield updated_state
#
#         # vertices_state_df = new_nodes.withColumn("key", F.lit("vertices")).groupBy("key").applyInPandasWithState(
#         #     func=process_vertices,
#         #     stateStructType=self.vertices_schema,
#         #     outputStructType=self.vertices_schema,
#         #     outputMode="update",
#         #     timeoutConf="NoTimeout"
#         # )
#         #
#         # # Use applyInPandasWithState for edges
#         # edges_state_df = new_edges.withColumn("key", F.lit("edges")).groupBy("key").applyInPandasWithState(
#         #     func=process_edges,
#         #     stateStructType=self.edges_schema,
#         #     outputStructType=self.edges_schema,
#         #     outputMode="update",
#         #     timeoutConf="NoTimeout"
#         # )
#
# # NOTHING WORKS IN PYSPARK API - TRY STATE STORAGE SMH - OTHERWISE JUMP INTO SCALA ??????
#         vertices_state_df = new_nodes.withColumn("key", F.lit("vertices")).groupBy("key").applyInPandas(
#             func=process_vertices,
#             schema=self.vertices_schema
#         )
#
#         edges_state_df = new_edges.withColumn("key", F.lit("edges")).groupBy("key").applyInPandas(
#             func=process_edges,
#             schema=self.edges_schema
#         )

        # # Update vertices and edges
            # updated_vertices = self.vertices.unionByName(new_nodes_filtered).distinct()
            # updated_edges = self.edges.unionByName(new_edges_filtered).distinct()
            #
            # printTrace("updated_vertices", updated_vertices)
            # printTrace("updated_edges", updated_edges)
            #
            # self.vertices = updated_vertices
            # self.edges = updated_edges

        printMsg("before theDataframe")
        if all_edges:
            all_edges = update_graph_with_edges(all_edges, new_edges)

        saveState(self, edges=all_edges)

        all_vertices, all_edges = loadState(self)
        # Add or update the edges in the GraphFrame
        # if self.g:
        #     self.g = update_graph_with_edges(self.g, new_edges)

        # common_neighbors = evolution(all_edges, new_edges)

        printMsg(" Before detect_common_neighbors")
        common_neighbors = detect_common_neighbors(all_edges, new_edges)
        printMsg(" After detect_common_neighbors")
        printTrace("Common Neighbors:", common_neighbors)

        common_neighbors_analysis(self, all_vertices, common_neighbors)


        # Show the updated edges (debugging)
        printMsg("graph edges:")
        # self.g.edges.show(truncate=False)
        printMsg("graph nodes:")
        # self.g.vertices.show(truncate=False)

def saveState(self, vertices: DataFrame = None, edges: DataFrame = None):
    """
    Saves the current state of vertices and edges to Parquet files.
    """
    if vertices is not None and not vertices.isEmpty():
        print("Saving state of vertices: ")
        print(vertices.show())
        vertices.write.mode("overwrite").format("parquet").save(self.vertices_path)

    if edges is not None and not edges.isEmpty():
        print("Saving state of edges: ")
        print(edges.show())
        edges.write.mode("overwrite").format("parquet").save(self.edges_path)


def loadState(self):
    """
    Loads the state of vertices and edges from Parquet files.
    If no state exists, returns empty DataFrames with the appropriate schema.
    """
    try:
        vertices = self.spark.read.format("parquet").load(self.vertices_path)
    except Exception:  # Handle case where file does not exist
        vertices = self.spark.createDataFrame([], self.vertices_schema)

    try:
        edges = self.spark.read.format("parquet").load(self.edges_path)
    except Exception:  # Handle case where file does not exist
        edges = self.spark.createDataFrame([], self.edges_schema)

    return vertices, edges


def common_neighbors_analysis(self, all_vertices: DataFrame, common_neighbors: DataFrame):
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

    # Step 3: Join with vertices DataFrame once
    # Perform the join once and get community information for both src and dst
    shared_coms = (
        common_neighbors_reshaped
        .join(all_vertices, (F.col("src") == F.col("id")) | (F.col("dst") == F.col("id")), how="left")
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

    printMsg("common_neighbors_analysis: AFTER single join with vertices")

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
    printMsg("common_neighbors_analysis: AFTER common_neighbors double join w/ vertices")


    printMsg("common_neighbors_analysis: BEFORE intersecting")
    # Step 3: Compute derived columns - shared_coms, only_u, and only_v
    shared_coms = (
        shared_coms
        .withColumn("shared_coms", F.array_intersect(F.col("src_coms"), F.col("dst_coms")))
        .withColumn("only_u", F.array_except(F.col("src_coms"), F.col("dst_coms")))
        .withColumn("only_v", F.array_except(F.col("dst_coms"), F.col("src_coms")))
    )
    printMsg("common_neighbors_analysis: AFTER intersecting")

    # Step 4: Rename and reselect columns in the final DataFrame
    printMsg("common_neighbors_analysis: BEFORE shared_coms selecting")
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
    printMsg("common_neighbors_analysis: AFTER shared_coms selecting")


    printMsg("shared_coms")
    # printTrace("shared_coms", shared_coms)

    # Propagate 'src' logic: Propagate src if 'common_neighbor' is in 'only_v'
    printMsg("common_neighbors_analysis: BEFORE shared_coms propagated expression ")
    propagated = shared_coms.withColumn(
        "propagated",
        F.when(
            F.expr(
                "size(array_intersect(common_neighbors, only_v)) > 0 OR"
                " size(array_intersect(common_neighbors, only_u)) > 0"),
            True
        ).otherwise(False)
    )
    printMsg("common_neighbors_analysis: AFTER shared_coms propagated expression ")

    #  TODO check common_neighbors list might need an exploded one

    printMsg("propagated")
    # printTrace("propagated", propagated)

    propagated = propagated.withColumn("community_id", F.monotonically_increasing_id())

    # printTrace("new_communities(propagated)", propagated)

    printMsg("common_neighbors_analysis: BEFORE shared_coms nodesToBeAddedToCom = expression ")

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

    printMsg(" BEFORE add to community") # TODO Handle prexistingCommunities !!
    print("nodesToBeAddedToCom:")
    print(nodesToBeAddedToCom.show())

    # add_to_community(self, nodesToBeAddedToCom)
    printMsg(" AFTER add to community")
    print("nodesToBeAddedToCom:")
    print(prexistingCommunities.show())


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

# def detect_common_neighbors2(self, edge_updates):
#     """
#     Detect common neighbors for edges in the batch using GraphFrames.
#     Optimized to use a single DataFrame.
#     """
#
#     # Step 1: Extract neighbors for each node in the graph
#     edges = self.g.edges
#     neighbors = (
#         edges.select(F.col("src").alias("node"), F.col("dst").alias("neighbor"))
#         .union(edges.select(F.col("dst").alias("node"), F.col("src").alias("neighbor")))
#         .groupBy("node")
#         .agg(F.collect_list("neighbor").alias("neighbors"))
#     )
#
#     # Step 2 to Step 4: Combine edge_updates and neighbors and compute common neighbors in one DataFrame
#     common_neighbors_filtered = (
#         edge_updates
#         .join(neighbors.withColumnRenamed("node", "src"), on="src", how="left")
#         .withColumnRenamed("neighbors", "src_neighbors")
#         .join(neighbors.withColumnRenamed("node", "dst"), on="dst", how="left")
#         .withColumnRenamed("neighbors", "dst_neighbors")
#         .withColumn("common_neighbors", F.array_intersect(F.col("src_neighbors"), F.col("dst_neighbors")))
#         .filter(F.col("common_neighbors").isNotNull() & (F.size(F.col("common_neighbors")) > 0))
#         .select("dst", "src", "timestamp", "tags", "weight", "common_neighbors")
#     )
#
#     return common_neighbors_filtered

def detect_common_neighbors(all_edges: DataFrame, edge_updates):
    """
    Detect common neighbors for edges in the batch using GraphFrames.
    """

    # Step 1: Extract neighbors for each node in the graph using a simpler approach
    printMsg("detect_common_neighbors - Before Union")

    neighbors = (
        all_edges.select(F.col("src").alias("node"), F.col("dst").alias("neighbor"))
        .union(all_edges.select(F.col("dst").alias("node"), F.col("src").alias("neighbor")))
        .groupBy("node")
        .agg(F.collect_list("neighbor").alias("neighbors"))
    )
    printMsg("detect_common_neighbors - After Union")

    # Step 2: Join edge_updates with neighbors for both src and dst nodes
    printMsg("detect_common_neighbors - Before double join")
    # Step 1: Flatten the neighbors DataFrame
    src_neighbors = (
        neighbors.withColumnRenamed("node", "src")
        .withColumnRenamed("neighbors", "src_neighbors")
    )

    dst_neighbors = (
        neighbors.withColumnRenamed("node", "dst")
        .withColumnRenamed("neighbors", "dst_neighbors")
    )

    # Step 2: Perform one join with flattened neighbor DataFrames
    edge_neighbors = edge_updates.join(src_neighbors, on="src", how="left") \
        .join(dst_neighbors, on="dst", how="left")

    # edge_neighbors = (
    #     edge_updates
    #     .join(neighbors.withColumnRenamed("node", "src"), on="src", how="left")
    #     .withColumnRenamed("neighbors", "src_neighbors")
    #     .join(neighbors.withColumnRenamed("node", "dst"), on="dst", how="left")
    #     .withColumnRenamed("neighbors", "dst_neighbors")
    # )
    printMsg("detect_common_neighbors - After double join")

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

def printMsg(msg):
    print(time.ctime() + " " + msg)
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

def evolution(all_edges: DataFrame, edge_updates: DataFrame):
    # Extract neighbors for both source (u) and destination (v)
    neighbors = all_edges.select(
        F.col("src").alias("node"),
        F.col("dst").alias("neighbor")
    ).union(
        all_edges.select(
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
    printMsg(str)
    # df.show()

def update_graph_with_edges(all_edges: DataFrame, new_edges: DataFrame):
    # Current edges in the graph
    existing_edges = all_edges.alias("existing")

    # Alias the new edges DataFrame
    new_edges = new_edges.alias("new")

    # printMsg("existing_edges:")
    # existing_edges.show()
    # printMsg("new_edges:")
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
    )

    # printMsg('updated_edges:')
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

    printMsg('added_edges:') # TODO CHECK THIS - MIGHT NOT WORKING - ALWAYS EMPTY
    added_edges.show()
    # Combine updated and added edges
    final_edges = updated_edges.unionByName(added_edges)

    # printMsg('final_edges:')
    # final_edges.show()

    # Return a new Dataframe with updated edges
    return final_edges