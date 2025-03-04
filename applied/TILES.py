import datetime
import os
import shutil
import string
import sys
import time

from graphframes import GraphFrame
from pyspark.sql import functions as F, Window
from pyspark.sql.functions import min
from pyspark.sql.types import StructType, StringType, StructField, ArrayType, IntegerType

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
        self.communities_path = "/home/bigdata/PycharmProjects/SparkStreamingCotiles/parquet/communities"
        self.communityTags_path = "/home/bigdata/PycharmProjects/SparkStreamingCotiles/parquet/communityTags"
        self.counter_path = "/home/bigdata/PycharmProjects/SparkStreamingCotiles/parquet/counter"

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

        self.communities_schema = StructType([
            StructField("cid", StringType(), True),
            StructField("nodes", ArrayType(StringType()), True),
        ])

        self.communityTags_schema = StructType([
            StructField("cid", StringType(), True),
            StructField("tags", ArrayType(StringType()), True)
        ])
        self.counter_schema = StructType([
            StructField("id", StringType(), True)
        ])

        self.clear_directory(directory_path=self.vertices_path)
        self.clear_directory(directory_path=self.edges_path)
        self.clear_directory(directory_path=self.communities_path)
        self.clear_directory(directory_path=self.communityTags_path)

    def clear_directory(self, directory_path: str):
        """
        Clears all the contents of the specified directory.
        If the directory does not exist, it creates it.
        """
        if os.path.exists(directory_path):
            # Remove all contents of the directory
            for filename in os.listdir(directory_path):
                file_path = os.path.join(directory_path, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)  # Remove file or symbolic link
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)  # Remove directory
                except Exception as e:
                    print(f"Failed to delete {file_path}. Reason: {e}")
        else:
            # Create the directory if it doesn't exist
            os.makedirs(directory_path)

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


        # 1 TODO remove_edge
        to_remove_df = batch_df.filter(F.col("action") == "-").select(F.col("nodeU").alias("src"),
                                                                      F.col("nodeV").alias("dst"),
                                                                      F.col("timestamp"),
                                                                      F.col("tags"),
                                                                      F.lit(1).alias("weight")).sort("timestamp")
        remove_edge(self, to_remove_df.filter(F.col("nodeU") != F.col("nodeV")))

        # Filter valid edges
        theDataframe = batch_df.filter(F.col("action") == "+").filter(F.col("nodeU") != F.col("nodeV"))

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
                                             F.lit(1).alias("weight")).sort("timestamp")

        # Load previous state
        vertices_state, edges_state = loadStateVerticesAndEdges(self)

        updated_edges = update_graph_with_edges(edges_state, new_edges)

        new_nodes_filtered = new_nodes.join(vertices_state.select("id"), on="id", how="left_anti")

        # Update state
        updated_vertices = vertices_state.unionByName(new_nodes_filtered).dropDuplicates(subset=["id"])

        # combined_edges = updated_edges.unionByName(edges_state)
        # printTrace("combined_edges (after union, before deduplication)", combined_edges)
        #
        # deduplicated_edges = combined_edges.dropDuplicates(subset=["src", "dst"])
        # printTrace("deduplicated_edges (after dropDuplicates)", deduplicated_edges)

        # updated_edges = updated_edges.unionByName(edges_state).dropDuplicates(subset=["src", "dst"])
        # printTrace("updated_edges (before save state)", updated_edges)

        # printTrace("updated_vertices (before save)", updated_vertices)
        # printTrace("updated_edges (before save)", updated_edges)
        # printTrace("firstTimeEdges (before save)", firstTimeEdges)
        # Persist updated state back to storage

        # saveStateVerticesAndEdges(self, updated_vertices, updated_edges)

        # printTrace("updated_vertices (after save)", updated_vertices)
        # printTrace("updated_edges (after save)", updated_edges)

        # Detect edges that are getting processed for the first time
        # Only those edges are getting neighbor analyzed
        normalized_new_edges = normalize_edges(new_edges)
        normalized_all_edges = normalize_edges(edges_state)
        printTrace("normalized_new_edges", normalized_new_edges)
        printTrace("normalized_all_edges", normalized_all_edges)
        first_edges = normalized_new_edges.join(
            normalized_all_edges,
            on=["src", "dst"],
            how="left_anti"
        ).sort("timestamp")
        printTrace("first_edges", first_edges)
        firstTimeEdges = (first_edges.groupBy("src", "dst").agg(
            F.first("timestamp").alias("timestamp"),
            F.first("tags").alias("tags"),
            # F.sum("weight").alias("weight")
            F.first("weight").alias("weight")
        ).sort("timestamp"))
        # printTrace("firstTimeEdges (after save)", firstTimeEdges)

        # Reload updated state from Parquet to ensure it includes all persisted data
        # all_vertices, all_edges = loadStateVerticesAndEdges(self)

        all_vertices, all_edges = updated_vertices, updated_edges

        # printTrace("new edges: (before evo)", new_edges)
        # printTrace("updated_edges: (before evo)", updated_edges)
        # printTrace("all all_edges: (before evo)", all_edges)
        printTrace("firstTimeEdges: ", firstTimeEdges)

        # common_neighbors = common_neighbor_detection(updated_edges, firstTimeEdges)
        common_neighbors = common_neighbor_detection(all_edges, firstTimeEdges)
        printTrace("Common_neighbors: ", common_neighbors)
        # print(f"all_vertices count BEFORE saving/loading: {all_vertices.count()}")
        # print(f"all_edges count BEFORE saving/loading: {all_edges.count()}")


        # test saving in last step VV
        # saveStateVerticesAndEdges(self, updated_vertices, updated_edges)
        # all_vertices, all_edges = loadStateVerticesAndEdges(self)
        # printTrace("all_vertices AFTER neighbors detection loaded", all_vertices)
        # printTrace("all_vertices AFTER neighbors detection: ", all_vertices)
        # printTrace("all_edges AFTER neighbors detection loaded: ", all_edges)
        # printTrace("Common_neighbors AFTER Saving/Loading ", common_neighbors)

        # common_neighbors = detect_common_neighbors(all_edges, new_edges)
        # printMsg(" After detect_common_neighbors")
        # printTrace("Common Neighbors:", common_neighbors)

        # common_neighbors_analysis(self, all_vertices, common_neighbors)

        propagated_edges, new_community_edges = None, None

        # propagated_edges, new_community_edges = analyze_common_neighbors(self, common_neighbors, all_vertices)
        (exploded_only_u_communities, exploded_only_v_communities,
                exploded_shared_coms_communities, exploded_neighbors) = analyze_common_neighbors(self, common_neighbors, all_vertices)

        add_community_neighbors(self, exploded_neighbors, exploded_only_u_communities, exploded_only_v_communities,
                                exploded_shared_coms_communities, all_vertices)

        # printTrace("propagated_edges", propagated_edges)
        # printTrace("new_community_edges", new_community_edges)


        # if new_community_edges is not None:
        #     communitiesDf = loadState(self=self, pathToload=self.communities_path, schemaToCreate=self.communities_schema)
        #     communityTagsDf = loadState(self=self, pathToload=self.communityTags_path, schemaToCreate=self.communityTags_schema)
        #
        #     updated_community_tags, updated_communities, updated_vertices = add_to_community_streaming(all_vertices, new_community_edges, communitiesDf, communityTagsDf)
        #
            # saveState(updated_community_tags, self.communityTags_path)
            # saveState(updated_communities, self.communities_path)
            # saveState(updated_vertices, self.vertices_path)

        saveStateVerticesAndEdges(self, updated_vertices, updated_edges)
        all_vertices, all_edges = loadStateVerticesAndEdges(self)
        printTrace("all_vertices Loaded - Last step", all_vertices)
        printTrace("all_edges Loaded - Last step: ", all_edges)

        # Show the updated edges (debugging)
        # printMsg("graph edges:")
        # self.g.edges.show(truncate=False)
        # printMsg("graph nodes:")
        # self.g.vertices.show(truncate=False)


def add_community_neighbors(self, exploded_neighbors: DataFrame, exploded_only_u_communities:DataFrame, exploded_only_v_communities:DataFrame, exploded_shared_coms_communities: DataFrame, all_vertices: DataFrame) -> (DataFrame, DataFrame):
    if exploded_neighbors is None or exploded_neighbors.isEmpty():
        return

    exploded_neigh_communities = (
        exploded_neighbors
        .join(all_vertices.alias("all_nodes"), F.col("neighbor") == F.col("all_nodes.id"), "left")
        .withColumn("neighbor_community", F.when(F.col("all_nodes.c_coms").isNotNull(), F.col("all_nodes.c_coms")).otherwise(F.array()))
        .withColumn("neighbor_community", F.explode(F.col("neighbor_community")))   # -> skip null rows
    )

    only_v_coms_with_neigh_coms = ((exploded_only_v_communities.alias("only_v_coms")
            .join(exploded_neigh_communities.alias("neigh_coms"), F.col("neighbor_community") == F.col("only_v_exploded"), how="left_outer"))
            .select(F.col("neigh_coms.node_u").cast("string"), F.col("neigh_coms.node_v").cast("string"),
                        F.col("neigh_coms.common_neighbors"), F.col("neigh_coms.tags"), F.col("neigh_coms.timestamp"), F.col("neigh_coms.weight"),
                        F.col("neigh_coms.shared_coms"), F.col("neigh_coms.only_u"), F.col("neigh_coms.only_v"),
                        F.col("neigh_coms.neighbor_communities"), F.col("neigh_coms.neighbor_community")
    ).distinct())
    only_v_coms_with_neigh_coms = only_v_coms_with_neigh_coms.withColumnRenamed("neighbor_community", "new_community_id").withColumn("nodes", F.array("node_u"))

    # printTrace("only_v_coms_with_neigh_coms", only_v_coms_with_neigh_coms)

    add_to_community_streaming(self, all_vertices, only_v_coms_with_neigh_coms)

    only_u_coms_with_neigh_coms = ((exploded_only_u_communities.alias("only_u_coms")
             .join(exploded_neigh_communities.alias("neigh_coms"),
                F.col("neighbor_community") == F.col("only_u_exploded"), how="left_outer"))
               .select(F.col("neigh_coms.node_u").cast("string"), F.col("neigh_coms.node_v").cast("string"),
                    F.col("neigh_coms.common_neighbors"), F.col("neigh_coms.tags"), F.col("neigh_coms.timestamp"), F.col("neigh_coms.weight"),
                    F.col("neigh_coms.shared_coms"), F.col("neigh_coms.only_u"), F.col("neigh_coms.only_v"),
                    F.col("neigh_coms.neighbor_communities"), F.col("neigh_coms.neighbor_community")
            ).distinct())

    only_u_coms_with_neigh_coms = only_u_coms_with_neigh_coms.withColumnRenamed("neighbor_community", "new_community_id").withColumn("nodes", F.array("node_v"))

    # printTrace("only_u_coms_with_neigh_coms", only_u_coms_with_neigh_coms)

    add_to_community_streaming(self, all_vertices, only_u_coms_with_neigh_coms)

    # if not exploded_shared_coms_communities.isEmpty():
    shared_com_not_in_common_neigh_community = (
        exploded_shared_coms_communities.join(exploded_neigh_communities,
                                              F.col("neighbor_community") == F.col("shared_coms_exploded"), how="left_anti")
    ).withColumnRenamed("neighbor_community", "new_community_id")

    shared_com_not_in_common_neigh_community = shared_com_not_in_common_neigh_community.withColumn("nodes", F.array("neighbor"))
    # printTrace("shared_com_not_in_common_neigh_community", shared_com_not_in_common_neigh_community)

    add_to_community_streaming(self, all_vertices, shared_com_not_in_common_neigh_community)

    if exploded_neigh_communities.isEmpty() or (only_v_coms_with_neigh_coms.isEmpty() and only_u_coms_with_neigh_coms.isEmpty() and
                                                (shared_com_not_in_common_neigh_community is not None and shared_com_not_in_common_neigh_community.isEmpty())):
        printMsg("exploded_neigh_communities is empty thus create new community and add the nodes")
        new_community_edges = (exploded_neighbors
                               .select(
            F.col("node_u").cast("string"), F.col("node_v").cast("string"),
            F.col("common_neighbors"),
            F.col("tags"), F.col("timestamp"), F.col("weight"),
            F.col("shared_coms"), F.col("only_u"), F.col("only_v"))
                               .withColumn("new_community_id", F.expr("uuid()")))

        new_community_edges = new_community_edges.withColumn(
            "new_community_id",
            F.when(
                F.expr("size(shared_coms) > 0"),
                F.first("new_community_id").over(Window.partitionBy("node_u", "node_v"))
            ).otherwise(F.col("new_community_id"))
        ).sort("timestamp")

        add_to_community_streaming(self, all_vertices, new_community_edges)

    # if exploded_neigh_communities.isEmpty(): # no propagation - create new community and add the nodes
    #     printMsg("exploded_neigh_communities is empty thus create new community and add the nodes")
    #     new_community_edges = (exploded_neighbors
    #     .select(
    #         F.col("node_u").cast("string"), F.col("node_v").cast("string"),
    #         F.col("common_neighbors"),
    #         F.col("tags"), F.col("timestamp"), F.col("weight"),
    #         F.col("shared_coms"), F.col("only_u"), F.col("only_v"))
    #     .withColumn("new_community_id", F.expr("uuid()")))
    #
    #     new_community_edges = new_community_edges.withColumn(
    #         "new_community_id",
    #         F.when(
    #             F.expr("size(shared_coms) > 0"),
    #             F.first("new_community_id").over(Window.partitionBy("node_u", "node_v"))
    #         ).otherwise(F.col("new_community_id"))
    #     ).sort("timestamp")
    #
    #     add_to_community_streaming(self, all_vertices, new_community_edges)
    #     return
    # else:
    #     print("Place holder for exploded_neigh_communities is NOT empty, reprinting it and return")

    # if not exploded_shared_coms_communities.isEmpty():
    #     shared_com_not_in_common_neigh_community = (
    #         exploded_shared_coms_communities.join(exploded_neigh_communities,
    #                                               F.col("neighbor_community") == F.col("shared_coms_exploded"), how="left_anti")
    #     ).withColumnRenamed("neighbor_community", "new_community_id")
    #
    #     shared_com_not_in_common_neigh_community = shared_com_not_in_common_neigh_community.withColumn("nodes", F.array("neighbor"))
    #     printTrace("shared_com_not_in_common_neigh_community", shared_com_not_in_common_neigh_community)
    #
    #     add_to_community_streaming(self, all_vertices, shared_com_not_in_common_neigh_community)


    # only_u_coms_with_neigh_coms = ((exploded_only_u_communities.alias("only_u_coms")
    #          .join(exploded_neigh_communities.alias("neigh_coms"),
    #             F.col("neighbor_community") == F.col("only_u_exploded"), how="left_outer"))
    #            .select(F.col("neigh_coms.node_u").cast("string"), F.col("neigh_coms.node_v").cast("string"),
    #                 F.col("neigh_coms.common_neighbors"), F.col("neigh_coms.tags"), F.col("neigh_coms.timestamp"), F.col("neigh_coms.weight"),
    #                 F.col("neigh_coms.shared_coms"), F.col("neigh_coms.only_u"), F.col("neigh_coms.only_v"),
    #                 F.col("neigh_coms.neighbor_communities"), F.col("neigh_coms.neighbor_community")
    #         ).distinct())
    #
    # only_u_coms_with_neigh_coms = only_u_coms_with_neigh_coms.withColumnRenamed("neighbor_community", "new_community_id").withColumn("nodes", F.array("node_v"))
    # # only_u_coms_with_neigh_coms = only_u_coms_with_neigh_coms.withColumn("nodes", F.array("node_v"))
    #
    # printTrace("only_u_coms_with_neigh_coms", only_u_coms_with_neigh_coms)
    #
    # add_to_community_streaming(self, all_vertices, only_u_coms_with_neigh_coms)

    # only_v_coms_with_neigh_coms = ((exploded_only_v_communities.alias("only_v_coms")
    #         .join(exploded_neigh_communities.alias("neigh_coms"), F.col("neighbor_community") == F.col("only_v_exploded"), how="left_outer"))
    #         .select(F.col("neigh_coms.node_u").cast("string"), F.col("neigh_coms.node_v").cast("string"),
    #                     F.col("neigh_coms.common_neighbors"), F.col("neigh_coms.tags"), F.col("neigh_coms.timestamp"), F.col("neigh_coms.weight"),
    #                     F.col("neigh_coms.shared_coms"), F.col("neigh_coms.only_u"), F.col("neigh_coms.only_v"),
    #                     F.col("neigh_coms.neighbor_communities"), F.col("neigh_coms.neighbor_community")
    # ).distinct())
    # only_v_coms_with_neigh_coms = only_v_coms_with_neigh_coms.withColumnRenamed("neighbor_community", "new_community_id").withColumn("nodes", F.array("node_u"))
    # # only_v_coms_with_neigh_coms = only_v_coms_with_neigh_coms.withColumn("nodes", F.array("node_u"))
    #
    # printTrace("only_v_coms_with_neigh_coms", only_v_coms_with_neigh_coms)
    #
    # add_to_community_streaming(self, all_vertices, only_v_coms_with_neigh_coms)

def analyze_common_neighbors(self, common_neighbors: DataFrame, all_vertices: DataFrame) -> (DataFrame, DataFrame):
    if common_neighbors.isEmpty():
        return None, None, None, None

    common_neighbors = common_neighbors.filter(F.expr("size(common_neighbors) > 0"))

    if common_neighbors.isEmpty():
        return None, None, None, None

    common_neighbors = (
        common_neighbors
        .join(all_vertices.alias("u_info"), common_neighbors["node_u"] == F.col("u_info.id"), "left")
        .join(all_vertices.alias("v_info"), common_neighbors["node_v"] == F.col("v_info.id"), "left")
        .select(
            F.col("node_u"), F.col("node_v"),
            F.col("common_neighbors"),
            F.col("common_u_neighbor"),
            F.col("common_v_neighbor"),
            F.col("u_info.c_coms").alias("src_coms"),
            F.col("v_info.c_coms").alias("dst_coms"),
            F.col("tags"), F.col("timestamp"), F.col("weight")
        )
        # Ensure `src_coms` and `dst_coms` are non-null
        .withColumn("src_coms", F.when(F.col("src_coms").isNull(), F.array()).otherwise(F.col("src_coms")))
        .withColumn("dst_coms", F.when(F.col("dst_coms").isNull(), F.array()).otherwise(F.col("dst_coms")))
        # Compute `only_u` and `only_v`
        .withColumn("shared_coms", F.array_intersect(F.col("src_coms"), F.col("dst_coms")))
        .withColumn("only_u", F.array_except(F.col("src_coms"), F.col("dst_coms")))
        .withColumn("only_v", F.array_except(F.col("dst_coms"), F.col("src_coms")))
    ).sort("timestamp")

    # Explode the common_neighbors column to get individual neighbors
    exploded_neighbors = (
        common_neighbors
        .withColumn("neighbor", F.explode(F.col("common_neighbors")))
        .join(all_vertices.alias("all_vertices"), F.col("neighbor") == F.col("all_vertices.id"), "left")
        .withColumnRenamed("c_coms", "neighbor_communities")
        .drop("id")
    )

    # printTrace("all_vertices", all_vertices)
    # printTrace("exploded_neighbors", exploded_neighbors)

    # exploded_neighbor = (
    #     exploded_neighbors
    #     .withColumn("neighbor", F.explode(F.col("common_neighbors")))
    #     .join(all_vertices.alias("all_vertices"), F.col("neighbor") == F.col("all_vertices.id"), "left")
    #     .withColumn(
    #         "neighbor_community",
    #         F.when(
    #             F.col("all_vertices.c_coms").isNotNull() & (F.size(F.col("all_vertices.c_coms")) > 0),
    #             F.col("all_vertices.c_coms")
    #         ).otherwise(F.array(F.lit(None)))  # Add a placeholder for empty arrays
    #     )
    #     .withColumn("neighbor_community", F.explode(F.col("neighbor_community")))
    #     .withColumn("neighbor_community", F.when(F.col("neighbor_community").isNotNull(), F.col("neighbor_community")))
    # )

    # exploded_neigh_communities = (
    #     exploded_neighbors
    #     .join(all_vertices.alias("all_nodes"), F.col("neighbor") == F.col("all_nodes.id"), "left")
    #     .withColumn("neighbor_community", F.when(F.col("all_nodes.c_coms").isNotNull(), F.col("all_nodes.c_coms")).otherwise(F.array()))
    #     .withColumn("neighbor_community", F.explode(F.col("neighbor_community")))   # -> skip null rows
    # #   .withColumn("neighbor_community", F.explode_outer(F.col("neighbor_community")))     # -> adds null in the column if empty
    # )

    # printTrace("exploded_neigh_communities", exploded_neigh_communities)

        # .groupBy("node_u", "node_v", "common_neighbors", "src_coms", "dst_coms", "tags", "timestamp", "weight", "only_u", "only_v", "shared_coms")
        # .agg(F.collect_list(F.col("all_vertices.c_coms")).alias("neighbors_communities"))

    exploded_only_u_communities = exploded_neighbors.withColumn("only_u_exploded", F.explode(F.when(F.expr("size(only_u) > 0"), F.col("only_u"))
                                                                                            .otherwise(F.array())))
    exploded_only_v_communities = exploded_neighbors.withColumn("only_v_exploded", F.explode(F.when(F.expr("size(only_v) > 0"), F.col("only_v"))
                                                                                            .otherwise(F.array())))
    exploded_shared_coms_communities = exploded_neighbors.withColumn("shared_coms_exploded", F.explode(F.when(F.expr("size(shared_coms) > 0"), F.col("shared_coms"))
                                                                                            .otherwise(F.array())))
    # printTrace("exploded_only_u_communities", exploded_only_u_communities)
    # printTrace("exploded_only_v_communities", exploded_only_v_communities)
    # printTrace("exploded_shared_coms_communities", exploded_shared_coms_communities)

    return exploded_only_u_communities, exploded_only_v_communities, exploded_shared_coms_communities, exploded_neighbors

    # # join common neigh communities with shared coms find anti join
    # if not exploded_shared_coms_communities.isEmpty():
    #     shared_com_not_in_common_neigh_community = (
    #         exploded_shared_coms_communities.join(exploded_neigh_communities,
    #                                               F.col("neighbor_community") == F.col("shared_coms_exploded"), how="left_anti")
    #     ).withColumnRenamed("neighbor_community", "new_community_id")
    #
    #     shared_com_not_in_common_neigh_community = shared_com_not_in_common_neigh_community.withColumn("nodes", F.array("neighbor"))
    #     printTrace("shared_com_not_in_common_neigh_community", shared_com_not_in_common_neigh_community)
    #
    #     # check these files only 2 communities detected somethings very wrong
    #     communitiesDf1 = loadState(self=self, pathToload=self.communities_path, schemaToCreate=self.communities_schema)
    #     communityTagsDf1 = loadState(self=self, pathToload=self.communityTags_path,
    #                                 schemaToCreate=self.communityTags_schema)
    #
    #     updated_community_tags1, updated_communities1, updated_vertices1 = (
    #         add_to_community_streaming(all_vertices, shared_com_not_in_common_neigh_community, communitiesDf1, communityTagsDf1))
    #
    #     print("Following 3 DFs after shared_com_not_in_common_neigh_community was added")
    #     printTrace("updated_community_tags:", updated_community_tags1)
    #     printTrace("updated_communities:", updated_communities1)
    #     printTrace("updated_vertices:", updated_vertices1)
    #
    #     saveState(updated_community_tags1, self.communityTags_path)
    #     saveState(updated_communities1, self.communities_path)
    #     saveState(updated_vertices1, self.vertices_path)
    #
    #     # should add the shared coms id to the common neighbor of above df
    #
    # # join exploded_only_u_communities with exploded_shared_coms_communities - add the u node
    # # join exploded_only_v_communities with exploded_shared_coms_communities - add the v node
    #
    # # perhaps add to communities inside this method instead of returning dfs and passing them into different method
    # # if added inside this method i have to keep updated the state of required dfs
    #
    #
    # if exploded_neigh_communities.isEmpty(): # no propagation - create new community and add the nodes
    #     printMsg("exploded_neigh_communities is empty thus create new community and add the nodes")
    #     new_community_edges = (exploded_neighbors
    #     .select(
    #         F.col("node_u").cast("string"), F.col("node_v").cast("string"),
    #         F.col("common_neighbors"),
    #         F.col("tags"), F.col("timestamp"), F.col("weight"),
    #         F.col("shared_coms"), F.col("only_u"), F.col("only_v"))
    #     .withColumn("new_community_id", F.expr("uuid()")))
    #
    #     new_community_edges = new_community_edges.withColumn(
    #         "new_community_id",
    #         F.when(
    #             F.expr("size(shared_coms) > 0"),
    #             F.first("new_community_id").over(Window.partitionBy("node_u", "node_v"))
    #         ).otherwise(F.col("new_community_id"))
    #     ).sort("timestamp")
    #
    #     # printTrace("new_community_edges in if: ", new_community_edges)
    #     communitiesDf2 = loadState(self=self, pathToload=self.communities_path, schemaToCreate=self.communities_schema)
    #     communityTagsDf2 = loadState(self=self, pathToload=self.communityTags_path,
    #                                 schemaToCreate=self.communityTags_schema)
    #
    #     updated_community_tags2, updated_communities2, updated_vertices2 = (
    #         add_to_community_streaming(all_vertices, new_community_edges, communitiesDf2, communityTagsDf2))
    #
    #     print("Following 3 DFs after new_community_edges was added")
    #     printTrace("updated_community_tags:", updated_community_tags2)
    #     printTrace("updated_communities:", updated_communities2)
    #     printTrace("updated_vertices:", updated_vertices2)
    #
    #     saveState(updated_community_tags2, self.communityTags_path)
    #     saveState(updated_communities2, self.communities_path)
    #     saveState(updated_vertices2, self.vertices_path)
    #     return
    #
    # else:
    #     print("Place holder for exploded_neigh_communities is NOT empty, reprinting it and return")
    #     # printTrace("exploded_neigh_communities", exploded_neigh_communities)
    #
    # # if not exploded_only_u_communities.isEmpty():
    # # if exploded_only_u_communities.
    # only_u_coms_with_neigh_coms = ((exploded_only_u_communities.alias("only_u_coms")
    #          .join(exploded_neigh_communities.alias("neigh_coms"),
    #             F.col("neighbor_community") == F.col("only_u_exploded"), how="left_outer"))
    #            .select(F.col("neigh_coms.node_u").cast("string"), F.col("neigh_coms.node_v").cast("string"),
    #                 F.col("neigh_coms.common_neighbors"), F.col("neigh_coms.tags"), F.col("neigh_coms.timestamp"), F.col("neigh_coms.weight"),
    #                 F.col("neigh_coms.shared_coms"), F.col("neigh_coms.only_u"), F.col("neigh_coms.only_v"),
    #                 F.col("neigh_coms.neighbor_communities"), F.col("neigh_coms.neighbor_community")
    #         ).distinct())
    #
    # only_u_coms_with_neigh_coms = only_u_coms_with_neigh_coms.withColumnRenamed("neighbor_community", "new_community_id")
    # only_u_coms_with_neigh_coms = only_u_coms_with_neigh_coms.withColumn("nodes", F.array("node_v"))
    #
    # printTrace("only_u_coms_with_neigh_coms", only_u_coms_with_neigh_coms)
    #
    # only_v_coms_with_neigh_coms = ((exploded_only_v_communities.alias("only_v_coms")
    #         .join(exploded_neigh_communities.alias("neigh_coms"), F.col("neighbor_community") == F.col("only_v_exploded"), how="left_outer"))
    #         .select(F.col("neigh_coms.node_u").cast("string"), F.col("neigh_coms.node_v").cast("string"),
    #                     F.col("neigh_coms.common_neighbors"), F.col("neigh_coms.tags"), F.col("neigh_coms.timestamp"), F.col("neigh_coms.weight"),
    #                     F.col("neigh_coms.shared_coms"), F.col("neigh_coms.only_u"), F.col("neigh_coms.only_v"),
    #                     F.col("neigh_coms.neighbor_communities"), F.col("neigh_coms.neighbor_community")
    # ).distinct())
    # only_v_coms_with_neigh_coms = only_v_coms_with_neigh_coms.withColumnRenamed("neighbor_community", "new_community_id")
    # only_v_coms_with_neigh_coms = only_v_coms_with_neigh_coms.withColumn("nodes", F.array("node_u"))
    #
    # printTrace("only_v_coms_with_neigh_coms", only_v_coms_with_neigh_coms)
    #
    # # check only_u and only_v columns of both dfs something's sus there
    # # need to add also only_v-related df
    # # check if i ADD the correct edges, for only_u i should add the common_neighbors community it to V edge !! and vice versa for only_v -> u
    #
    # ### ONLY U ####
    # communitiesDf3 = loadState(self=self, pathToload=self.communities_path, schemaToCreate=self.communities_schema)
    # communityTagsDf3 = loadState(self=self, pathToload=self.communityTags_path,
    #                             schemaToCreate=self.communityTags_schema)
    #
    # updated_community_tags3, updated_communities3, updated_vertices3 = (
    #     add_to_community_streaming(all_vertices, only_u_coms_with_neigh_coms, communitiesDf3, communityTagsDf3))
    #
    # print("Following 3 DFs after only_u_coms_with_neigh_coms was added")
    # printTrace("updated_community_tags:", updated_community_tags3)
    # printTrace("updated_communities:", updated_communities3)
    # printTrace("updated_vertices:", updated_vertices3)
    #
    # saveState(updated_community_tags3, self.communityTags_path)
    # saveState(updated_communities3, self.communities_path)
    # saveState(updated_vertices3, self.vertices_path)
    # ### ONLY U ####
    #
    # #### ONLY V ####
    # communitiesDf4 = loadState(self=self, pathToload=self.communities_path, schemaToCreate=self.communities_schema)
    # communityTagsDf4 = loadState(self=self, pathToload=self.communityTags_path,
    #                             schemaToCreate=self.communityTags_schema)
    #
    # updated_community_tags4, updated_communities4, updated_vertices4 = (
    #     add_to_community_streaming(all_vertices, only_v_coms_with_neigh_coms, communitiesDf4, communityTagsDf4))
    #
    # print("Following 3 DFs after only_v_coms_with_neigh_coms was added")
    # printTrace("updated_community_tags:", updated_community_tags4)
    # printTrace("updated_communities:", updated_communities4)
    # printTrace("updated_vertices:", updated_vertices4)
    #
    # saveState(updated_community_tags4, self.communityTags_path)
    # saveState(updated_communities4, self.communities_path)
    # saveState(updated_vertices4, self.vertices_path)
    #### ONLY V ####

    # # Fill nulls in neighbors_communities with an empty array
    # common_neighbors = exploded_neighbors.withColumn(
    #     "neighbors_communities",
    #     F.when(F.col("neighbors_communities").isNull(), F.array()).otherwise(F.col("neighbors_communities"))
    # )


    # printTrace("common_neighbors NEW: ", exploded_neighbors)
    #
    # # Explode common_neighbors
    # common_neighbors = (
    #     common_neighbors
    #     .filter(F.expr("size(common_neighbors) > 0"))
    #     # .filter(F.size(F.col("common_neighbors")) > 0)
    #     # .withColumn("common_neighbor", F.explode(F.col("common_neighbors")))
    # )
    #
    # # Add propagated column
    # common_neighbors = (
    #     common_neighbors
    #     .withColumn(
    #         "propagated",
    #         F.expr(
    #             "size(array_intersect(only_v, src_coms)) > 0 OR "
    #             "size(array_intersect(only_u, dst_coms)) > 0  "
    #         )
    #     )
    # )
    #
    # # printTrace("common_neighbors propagated NEW: ", common_neighbors)
    #
    # # Step 5: Separate propagated edges
    # propagated_edges = (
    #     common_neighbors
    #     .filter(F.col("propagated") == True)
    #     .select(
    #         F.col("node_u"), F.col("node_v"),
    #         F.col("common_neighbors"),
    #         # F.col("common_neighbor"),
    #         F.col("tags"), F.col("timestamp"), F.col("weight"),
    #         F.col("shared_coms"), F.col("only_u"), F.col("only_v")
    #     )
    # )
    # # printTrace("propagated_edges NEW: ", propagated_edges)
    #
    # # Step 6: Identify new community edges
    # new_community_edges = (
    #     common_neighbors
    #     .filter(F.col("shared_coms").isNull() | (F.size(F.col("shared_coms")) == 0))
    #     .filter(F.col("propagated") == False)
    #     .select(
    #         F.col("node_u").cast("string"), F.col("node_v").cast("string"),
    #         F.col("common_neighbors"),
    #         # F.col("common_neighbor"),
    #         F.col("tags"), F.col("timestamp"), F.col("weight"),
    #         F.col("shared_coms"), F.col("only_u"), F.col("only_v")
    #     ).withColumn("new_community_id", F.expr("uuid()"))
    # )
    #
    # new_community_edges = new_community_edges.withColumn(
    #     "new_community_id",
    #     F.when(
    #         F.size(F.col("shared_coms")) > 0,
    #         F.first("new_community_id").over(Window.partitionBy("node_u", "node_v"))
    #     ).otherwise(F.col("new_community_id"))
    # ).sort("timestamp")
    # # TODO check common neighbor if found in shared_coms , last for loop of analysis method
    #
    # # printTrace("new_community_edges NEW: ", new_community_edges)
    #
    # return propagated_edges, new_community_edges


def common_neighbors_analysis(self, all_vertices: DataFrame, common_neighbors: DataFrame):
    """
    Analyze and update communities for nodes with common neighbors in a streaming manner.

    :param common_neighbors: DataFrame with columns ["src", "dst", "common_neighbors"]
    """
    if common_neighbors.isEmpty():
        return

    common_neighbors = common_neighbors.withColumnsRenamed({'node_u':'src', 'node_v':'dst'})

    # Step 1: Filter out null common_neighbors and explode to one row per common_neighbor
    common_neighbors = (
        common_neighbors
        .filter(F.col("common_neighbors").isNotNull())
        .withColumn("common_neighbor", F.explode(F.col("common_neighbors")))
    )

    printTrace("common_neighbors: ", common_neighbors)

    # Step 2: Reshape common_neighbors for a single join (avoiding double join)
    common_neighbors_reshaped = (
        common_neighbors
        .select(
            F.col("src").alias("node_id"),
            F.col("dst").alias("other_node_id"),
            F.col("tags"),
            F.col("timestamp"),
            F.col("weight"),
            F.col("common_neighbors")
        )
        .union(
            common_neighbors
            .select(
                F.col("dst").alias("node_id"),
                F.col("src").alias("other_node_id"),
                F.col("tags"),
                F.col("timestamp"),
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

    printTrace("common_neighbors_reshaped: ", common_neighbors_reshaped)

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

    printTrace("shared_coms: ", shared_coms)

    # printMsg("common_neighbors_analysis: AFTER single join with vertices")
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
    # printMsg("common_neighbors_analysis: AFTER common_neighbors double join w/ vertices")


    printMsg("common_neighbors_analysis: BEFORE intersecting")
    # Step 3: Compute derived columns - shared_coms, only_u, and only_v
    shared_coms = (
        shared_coms
        .withColumn("shared_coms", F.array_intersect(F.col("src_coms"), F.col("dst_coms")))
        .withColumn("only_u", F.array_except(F.col("src_coms"), F.col("dst_coms")))
        .withColumn("only_v", F.array_except(F.col("dst_coms"), F.col("src_coms")))
    )

    printTrace("shared_coms After intersecting arrays : ", shared_coms)

    # Step 4: Rename and reselect columns in the final DataFrame
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

    # Propagate 'src' logic: Propagate src if 'common_neighbor' is in 'only_v'
    propagated = shared_coms.withColumn(
        "propagated",
        F.when(
            F.expr(
                "size(array_intersect(common_neighbors, only_v)) > 0 OR"
                " size(array_intersect(common_neighbors, only_u)) > 0"),
            True
        ).otherwise(False)
    )

    #  TODO check common_neighbors list might need an exploded one

    printTrace("propagated: ", propagated)
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

    printTrace("node propagted FASLE", nodesToBeAddedToCom)
    printTrace("node propagted TRUE", prexistingCommunities)

    # printMsg(" BEFORE add to community") # TODO Handle prexistingCommunities !!
    # print("nodesToBeAddedToCom:")
    # print(nodesToBeAddedToCom.show())

    # add_to_community(self, nodesToBeAddedToCom)
    # printMsg(" AFTER add to community")
    # print("nodesToBeAddedToCom:")
    # print(prexistingCommunities.show())


    # if prexistingCommunities is not None:
    #     printTrace("prexistingCommunities", prexistingCommunities)

    # Step 4: Handle new communities if no propagation occurs TODO this
    # new_communities = common_neighbors_exploded.join(
    #     shared_coms, on=["src", "dst"], how="left_anti"
    # ).withColumn("community_id", F.monotonically_increasing_id())

def add_to_community_streaming(self, all_vertices:DataFrame, new_community_edges:DataFrame):
    """
    Args:
        new_community_edges: DataFrame containing new community information with columns
                              [node_u, node_v, common_neighbor, tags, new_community_id]
        communitiesDf: DataFrame containing existing communities with columns [cid, nodes]
        communityTagsDf: DataFrame containing community tags with columns [cid, tags]

    Returns:
        Updated communitiesDf and communityTagsDf as DataFrames
    """
    if new_community_edges.isEmpty():
        return

    communitiesDf = loadState(self=self, pathToload=self.communities_path, schemaToCreate=self.communities_schema)
    communityTagsDf = loadState(self=self, pathToload=self.communityTags_path, schemaToCreate=self.communityTags_schema)

    # Step 1: Process and aggregate tags by new_community_id
    new_tags = (
        new_community_edges
        .select(F.col("new_community_id").alias("cid"), "tags")
        .groupBy("cid")
        .agg(F.array_distinct(F.flatten(F.collect_list("tags"))).alias("tags"))
    )

    # printTrace("new_tags: ", new_tags)

    # Step 2: Process and aggregate community nodes (node_u, node_v, common_neighbor)
    new_communities = (
        new_community_edges
        .select(F.col("new_community_id").alias("cid"), "node_u", "node_v", "common_neighbors")
        .withColumn("nodes", F.array_union(F.array("node_u", "node_v"), "common_neighbors"))
        .withColumn("nodes", F.when((F.col("nodes").isNull()) | F.expr("size(nodes) > 0"),
                                    F.array_union(F.array("node_u", "node_v"), "common_neighbors")).otherwise(F.col("nodes")))
        # F.when(F.col("src_coms").isNull(), F.array()).otherwise(F.col("src_coms")))
        .groupBy("cid")
        .agg(F.array_distinct(F.flatten(F.collect_list("nodes"))).alias("nodes"))
    )


    # CHECK THOSE DFS AFTER LOADING/SAVING I only get this
    #Thu Feb 13 19:52:59 2025 communityTagsDf:
    # +--------------------+--------------------+
    # |                 cid|                tags|
    # +--------------------+--------------------+
    # |b9fcffa5-4d50-47e...|[untagged, unix, ...|
    # |cf574624-b192-474...|[unix, command-li...|
    # |                NULL|                  []|
    # |1d6ecd17-4d3a-412...|[debugging, memor...|
    # |7ef9bac1-1070-473...|[command-line, le...|
    # +--------------------+--------------------+
    # printTrace("new_communities: ", new_communities)
    # printTrace("communitiesDf: (inside add() first loaded)", communitiesDf)
    # printTrace("communityTagsDf: (inside add() first loaded)", communityTagsDf)

    # Step 3: Combine and update communityTags and communities
    if communityTagsDf.isEmpty():
        updated_community_tags = new_tags.withColumnRenamed("new_community_id", "cid")
    else:
        updated_community_tags = (new_tags.alias("new")
        .join(communityTagsDf.alias("old"),
            F.col("old.cid") == F.col("new.cid"),
            "full_outer")
        .select(
            F.coalesce(F.col("new.cid"), F.col("old.cid")).alias("cid"),
            F.when(
                F.col("new.tags").isNotNull() & F.col("old.tags").isNotNull(),
                F.array_union(F.col("new.tags"), F.col("old.tags"))
            )
            .when(F.col("new.tags").isNotNull(), F.col("new.tags"))
            .otherwise(F.col("old.tags")).alias("tags")
        )
    )

    if communitiesDf.isEmpty():
        updated_communities = new_communities.withColumnRenamed("new_community_id", "cid")
    else:
        updated_communities = (new_communities.alias("new_coms")
        .join(
            communitiesDf.alias("old_coms"),
            F.col("old_coms.cid") == F.col("new_coms.cid"),
            "full_outer"
        )
        .select(
            F.coalesce(F.col("new_coms.cid"), F.col("old_coms.cid")).alias("cid"),
            F.when(
                F.col("new_coms.nodes").isNotNull() & F.col("old_coms.nodes").isNotNull(),
                F.array_union(F.col("new_coms.nodes"), F.col("old_coms.nodes"))
            )
            .when(F.col("new_coms.nodes").isNotNull(), F.col("new_coms.nodes"))
            .otherwise(F.col("old_coms.nodes")).alias("nodes")
        )
    )
        # updated_communities = communitiesDf.join(
        #     new_communities,
        #     F.col("communitiesDf.cid") == F.col("new_communities.cid"),
        #     "outer"
        # ).select(
        #     F.coalesce(new_communities.cid, communitiesDf.cid),
        #     F.coalesce(new_communities.nodes, communitiesDf.nodes).alias("nodes")
        # )

    # printTrace("updated_communities before new changes: ", updated_communities)

    community_nodes = updated_communities.select(F.explode("nodes").alias("id"), F.col("cid").cast("string")).distinct()
    community_nodes = community_nodes.groupBy("id").agg(
        F.collect_list("cid").alias("cids")
    )

    # printTrace("community_nodes in add: ", community_nodes)
    # printTrace("all vertices in add: ", all_vertices)

    all_vertices = (all_vertices.withColumn("id", F.col("id").cast("string")).
                    withColumn("c_coms", F.col("c_coms").cast("array<string>")))
    community_nodes = (community_nodes.withColumn("id", F.col("id").cast("string"))
                       .withColumn("cids", F.col("cids").cast("array<string>")))

    updated_vertices = (community_nodes.alias("community").join(
        all_vertices.alias("vertices"),
        F.col("vertices.id") == F.col("community.id"),
        "full"
    ).withColumn("c_coms",
                 F.when(
                     F.col("community.cids").isNotNull(),
                     F.array_union(F.col("vertices.c_coms"), F.col("community.cids"))
                 ).otherwise(F.col("vertices.c_coms"))).select(F.col("vertices.id"), F.col("c_coms")))

    # printTrace("updated_community_tags: ", updated_community_tags)
    # printTrace("updated_communities: ", updated_communities)
    # printTrace("updated_vertices: ", updated_vertices)

    # print("Following 3 DFs after inside add() was added BEFORE added")
    # printTrace("updated_community_tags:", updated_community_tags)
    # printTrace("updated_communities:", updated_communities)
    # printTrace("updated_vertices:", updated_vertices)

    saveState(updated_community_tags, self.communityTags_path)
    saveState(updated_communities, self.communities_path)
    saveState(updated_vertices, self.vertices_path)

    # print("Following 3 DFs after inside add() was added AFTER added")
    # printTrace("updated_community_tags:", updated_community_tags)
    # printTrace("updated_communities:", updated_communities)
    # printTrace("updated_vertices:", updated_vertices)


    # return updated_community_tags, updated_communities, updated_vertices




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

def detect_common_neighbors(all_edges: DataFrame, detected_common_neighbors):
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
    edge_neighbors = detected_common_neighbors.join(src_neighbors, on="src", how="left") \
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

# def evolution(all_edges: DataFrame, edge_updates: DataFrame):
#     # Extract neighbors for both source (u) and destination (v)
#     neighbors = all_edges.select(
#         F.col("src").alias("node"),
#         F.col("dst").alias("neighbor")
#     ).union(
#         all_edges.select(
#             F.col("dst").alias("node"),
#             F.col("src").alias("neighbor")
#         )
#     ).distinct()  # Ensure no duplicate (node, neighbor) pairs
#
#     # Calculate neighbor counts for all nodes
#     neighbor_counts = neighbors.groupBy("node").agg(F.count("neighbor").alias("neighbor_count"))
#
#     # Filter for nodes with more than one neighbor (u_n and v_n conditions)
#     valid_neighbors = neighbor_counts.filter(F.col("neighbor_count") > 1).select("node")
#
#     # Join edges with valid nodes to ensure u and v both meet the condition
#     valid_edges = (
#         edge_updates.alias("valid_edges")
#         .join(valid_neighbors.alias("valid_u"), F.col("valid_edges.src") == F.col("valid_u.node"))
#         .join(valid_neighbors.alias("valid_v"), F.col("valid_edges.dst") == F.col("valid_v.node"))
#         .select("valid_edges.*")  # Only include columns from edge_updates
#     )
#
#     # Find common neighbors for valid (u, v) pairs
#     common_neighbors = (
#         valid_edges
#         .join(neighbors.alias("u_neighbors"), F.col("valid_edges.src") == F.col("u_neighbors.node"), "inner")
#         .join(neighbors.alias("v_neighbors"), F.col("valid_edges.dst") == F.col("v_neighbors.node"), "inner")
#         .filter(F.col("u_neighbors.neighbor") == F.col("v_neighbors.neighbor"))
#         .select(
#             F.col("valid_edges.src").alias("node_u"),
#             F.col("valid_edges.dst").alias("node_v"),
#             F.col("u_neighbors.neighbor").alias("common_neighbor")
#         )
#         .groupBy("node_u", "node_v")
#         .agg(F.collect_set("common_neighbor").alias("common_neighbors"))
#     )
#
#     return common_neighbors


def common_neighbor_detection(all_edges: DataFrame, edge_updates: DataFrame):
    # Extract neighbors for both source (u) and destination (v)
    neighbors = all_edges.select(
        F.col("src").alias("node"),
        F.col("dst").alias("neighbor")
    ).union(
        all_edges.select(
            F.col("dst").alias("node"),
            F.col("src").alias("neighbor")
        )
    ).distinct()

    # printTrace("neighbors", neighbors)
    # printTrace("edge_updates (common neigh det)", edge_updates)

    # Calculate neighbor counts for all nodes
    neighbor_counts = neighbors.groupBy("node").agg(F.count("neighbor").alias("neighbor_count"))

    # printTrace("neighbor_counts", neighbor_counts)

    # Filter for nodes with more than one neighbor (u_n and v_n conditions)
    valid_neighbors = neighbor_counts.filter(F.expr("neighbor_count > 1")).select("node")
                                            # F.col("neighbor_count") > 1
    # printTrace("valid_neighbors", valid_neighbors)

    # Join edges with valid nodes to ensure u and v both meet the condition
    valid_edges = (
        edge_updates.alias("edge_updates")
        .join(valid_neighbors.alias("valid_u"), F.col("edge_updates.src") == F.col("valid_u.node"))
        .join(valid_neighbors.alias("valid_v"), F.col("edge_updates.dst") == F.col("valid_v.node"))
    )

    # printTrace("valid edges: ", valid_edges)

    neighbors_of_each_node = (
        valid_edges
        .join(neighbors.alias("u_neighbors"), F.col("edge_updates.src") == F.col("u_neighbors.node"), "inner")
        .join(neighbors.alias("v_neighbors"), F.col("edge_updates.dst") == F.col("v_neighbors.node"), "inner")
        .select(
            F.col("edge_updates.src").alias("node_u"),
            F.col("edge_updates.dst").alias("node_v"),
            F.col("u_neighbors.neighbor").alias("common_u_neighbor"),
            F.col("v_neighbors.neighbor").alias("common_v_neighbor"),
            F.col("edge_updates.tags").alias("tags"),
            F.col("edge_updates.timestamp").alias("timestamp"),
            F.col("edge_updates.weight").alias("weight")
        )
        .groupBy("node_u", "node_v", "tags", "timestamp", "weight")
        .agg(F.collect_set("common_u_neighbor").alias("common_u_neighbor"), F.collect_set("common_v_neighbor").alias("common_v_neighbor"))
    )

    common_neighbors = neighbors_of_each_node.withColumn("common_neighbors", F.array_intersect(F.col("common_v_neighbor"), F.col("common_u_neighbor")))

    # Find common neighbors for valid (u, v) pairs
    # common_neighbors2 = (
    #     valid_edges
    #     .join(neighbors.alias("u_neighbors"), F.col("edge_updates.src") == F.col("u_neighbors.node"), "inner")
    #     .join(neighbors.alias("v_neighbors"), F.col("edge_updates.dst") == F.col("v_neighbors.node"), "inner")
    #     .filter(F.col("u_neighbors.neighbor") == F.col("v_neighbors.neighbor"))
    #     .select(
    #         F.col("edge_updates.src").alias("node_u"),
    #         F.col("edge_updates.dst").alias("node_v"),
    #         F.col("u_neighbors.neighbor").alias("common_neighbor"),
    #         F.col("edge_updates.tags").alias("tags"),
    #         F.col("edge_updates.timestamp").alias("timestamp"),
    #         F.col("edge_updates.weight").alias("weight")
    #     )
    #     .groupBy("node_u", "node_v", "tags", "timestamp", "weight")
    #     .agg(F.collect_set("common_neighbor").alias("common_neighbors"))
    # )
    # printTrace("common_neighbors2(inside)", common_neighbors2)
    # printTrace("common_neighbors(inside)", common_neighbors)

    return common_neighbors.sort("timestamp")


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


def printTrace(msg: string, df: DataFrame):
    printMsg(msg)
    if df and not df.isEmpty():
        df.show()
        # df.show(50, truncate=False)
    else:
        print("^ empty ^")

def remove_edge(self, dfToRemove: DataFrame):
    if dfToRemove.isEmpty():
        return
    print("removing edges:")

    dfToRemove = dfToRemove.alias("remove")
    all_vertices, all_edges = loadStateVerticesAndEdges(self)
    printTrace("remove_edge: dfToRemove:", dfToRemove)
    # printTrace("remove_edge: loaded all_edges:", all_edges)

    all_edges = normalize_edges(all_edges)
    dfToRemove = normalize_edges(dfToRemove)

    existing_edges = (
        dfToRemove.alias("remove")
        .join(
            all_edges.alias("all"),
            ((F.col("remove.src") == F.col("all.src")) & (F.col("remove.dst") == F.col("all.dst"))) |
            ((F.col("remove.src") == F.col("all.dst")) & (F.col("remove.dst") == F.col("all.src"))),
            how="inner"
        )
        .select("remove.*")
    )
    printTrace("existing_edges:", existing_edges)
    non_existing_edges = (
        dfToRemove.alias("remove")
        .join(
            all_edges.alias("all"),
            ((F.col("remove.src") == F.col("all.src")) & (F.col("remove.dst") == F.col("all.dst"))) |
            ((F.col("remove.src") == F.col("all.dst")) & (F.col("remove.dst") == F.col("all.src"))),
            how="left_anti"
        )
        .select("remove.*")
    )
    printTrace("non_existing_edges:", non_existing_edges)

    common_neighbors = common_neighbor_detection(all_edges, existing_edges)

    printTrace("remove_edge: common_neighbor ", common_neighbors)
    moreThanOneCommonNeighbor = common_neighbors.filter(F.expr("size(common_u_neighbor) > 1 and size(common_u_neighbor) > 1"))

    exploded_only_u_communities, exploded_only_v_communities, exploded_shared_coms_communities, exploded_neighbors = \
        analyze_common_neighbors(self, common_neighbors, all_vertices)

    printTrace("remove_edge: exploded_only_u_communities ", exploded_only_u_communities)
    printTrace("remove_edge: exploded_only_v_communities ", exploded_only_v_communities)
    printTrace("remove_edge: exploded_shared_coms_communities ", exploded_shared_coms_communities)

    if moreThanOneCommonNeighbor:
        if exploded_shared_coms_communities:
            shared_coms_df = exploded_shared_coms_communities.select(
                F.col("shared_coms_exploded").alias("community"),
                F.col("node_u"),
                F.col("node_v")
            )
            printTrace("remove_edge: shared_coms_df ", shared_coms_df)

            # Select relevant columns for neighbors
            # neighbors_df = exploded_neighbors.select(
            #     F.col("neighbor").alias("node_u"),
            #     F.lit(None).cast("int").alias("node_v"),  # Placeholder to match schema
            #     F.explode(F.col("neighbor_communities")).alias("community")
            # )

            neighbors_df = exploded_neighbors.select(
                F.explode(F.col("common_neighbors")).alias("common_neighbor"),
                F.explode(F.col("neighbor_communities")).alias("community")
            )

            printTrace("remove_edge: neighbors_df ", neighbors_df)

            coms_to_change_df = (
                shared_coms_df
                .select("community", "node_u", "node_v")
                .union(
                    neighbors_df.withColumnRenamed("common_neighbor", "node_u")
                    .withColumn("node_v", F.lit(None).cast(StringType()))
                    .select("community", "node_u", "node_v")
                )
                .union(
                    neighbors_df.withColumnRenamed("common_neighbor", "node_v")
                    .withColumn("node_u", F.lit(None).cast(StringType()))
                    .select("community", "node_u", "node_v")
                )
                .withColumn("node_u", F.col("node_u").cast(StringType()))
                .withColumn("node_v", F.col("node_v").cast(StringType()))
                .groupby("community")
                .agg(
                    F.collect_set("node_u").alias("affected_nodes_u"),
                    F.collect_set("node_v").alias("affected_nodes_v")
                )
                .withColumn(
                    "affected_nodes",
                    F.array_union(F.col("affected_nodes_u"), F.col("affected_nodes_v"))
                )
                .drop("affected_nodes_u", "affected_nodes_v")
            )

            printTrace("coms_to_change_df", coms_to_change_df)
    else:
        removeFromComDfU = exploded_only_u_communities.filter(F.expr("size(common_u_neighbor) < 2"))
        removeFromComDfV = exploded_only_v_communities.filter(F.expr("size(common_v_neighbor) < 2"))
        remove_from_community(self, removeFromComDfU)
        remove_from_community(self, removeFromComDfV)

    # Remove edge
    edges_state = loadState(self, pathToload=self.edges_path, schemaToCreate=self.edges_schema)
    # Step 1: Remove edges from edges_state that exist in existing_edges
    filtered_edges_state = edges_state.join(
        existing_edges,
        on=["src", "dst"],
        how="left_anti"  # Keep only edges that do not match existing_edges
    )

    # # Step 2: Identify nodes that are in existing_edges (to check if they should be removed)
    # nodes_to_check = existing_edges.select(F.col("src").alias("node")).union(
    #     existing_edges.select(F.col("dst").alias("node"))
    # ).distinct()
    #
    # # Step 3: Find nodes that no longer have any edges in the updated edges_state
    # remaining_nodes = filtered_edges_state.select(F.col("src").alias("node")).union(
    #     filtered_edges_state.select(F.col("dst").alias("node"))
    # ).distinct()
    #
    # # Nodes to remove: those that were in nodes_to_check but are not in remaining_nodes
    # nodes_to_remove = nodes_to_check.join(remaining_nodes, on="node", how="left_anti")
    # printTrace("nodes_to_remove", nodes_to_remove)
    #
    # # Step 4: Remove these nodes from vertices_state
    # filtered_vertices_state = vertices_state.join(nodes_to_remove, on="node", how="left_anti")

    printTrace("Updated edges_state after removal(Before save):", filtered_edges_state)
    saveState(filtered_edges_state, self.edges_path)
    # CHECK IF THIS IS SAVED CORRECTLY
    # printTrace("Updated edges_state after removal(After save):", filtered_edges_state)



    if coms_to_change_df and not coms_to_change_df.isEmpty():
        print("update_shared_coms should run here")

    print("end of remove edge")
        # Remove from community


def remove_from_community(self, df:DataFrame):
    if not df or df.isEmpty():
        return

    print("removing from community")
    nodes_to_remove_comms = (df.select(F.col("node_u").alias("node"))
          .union(df.select(F.col("node_v").alias("node"))).distinct())

    # Step 2: Explode tags
    exploded_tags_df = nodes_to_remove_comms.withColumn(
        "tag", F.explode(F.split(F.col("tags"), ","))
    )

    communitiesDf = loadState(self=self, pathToload=self.communities_path, schemaToCreate=self.communities_schema)
    communityTagsDf = loadState(self=self, pathToload=self.communityTags_path, schemaToCreate=self.communityTags_schema)

    # Step 3: Remove from CommunityTags - filter and remove tags
    updated_communityTags_df = exploded_tags_df.join(
        communityTagsDf,
        exploded_tags_df["tag"] == communityTagsDf["tag"],
        "left_outer"
    ).filter(
        F.col("community_id").isNotNull()  # Ensure community ID exists
    ).select(
        "community_id", "tag"
    ).distinct()

    printTrace("remove from com updated_communityTags_df:", updated_communityTags_df)

    communityTags_removed_df = communityTagsDf.join(
        updated_communityTags_df,
        ["community_id", "tag"],
        "left_anti"  # Keep only the rows where community_id + tag does not match
    )

    printTrace("remove from com communityTags_removed_df:", communityTags_removed_df)

    # Step 4: Remove nodes from communities
    updated_communities_df_u = communitiesDf.join(
        updated_communityTags_df,
        (communitiesDf["node_id"] == df["node_u"]) & (
                    communitiesDf["cid"] == updated_communityTags_df["community_id"]),
        "left_anti"  # Use left_anti to remove matching rows
    )
    printTrace("remove from com updated_communities_df_u:", updated_communities_df_u)

    updated_communities_df_v = communitiesDf.join(
        updated_communityTags_df,
        (communitiesDf["node_id"] == df["node_v"]) & (
                    communitiesDf["cid"] == updated_communityTags_df["community_id"]),
        "left_anti"  # Use left_anti to remove matching rows
    )
    printTrace("remove from com updated_communities_df_v:", updated_communities_df_v)

    updated_communities_df = updated_communities_df_u.union(updated_communities_df_v).distinct()
    printTrace("remove from com updated_communities_df:", updated_communities_df)


    saveState(communityTags_removed_df, self.communityTags_path)
    saveState(updated_communities_df, self.communities_path)
    print("finished removing edge(after save)")






def normalize_edges(edges: DataFrame):
    """
    Normalize edges such that (u, v) and (v, u) are treated as equivalent.
    Always store the edge as (min(u, v), max(u, v)).
    """
    return (edges.withColumn("src_normalized", F.least(F.col("src"), F.col("dst")))
            .withColumn("dst_normalized", F.greatest(F.col("src"), F.col("dst")))
            .drop("src", "dst")
            .withColumnRenamed("src_normalized", "src")
            .withColumnRenamed("dst_normalized", "dst").select(F.col("src"), F.col("dst"), F.col("timestamp"), F.col("tags"), F.col("weight")))

def update_graph_with_edges(all_edges: DataFrame, new_edges: DataFrame):
    # printTrace("new edges (update method)", new_edges)
    # printTrace("all_edges (update method)", all_edges)

    # g.has_edge(u, v) == g.has_edge(v, u) Undirected graph !!

    # Current edges in the graph
    # new_edges = new_edges.withColumn(
    #     "edge_key", F.array(F.col("src"), F.col("dst"))
    # )
    # printTrace("new_edges (with key): ", new_edges)

    normalized_new_edges = normalize_edges(new_edges)
    # printTrace("normalized_new_edges: ", normalized_new_edges)

    normalized_all_edges = normalize_edges(all_edges)
    # printTrace("normalized_all_edges: ", normalized_all_edges)

    # first_edges = normalized_new_edges.join(
    #     normalized_all_edges,
    #     on=["src", "dst"],
    #     how="left_anti"  # Keep only rows in `new_edges` that are NOT in `all_edges`
    # ).sort("timestamp")
    # printTrace("first_edges", first_edges)
    #
    # first_edges_deduplicated = (first_edges.groupBy("src", "dst").agg(
    #     F.first("timestamp").alias("timestamp"),
    #     F.first("tags").alias("tags"),
    #     # F.sum("weight").alias("weight")
    #     F.first("weight").alias("weight")
    # )
    # .sort("timestamp"))
    # printTrace("first_edges_deduplicated", first_edges_deduplicated) # Send them for neighbor analysis

    # Step 1: Identify preexisting edges and count their occurrences in `normalized_new_edges`
    normalized_new_edges_renamed = (normalized_new_edges.withColumnRenamed("weight", "new_weight")
                                    .withColumnRenamed("timestamp", "new_timestamp")
                                    .withColumnRenamed("tags", "new_tags")
                                    )
    # normalized_all_edges_renamed = (normalized_all_edges.withColumnRenamed("weight", "existing_weight")
    #                                 .withColumnRenamed("timestamp", "existing_timestamp")
    #                                 .withColumnRenamed("tags", "existing_tags")
    #                                 )

    # printTrace("normalized_new_edges_renamed", normalized_new_edges_renamed)
    # printTrace("normalized_all_edges_renamed", normalized_all_edges_renamed)


    new_edges_grouped = normalized_new_edges_renamed.groupBy("src", "dst").agg(
        F.count("*").alias("new_occurrences"),
        F.max(F.struct("new_timestamp", "new_tags")).alias("latest_entry")
    ).select("src", "dst", "new_occurrences",
        F.col("latest_entry.new_timestamp").alias("latest_timestamp"),
        F.col("latest_entry.new_tags").alias("latest_tags")
    )

    # printTrace("new_edges_grouped", new_edges_grouped)

    if new_edges_grouped.isEmpty():
        updated_preexisting_edges = normalized_all_edges
    else:
        updated_preexisting_edges = (
            new_edges_grouped
            .join(normalized_all_edges, on=["src", "dst"], how="left_outer")
            .withColumn("weight", F.coalesce(F.col("weight"), F.lit(0)) + F.col("new_occurrences"))
            .withColumn("tags", F.coalesce(F.col("latest_tags"), F.col("tags")))
            .withColumn("timestamp", F.coalesce(F.col("latest_timestamp"), F.col("timestamp")))
            .drop("new_occurrences", "latest_timestamp", "latest_tags")
        )

    # printTrace("updated_preexisting_edges", updated_preexisting_edges)

    # totalEdges = updated_preexisting_edges.unionByName(first_edges_deduplicated).dropDuplicates(["src", "dst"]).sort("timestamp")

    totalEdges = updated_preexisting_edges.unionByName(normalized_all_edges).dropDuplicates(["src", "dst"]).sort("timestamp")
    # totalEdges = totalEdges.unionByName(normalized_all_edges).dropDuplicates(["src", "dst"]).sort("timestamp")

    # printTrace("totalEdges:", totalEdges)

    return totalEdges

def saveStateVerticesAndEdges(self, vertices: DataFrame = None, edges: DataFrame = None):
    """
    Saves the current state of vertices and edges to Parquet files.
    """
    saveState(dataframeToBeSaved=vertices, pathToBeSaved=self.vertices_path)
    saveState(dataframeToBeSaved=edges, pathToBeSaved=self.edges_path)

def saveState(dataframeToBeSaved: DataFrame, pathToBeSaved: str):
    if dataframeToBeSaved is not None and not dataframeToBeSaved.isEmpty():
        dataframeToBeSaved.cache()
        dataframeToBeSaved.count()
        dataframeToBeSaved.write.mode("overwrite").format("parquet").save(pathToBeSaved)

        # temp_path = pathToBeSaved + "_temp"
        # # Step 2: Move the temporary directory to the final location
        # if os.path.exists(pathToBeSaved):
        #     shutil.rmtree(pathToBeSaved)  # Delete the existing directory
        # os.rename(temp_path, pathToBeSaved)  # Move the temporary directory to the final path
        # dataframeToBeSaved.count()

def loadState(self, pathToload: str, schemaToCreate: str = None):
    """
    Loads the state of vertices and edges from Parquet files.
    If no state exists, returns empty DataFrames with the appropriate schema.
    """
    try:
        loadedDataframe = self.spark.read.format("parquet").load(pathToload)
    except Exception:  # Handle case where file does not exist
        loadedDataframe = self.spark.createDataFrame([], schemaToCreate)

    return loadedDataframe


def loadStateVerticesAndEdges(self):
    vertices = loadState(self=self, pathToload=self.vertices_path, schemaToCreate=self.vertices_schema)
    edges = loadState(self=self, pathToload=self.edges_path, schemaToCreate=self.edges_schema)
    return vertices, edges