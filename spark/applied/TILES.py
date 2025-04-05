import datetime
import os
import shutil
import string
import sys
import time

from click.core import batch
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
        self.vertices_path = f"{self.base}/parquet/vertices"
        self.edges_path = f"{self.base}/parquet/edges"
        self.communities_path = f"{self.base}/parquet/communities"
        self.communityTags_path = f"{self.base}/parquet/communityTags"

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
            if not new_slice_df.isEmpty():
                # Update last_break and actual_time to the latest timestamp in this slice
                max_timestamp = new_slice_df.agg(F.max("timestamp")).collect()[0][0]
                self.last_break = datetime.datetime.fromtimestamp(max_timestamp)
                self.actual_time = self.last_break
                printMsg(f"~ NEW actual_time = {self.actual_time}")

        batch_df = batch_df.drop("gap", "dif", "dt")
        printTrace("batch_df", batch_df)
        to_remove_df = batch_df.filter(F.col("action") == "-").select(F.col("nodeU").alias("src"),
                                                                      F.col("nodeV").alias("dst"),
                                                                      F.col("timestamp"),
                                                                      F.col("tags"),
                                                                      F.lit(1).alias("weight")).sort("timestamp")

        # try to load only once the saved dataframes

        saved_vertices = loadState(self=self, pathToload=self.vertices_path, schemaToCreate=self.vertices_schema)
        saved_edges = loadState(self=self, pathToload=self.edges_path, schemaToCreate=self.edges_schema)
        communityTagsDf = loadState(self=self, pathToload=self.communityTags_path, schemaToCreate=self.communityTags_schema)
        communitiesDf = loadState(self=self, pathToload=self.communities_path, schemaToCreate=self.communities_schema)

        printMsg("Loaded State:")
        printTrace("[State] saved_vertices", saved_vertices)
        printTrace("[State] saved_edges", saved_edges)
        printTrace("[State] communityTagsDf", communityTagsDf)
        printTrace("[State] communitiesDf", communitiesDf)
        printMsg("Starting processing batch after loaded state:")

        saved_edges, saved_vertices, communityTagsDf, communitiesDf = remove_edge(self, dfToRemove=to_remove_df.filter(F.col("nodeU") != F.col("nodeV")),
                    saved_edges=saved_edges, saved_vertices=saved_vertices, saved_communities=communitiesDf, saved_communityTags=communityTagsDf)

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

        # state might have been modified due to remove_edge
        vertices_state, edges_state = saved_vertices, saved_edges
        printTrace("[new state] vertices_state", vertices_state)
        printTrace("[new state]  edges_state", edges_state)

        edges_state = normalize_edges(edges_state)

        updated_edges = update_graph_with_edges(edges_state, new_edges)

        # printTrace("vertices_state before 1st left_anti", vertices_state)
        new_nodes_filtered = new_nodes.join(vertices_state.select("id"), on="id", how="left_anti")
        # printTrace("new_nodes_filtered after 1st left_anti", new_nodes_filtered)

        # Update state
        updated_vertices = vertices_state.unionByName(new_nodes_filtered).dropDuplicates(subset=["id"])
        printTrace("updated_vertices after 1st union", updated_vertices)


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
        # printTrace("normalized_new_edges", normalized_new_edges)
        # printTrace("normalized_all_edges", normalized_all_edges)
        firstTimeEdges = normalized_new_edges.join(
            normalized_all_edges,
            on=["src", "dst"],
            how="left_anti"
        ).groupBy("src", "dst").agg(
            F.first("timestamp").alias("timestamp"),
            F.first("tags").alias("tags"),
            F.first("weight").alias("weight")
        ).sort("timestamp")
        # printTrace("first_edges", first_edges)
        # firstTimeEdges = (first_edges.groupBy("src", "dst").agg(
        #     F.first("timestamp").alias("timestamp"),
        #     F.first("tags").alias("tags"),
        #     F.first("weight").alias("weight")
        # ).sort("timestamp"))
        # printTrace("firstTimeEdges (after save)", firstTimeEdges)

        # Reload updated state from Parquet to ensure it includes all persisted data
        # all_vertices, all_edges = loadStateVerticesAndEdges(self)

        all_vertices, all_edges = updated_vertices, updated_edges

        # printTrace("new edges: (before evo)", new_edges)
        # printTrace("updated_edges: (before evo)", updated_edges)
        # printTrace("all all_edges: (before evo)", all_edges)
        # printTrace("firstTimeEdges: ", firstTimeEdges)
        # printTrace("all_vertices 1st Loaded (after updated_vertices) : ", all_vertices)

        # common_neighbors = common_neighbor_detection(updated_edges, firstTimeEdges)
        printMsg("Starting detecting common neighbros")
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

        communityTagsDf, communitiesDf, all_vertices = add_community_neighbors(self, exploded_neighbors, exploded_only_u_communities, exploded_only_v_communities,
                                exploded_shared_coms_communities, all_vertices, communitiesDf, communityTagsDf)

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
        all_vertices, all_edges, updated_communitiesDf, communityTagsDf =  (
            print_coms(self, all_edges, all_vertices, communitiesDf, communityTagsDf, batch_id))

        printMsg("Saving state...")
        saveState(communityTagsDf, self.communityTags_path)
        saveState(communitiesDf, self.communities_path)
        saveState(all_vertices, self.vertices_path)
        saveState(all_edges, self.edges_path)



        printMsg("State saved successfully...")

        # saveStateVerticesAndEdges(self, all_vertices, all_edges)
        # saveStateVerticesAndEdges(self, updated_vertices, updated_edges)
        # all_vertices, all_edges = loadStateVerticesAndEdges(self)
        # latest_vertices, latest_edges = loadStateVerticesAndEdges(self)
        # printTrace("all_vertices Loaded - Last step", latest_vertices)
        # printTrace("all_edges Loaded - Last step: ", latest_edges)
        printMsg("end of processing batch...")

        # Show the updated edges (debugging)
        # printMsg("graph edges:")
        # self.g.edges.show(truncate=False)
        # printMsg("graph nodes:")
        # self.g.vertices.show(truncate=False)


def add_community_neighbors(self, exploded_neighbors: DataFrame, exploded_only_u_communities:DataFrame,
                            exploded_only_v_communities:DataFrame, exploded_shared_coms_communities: DataFrame,
                                all_vertices: DataFrame, communitiesDf: DataFrame, communityTagsDf: DataFrame) -> (DataFrame, DataFrame):
    if exploded_neighbors is None or exploded_neighbors.isEmpty():
        return communityTagsDf, communitiesDf, all_vertices

    exploded_neighbors.cache()
    exploded_neighbors.count()
    exploded_only_u_communities.cache()
    exploded_only_u_communities.count()
    exploded_only_v_communities.cache()
    exploded_only_v_communities.count()
    exploded_shared_coms_communities.cache()
    exploded_shared_coms_communities.count()

    # all_vertices.cache()
    # all_vertices.count()
    # communitiesDf.cache()
    # communitiesDf.count()
    # communityTagsDf.cache()
    # communityTagsDf.count()

    printMsg("Adding to neighbors to community...")
    # printTrace("add_community_neighbors exploded_neighbors", exploded_neighbors)
    exploded_neigh_communities = (
        exploded_neighbors
        .join(all_vertices.alias("all_nodes"), F.col("neighbor") == F.col("all_nodes.id"), "left")
        .withColumn("neighbor_community", F.when(F.col("all_nodes.c_coms").isNotNull(), F.col("all_nodes.c_coms")).otherwise(F.array()))
        .withColumn("neighbor_community", F.explode(F.col("neighbor_community")))   # -> skip null rows
    )
    # printTrace("add_community_neighbors exploded_neigh_communities", exploded_neigh_communities)

    only_v_coms_with_neigh_coms = ((exploded_only_v_communities.alias("only_v_coms")
            .join(exploded_neigh_communities.alias("neigh_coms"), F.col("neighbor_community") == F.col("only_v_exploded"), how="inner"))
            # .join(exploded_neigh_communities.alias("neigh_coms"), F.col("neighbor_community") == F.col("only_v_exploded"), how="left_outer"))
            .select(F.col("neigh_coms.node_u").cast("string"), F.col("neigh_coms.node_v").cast("string"),
                        F.col("neigh_coms.common_neighbors"), F.col("neigh_coms.tags"), F.col("neigh_coms.timestamp"), F.col("neigh_coms.weight"),
                        F.col("neigh_coms.shared_coms"), F.col("neigh_coms.only_u"), F.col("neigh_coms.only_v"),
                        F.col("neigh_coms.neighbor_communities"), F.col("neigh_coms.neighbor_community")
    ).distinct())
    only_v_coms_with_neigh_coms = only_v_coms_with_neigh_coms.withColumnRenamed("neighbor_community", "new_community_id").withColumn("nodes", F.array("node_u"))

    # printTrace("only_v_coms_with_neigh_coms", only_v_coms_with_neigh_coms)

    # communitiesDf = loadState(self=self, pathToload=self.communities_path, schemaToCreate=self.communities_schema)
    # communityTagsDf = loadState(self=self, pathToload=self.communityTags_path, schemaToCreate=self.communityTags_schema)

    # printTrace("before add_to_community_streaming only_v_coms_with_neigh_coms communitiesDf", communitiesDf)
    # printTrace("before add_to_community_streaming only_v_coms_with_neigh_coms communityTagsDf", communityTagsDf)
    updated_community_tags, updated_communities, updated_vertices =\
        add_to_community_streaming(self, all_vertices, only_v_coms_with_neigh_coms, communitiesDf, communityTagsDf)
    # printTrace("after add_to_community_streaming only_v_coms_with_neigh_coms communitiesDf", updated_communities)
    # printTrace("after add_to_community_streaming only_v_coms_with_neigh_coms communityTagsDf", updated_community_tags)


    # saveState(updated_community_tags, self.communityTags_path)
    # saveState(updated_communities, self.communities_path)
    # saveState(updated_vertices, self.vertices_path)


    # printTrace("[add-join] exploded_only_u_communities", exploded_only_u_communities)
    # printTrace("[add-join] exploded_neigh_communities", exploded_neigh_communities)

    only_u_coms_with_neigh_coms = ((exploded_only_u_communities.alias("only_u_coms")
             .join(exploded_neigh_communities.alias("neigh_coms"),
                F.col("neighbor_community") == F.col("only_u_exploded"), how="inner"))
                # F.col("neighbor_community") == F.col("only_u_exploded"), how="left_outer"))
               .select(F.col("neigh_coms.node_u").cast("string"), F.col("neigh_coms.node_v").cast("string"),
                    F.col("neigh_coms.common_neighbors"), F.col("neigh_coms.tags"), F.col("neigh_coms.timestamp"), F.col("neigh_coms.weight"),
                    F.col("neigh_coms.shared_coms"), F.col("neigh_coms.only_u"), F.col("neigh_coms.only_v"),
                    F.col("neigh_coms.neighbor_communities"), F.col("neigh_coms.neighbor_community")
            ).distinct())

    only_u_coms_with_neigh_coms = only_u_coms_with_neigh_coms.withColumnRenamed("neighbor_community", "new_community_id").withColumn("nodes", F.array("node_v"))

    # printTrace("only_u_coms_with_neigh_coms", only_u_coms_with_neigh_coms)

    # printTrace("before add_to_community_streaming only_u_coms_with_neigh_coms communitiesDf", updated_communities)
    # printTrace("before add_to_community_streaming only_u_coms_with_neigh_coms communityTagsDf", updated_community_tags)
    updated_community_tags, updated_communities, updated_vertices = (
        add_to_community_streaming(self, all_vertices, only_u_coms_with_neigh_coms, updated_communities, updated_community_tags))
    # printTrace("after add_to_community_streaming only_u_coms_with_neigh_coms communitiesDf", updated_communities)
    # printTrace("after add_to_community_streaming only_u_coms_with_neigh_coms communityTagsDf", updated_community_tags)


    # saveState(updated_community_tags, self.communityTags_path)
    # saveState(updated_communities, self.communities_path)
    # saveState(updated_vertices, self.vertices_path)


    shared_com_not_in_common_neigh_community = (
        exploded_shared_coms_communities.join(exploded_neigh_communities,
                                              F.col("neighbor_community") == F.col("shared_coms_exploded"), how="left_anti")
    ).withColumnRenamed("neighbor_community", "new_community_id")

    shared_com_not_in_common_neigh_community = (shared_com_not_in_common_neigh_community.withColumn("nodes", F.array("neighbor"))
                                                .withColumnRenamed("shared_coms_exploded", "new_community_id"))
    # printTrace("shared_com_not_in_common_neigh_community", shared_com_not_in_common_neigh_community)

    # printTrace("before add_to_community_streaming shared_com_not_in_common_neigh_community communitiesDf", updated_communities)
    # printTrace("before add_to_community_streaming shared_com_not_in_common_neigh_community communityTagsDf", updated_community_tags)
    updated_community_tags, updated_communities, updated_vertices = (
        add_to_community_streaming(self, all_vertices, shared_com_not_in_common_neigh_community, updated_communities, updated_community_tags))
    # printTrace("after add_to_community_streaming shared_com_not_in_common_neigh_community communitiesDf", updated_communities)
    # printTrace("after add_to_community_streaming shared_com_not_in_common_neigh_community communityTagsDf", updated_community_tags)


    # saveState(updated_community_tags, self.communityTags_path)
    # saveState(updated_communities, self.communities_path)
    # saveState(updated_vertices, self.vertices_path)



    if exploded_neigh_communities.isEmpty() or (only_v_coms_with_neigh_coms.isEmpty() and only_u_coms_with_neigh_coms.isEmpty() and
                                                (shared_com_not_in_common_neigh_community is not None and shared_com_not_in_common_neigh_community.isEmpty())):
        printMsg("exploded_neigh_communities is empty thus create new community and add the nodes")
        # printTrace("exploded_neighbors", exploded_neighbors)
        new_community_edges = (
            exploded_neighbors
            .select(
                F.col("node_u").cast("string"), F.col("node_v").cast("string"),
                F.col("common_neighbors"),
                F.col("tags"), F.col("timestamp"), F.col("weight"),
                F.col("shared_coms"), F.col("only_u"), F.col("only_v")
            )
            .groupBy("node_u", "node_v", "common_neighbors")
            .agg(
                F.first("tags").alias("tags"),
                F.first("timestamp").alias("timestamp"),
                F.first("weight").alias("weight"),
                F.first("shared_coms").alias("shared_coms"),
                F.first("only_u").alias("only_u"),
                F.first("only_v").alias("only_v")
            )
            .withColumn("new_community_id", F.expr("uuid()"))
        )

        # new_community_edges = (exploded_neighbors.select(
        #     F.col("node_u").cast("string"), F.col("node_v").cast("string"),
        #     F.col("common_neighbors"),
        #     F.col("tags"), F.col("timestamp"), F.col("weight"),
        #     F.col("shared_coms"), F.col("only_u"), F.col("only_v"))
        #                        .withColumn("new_community_id", F.expr("uuid()")))

        # printTrace("new_community_edges p1", new_community_edges)


        new_community_edges = new_community_edges.withColumn(
            "new_community_id",
            F.when(
                F.expr("size(shared_coms) > 0"),
                F.first("new_community_id").over(Window.partitionBy("node_u", "node_v"))
            ).otherwise(F.col("new_community_id"))
        ).sort("timestamp")

        printTrace("new_community_edges: p2 ", new_community_edges)
        # printTrace("before add_to_community_streaming new_community_edges communityTagsDf", updated_community_tags)
        updated_community_tags, updated_communities, updated_vertices = (
            add_to_community_streaming(self, all_vertices, new_community_edges, updated_communities, updated_community_tags))
        # printTrace("after add_to_community_streaming new_community_edges communitiesDf", updated_communities)
        # printTrace("after add_to_community_streaming new_community_edges communityTagsDf", updated_community_tags)

        # printTrace("updated_community_tags: (after save) ", updated_community_tags)
        # printTrace("updated_communities: (after save) ", updated_communities)
        # printTrace("updated_vertices: (after save) ", updated_vertices)


        # saveState(updated_community_tags, self.communityTags_path)
        # saveState(updated_communities, self.communities_path)
        # saveState(updated_vertices, self.vertices_path)


    # exploded_neighbors.unpersist()
    # exploded_only_u_communities.unpersist()
    # exploded_only_v_communities.unpersist()
    # exploded_shared_coms_communities.unpersist()


    return updated_community_tags, updated_communities, updated_vertices
    # saveState(updated_community_tags, self.communityTags_path)
    # saveState(updated_communities, self.communities_path)
    # saveState(updated_vertices, self.vertices_path)


    printMsg("Finished adding neighbors to community...")

def print_coms(self, edgesDf:DataFrame, all_vertices: DataFrame, communitiesDf: DataFrame, communityTagsDf: DataFrame, batch_id):
    output_base_path = f"{self.base}/output"
    communities_output_path = f"{output_base_path}/strong-communities-{batch_id}"
    graph_output_path = f"{output_base_path}/graph-{batch_id}"
    merge_output_path = f"{output_base_path}/merging-{batch_id}"

    # --- 1. Filter out small communities ---
    printMsg(f"[Batch {batch_id}] Filtering small communities...")
    communitiesDf_filtered = communitiesDf.withColumn("node_count", F.size(F.col("nodes")))
    valid_size_communities = communitiesDf_filtered.filter(F.col("node_count") > 2).select("cid", "nodes")
    small_communities = communitiesDf_filtered.filter(F.col("node_count") <= 2).select("cid")
    printTrace(f"[Batch {batch_id}] Found small communities", small_communities)
    communitiesDf = destroy_communities(self, small_communities.withColumnRenamed("cid", "community"), communitiesDf)

    # --- 2 & 3. Identify duplicates and determine kept/merged CIDs ---
    printMsg(f"[Batch {batch_id}] Identifying duplicate communities...")
    # Sort nodes within each community to create a canonical representation
    communities_with_sorted_nodes = valid_size_communities.withColumn(
        "sorted_nodes_key", F.array_sort(F.col("nodes"))
    )
    # Convert array to string for grouping (safer than grouping by array directly sometimes)
    # Use a delimiter that won't appear in node IDs
    communities_with_sorted_nodes = communities_with_sorted_nodes.withColumn(
        "sorted_nodes_str", F.concat_ws("||", F.col("sorted_nodes_key"))
    )

    # Window spec to find the minimum CID for each group of identical nodes
    window_spec = Window.partitionBy("sorted_nodes_str")

    # Find the minimum CID (kept_cid) for each unique set of nodes
    communities_with_min_cid = communities_with_sorted_nodes.withColumn(
        "kept_cid", F.min("cid").over(window_spec)
    )
    printTrace(f"[Batch {batch_id}] Communities with min CID assigned", communities_with_min_cid)

    # Identify unique communities (those where cid == kept_cid)
    unique_communities_df = communities_with_min_cid.filter(F.col("cid") == F.col("kept_cid")) \
        .select(
        F.col("cid").alias("final_cid"),
        F.col("nodes"),  # Keep original node order for output consistency if needed, or use sorted_nodes_key
        "sorted_nodes_key"  # Needed for joining later if original nodes aren't unique key
    ).distinct()  # Distinct needed in case the original communitiesDf had exact duplicates (same cid, same nodes)
    printTrace(f"[Batch {batch_id}] Unique communities identified", unique_communities_df)

    # Identify communities that will be merged away (cid != kept_cid)
    merged_away_communities = communities_with_min_cid.filter(F.col("cid") != F.col("kept_cid")) \
        .select("cid", "kept_cid", "sorted_nodes_str").distinct()  # Distinct needed
    printTrace(f"[Batch {batch_id}] Communities to be merged away", merged_away_communities)

    # --- 4. Prepare DataFrames for Output Files ---

    # A. Strong Communities File Data
    printMsg(f"[Batch {batch_id}] Preparing strong communities output...")
    # Join unique communities with their tags
    output_communities_intermediate = unique_communities_df.alias("uc") \
        .join(communityTagsDf.alias("ct"), F.col("uc.final_cid") == F.col("ct.cid"), "left_outer") \
        .select(
        F.col("uc.final_cid").alias("cid"),
        F.col("uc.nodes"),
        F.col("ct.tags")
    )

    # Add tag counts (approximating the Python logic)
    # Note: Replicating Counter().most_common() exactly without UDFs is complex.
    # This calculates unique tag count and total tag occurrences.
    output_communities_with_counts = output_communities_intermediate \
        .withColumn("unique_tag_count",
                    F.size(F.when(F.col("tags").isNotNull(), F.array_distinct(F.col("tags"))).otherwise(F.array()))) \
        .withColumn("total_tag_occurrences",
                    F.size(F.when(F.col("tags").isNotNull(), F.col("tags")).otherwise(F.array())))

    # Format for text output
    # Convert arrays to string representations compatible with the Python output
    output_communities_formatted = output_communities_with_counts.select(
        F.concat_ws("\t",
                    F.col("cid"),
                    F.regexp_replace(F.to_json(F.col("nodes")), r"[\"\[\]]", ""),
                    F.col("unique_tag_count").cast("string"),
                    F.col("total_tag_occurrences").cast("string"),
                    F.concat(F.lit("'"), F.when(F.col("tags").isNotNull(), F.to_json(F.col("tags"))).otherwise("[]"),
                             F.lit("'"))
                    ).alias("value")
    )
    printTrace(f"[Batch {batch_id}] Formatted strong communities", output_communities_formatted)

    # B. Graph File Data
    printMsg(f"[Batch {batch_id}] Preparing graph output...")
    # Format edges directly from edgesDf
    output_graph_formatted = edgesDf.select(
        F.concat_ws("\t",
                    F.col("src"),
                    F.col("dst"),
                    F.col("weight").cast("string"),
                    F.col("timestamp").cast("string")
                    ).alias("value")
    )
    printTrace(f"[Batch {batch_id}] Formatted graph edges", output_graph_formatted)

    # C. Merge File Data
    printMsg(f"[Batch {batch_id}] Preparing merge info output...")
    # Group merged away communities by the CID they are merged into (kept_cid)
    merge_info_grouped = merged_away_communities.groupBy("kept_cid").agg(
        F.collect_list("cid").alias("merged_cids")  # List of CIDs that were merged into kept_cid
    )

    # Format for text output
    output_merge_formatted = merge_info_grouped.select(
        F.concat_ws("\t",
                    F.col("kept_cid"),
                    F.regexp_replace(F.to_json(F.col("merged_cids")), r"[\"\[\]]", "")
                    ).alias("value")
    )
    printTrace(f"[Batch {batch_id}] Formatted merge info", output_merge_formatted)

    # --- Write Output Files ---
    printMsg(f"[Batch {batch_id}] Writing output files...")
    try:
        # Write communities
        (output_communities_formatted.write
         .mode("overwrite")  # Overwrite per batch slice
         .format("text")
         .option("compression", "gzip")
         .save(communities_output_path))

        # Write graph edges
        (output_graph_formatted.write
         .mode("overwrite")
         .format("text")
         .option("compression", "gzip")
         .save(graph_output_path))

        # Write merge info (only if there's something to merge)
        if not merge_info_grouped.isEmpty():
            (output_merge_formatted.write
             .mode("overwrite")
             .format("text")
             .option("compression", "gzip")
             .save(merge_output_path))
        else:
            printMsg(f"[Batch {batch_id}] No merge information to write.")

    except Exception as e:
        printMsg("Error writing output files")

        # printError(f"[Batch {batch_id}] Error writing output files: {e}")

    # --- 5. Determine All Communities to Remove from State ---
    printMsg(f"[Batch {batch_id}] Determining communities to remove from state...")
    ids_merged_away = merged_away_communities.select(F.col("cid")).distinct()
    ids_too_small = small_communities.select(F.col("cid")).distinct()

    ids_to_remove_df = ids_merged_away.unionByName(ids_too_small).distinct()
    printTrace(f"[Batch {batch_id}] Combined list of CIDs to remove", ids_to_remove_df)
    removed_count = ids_to_remove_df.count()  # Action to see count
    printMsg(f"[Batch {batch_id}] Total communities to remove (small or merged): {removed_count}")

    # --- 6. Update State DataFrames ---
    printMsg(f"[Batch {batch_id}] Updating state DataFrames...")
    # Use left_anti join to keep only rows whose 'cid' is NOT in ids_to_remove_df
    updated_communitiesDf = communitiesDf.join(
        ids_to_remove_df,
        communitiesDf.cid == ids_to_remove_df.cid,
        "left_anti"
    )

    updated_communityTagsDf = communityTagsDf.join(
        ids_to_remove_df,
        communityTagsDf.cid == ids_to_remove_df.cid,
        "left_anti"
    )

    updated_verticesDf = all_vertices
    updated_edgesDf = edgesDf

    # --- 7. Save Updated State ---
    printMsg(f"[Batch {batch_id}] Saving updated state...")
    return updated_verticesDf, updated_edgesDf, updated_communitiesDf, updated_communityTagsDf
    # saveState(updated_verticesDf, self.vertices_path)
    # saveState(updated_edgesDf, self.edges_path)  # Save edges state too if it's managed this way
    # saveState(updated_communitiesDf, self.communities_path)
    # saveState(updated_communityTagsDf, self.communityTags_path)

def analyze_common_neighbors(self, common_neighbors: DataFrame, all_vertices: DataFrame) -> (DataFrame, DataFrame):
    printMsg("Analyzing common neighbors...")

    if common_neighbors.isEmpty():
        printMsg("[analyze_common_neighbors] common_neighbors empty, skipping...")
        return None, None, None, None

    common_neighbors = common_neighbors.filter(F.expr("size(common_neighbors) > 0"))

    if common_neighbors.isEmpty():
        printMsg("[analyze_common_neighbors] common_neighbors > 0 empty, skipping...")
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

    exploded_only_u_communities = exploded_neighbors.withColumn("only_u_exploded", F.explode(F.when(F.expr("size(only_u) > 0"), F.col("only_u"))
                                                                                            .otherwise(F.array())))
    exploded_only_v_communities = exploded_neighbors.withColumn("only_v_exploded", F.explode(F.when(F.expr("size(only_v) > 0"), F.col("only_v"))
                                                                                            .otherwise(F.array())))
    exploded_shared_coms_communities = exploded_neighbors.withColumn("shared_coms_exploded", F.explode(F.when(F.expr("size(shared_coms) > 0"), F.col("shared_coms"))
                                                                                            .otherwise(F.array())))
    printMsg("Finished analyzing common neighbors...")
    return exploded_only_u_communities, exploded_only_v_communities, exploded_shared_coms_communities, exploded_neighbors



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

def add_to_community_streaming(self, all_vertices:DataFrame, new_community_edges:DataFrame, communitiesDf:DataFrame, communityTagsDf:DataFrame):
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
        return communityTagsDf, communitiesDf, all_vertices
    printMsg("Merging community tags and communities ...")
    # communitiesDf = loadState(self=self, pathToload=self.communities_path, schemaToCreate=self.communities_schema)
    # communityTagsDf = loadState(self=self, pathToload=self.communityTags_path, schemaToCreate=self.communityTags_schema)

    printTrace("[add] new_community_edges: ", new_community_edges)

    new_community_edges.cache()
    new_community_edges.count()
    # Step 1: Process and aggregate tags by new_community_id
    new_tags = (
        new_community_edges
        .select(F.col("new_community_id").alias("cid"), "tags")
        .groupBy("cid")
        .agg(F.array_distinct(F.flatten(F.collect_list("tags"))).alias("tags"))
    )

    printTrace("[add] new_tags: ", new_tags)

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

    # printTrace("new_communities: ", new_communities)
    # printTrace("communitiesDf: (inside add() first loaded)", communitiesDf)
    printTrace("[add] communityTagsDf: (inside add() first loaded)", communityTagsDf)

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
    printTrace("[add] updated_community_tags: ", updated_community_tags)

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

    updated_communities = updated_communities.filter(F.col("cid").isNotNull())
    printTrace("updated_communities before new changes: ", updated_communities)

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

    # printTrace("community_nodes after new changes: ", community_nodes)

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
    printMsg("  Saving state of communityTags, communities and vertices")

    # saveState(updated_community_tags, self.communityTags_path)
    # saveState(updated_communities, self.communities_path)
    # saveState(updated_vertices, self.vertices_path)

    printMsg("  Saved state of merged community tags and communities ...")
    updated_community_tags.cache()
    updated_community_tags.count()

    updated_communities.cache()
    updated_communities.count()

    updated_vertices.cache()
    updated_vertices.count()

    return updated_community_tags, updated_communities, updated_vertices
    # print("Following 3 DFs after inside add() was added AFTER added")
    # printTrace("updated_community_tags:", updated_community_tags)
    # printTrace("updated_communities:", updated_communities)
    # printTrace("updated_vertices:", updated_vertices)


    # return updated_community_tags, updated_communities, updated_vertices


def printMsg(msg):
    print(time.ctime() + " " + msg)

def common_neighbor_detection_of_nodes(all_edges: DataFrame, dfWithNodesArray: DataFrame):
    # neighbors = all_edges.select(
    #     F.col("src").alias("node"),
    #     F.col("dst").alias("neighbor")
    # ).union(
    #     all_edges.select(
    #         F.col("dst").alias("node"),
    #         F.col("src").alias("neighbor")
    #     )
    # ).distinct()
    neighbors = (all_edges.select(F.explode(F.array(F.struct(F.col("src").alias("node"), F.col("dst").alias("neighbor")),
                                           F.struct(F.col("dst").alias("node"), F.col("src").alias("neighbor")))))
        .select("col.*")
        .distinct()
    )

    # Step 2: Aggregate to get list of neighbors per node
    node_neighbors = neighbors.groupBy("node") \
                              .agg(F.collect_set("neighbor").alias("neighbors"))



    # Step 3: Explode shared_nodes to get (node, community) pairs
    exploded_nodes = dfWithNodesArray.select(
        F.explode("shared_nodes").alias("node"),
        "community", "communities"
    )

    # Step 4: Aggregate to get list of communities per node
    node_communities = exploded_nodes.groupBy("node") \
                                     .agg(F.collect_set("community").alias("communities"))

    # Step 5: Join communities with their neighbors
    result = node_communities.join(node_neighbors, "node", "left_outer")

    return result



def common_neighbor_detection(all_edges: DataFrame, edge_updates: DataFrame):
    # Extract neighbors for both source (u) and destination (v)
    printMsg("[common_neighbor_detection] common neighbor detection started")
    neighbors = (
        all_edges.select(F.explode(F.array(F.struct(F.col("src").alias("node"), F.col("dst").alias("neighbor")),
                                           F.struct(F.col("dst").alias("node"), F.col("src").alias("neighbor")))))
        .select("col.*")
        .distinct()
    )

    # Calculate neighbor counts for all nodes
    neighbor_counts = neighbors.groupBy("node").agg(F.count("neighbor").alias("neighbor_count"))

    # Filter for nodes with more than one neighbor (u_n and v_n conditions)
    valid_neighbors = neighbor_counts.filter(F.expr("neighbor_count > 1")).select("node")

    # Join edges with valid nodes to ensure u and v both meet the condition
    valid_edges = (
        edge_updates.alias("edge_updates")
        .join(valid_neighbors.alias("valid_u"), F.col("edge_updates.src") == F.col("valid_u.node"))
        .join(valid_neighbors.alias("valid_v"), F.col("edge_updates.dst") == F.col("valid_v.node"))
    )

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
    printMsg("[common_neighbor_detection] common neighbor detection finished")
    return common_neighbors.sort("timestamp")


def printTrace(msg: string, df: DataFrame):
    printMsg(msg)
    # if df:
    #     df.show(50, truncate=False)
    # else:
    #     print("^ empty ^")

def remove_edge(self, dfToRemove: DataFrame, saved_edges: DataFrame, saved_vertices: DataFrame, saved_communityTags: DataFrame, saved_communities: DataFrame):
    if dfToRemove.isEmpty():
        return saved_edges, saved_vertices, saved_communityTags, saved_communities
    printMsg("Removing edges:")

    dfToRemove = dfToRemove.alias("remove")
    all_vertices, all_edges = saved_vertices, saved_edges

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

    printTrace("[Remove] existing_edges", existing_edges)

    common_neighbors = common_neighbor_detection(all_edges, existing_edges)

    # printTrace("remove_edge: common_neighbor ", common_neighbors)
    moreThanOneCommonNeighbor = common_neighbors.filter(F.expr("size(common_u_neighbor) > 1 and size(common_u_neighbor) > 1"))

    exploded_only_u_communities, exploded_only_v_communities, exploded_shared_coms_communities, exploded_neighbors = \
        analyze_common_neighbors(self, common_neighbors, all_vertices)

    # printTrace("remove_edge: exploded_only_u_communities ", exploded_only_u_communities)
    # printTrace("remove_edge: exploded_only_v_communities ", exploded_only_v_communities)
    # printTrace("remove_edge: exploded_shared_coms_communities ", exploded_shared_coms_communities)
    # printTrace("remove_edge: exploded_neighbors ", exploded_neighbors)

    coms_to_change_df = None

    if moreThanOneCommonNeighbor:
        if exploded_shared_coms_communities:
            shared_coms_df = exploded_shared_coms_communities.select(
                F.col("shared_coms_exploded").alias("community"),
                F.col("node_u"),
                F.col("node_v")
            )
            # printTrace("[remove_edge] shared_coms_df ", shared_coms_df)

            neighbors_df = exploded_neighbors.select(
                F.explode(F.col("common_neighbors")).alias("common_neighbor"),
                F.explode(F.col("neighbor_communities")).alias("community")
            )

            # printTrace("[remove_edge] neighbors_df ", neighbors_df)

            common_neighbors_df = neighbors_df.alias("n").join(
                shared_coms_df.alias("s"),
                on="community",
                how="inner"
            ).select(
                F.col("s.community"),
                F.col("s.node_u"),
                F.col("s.node_v"),
                F.col("n.common_neighbor")
            )

            # printTrace("[remove_edge] common_neighbors_df ", common_neighbors_df)

            coms_to_change_df = (
                common_neighbors_df
                .groupBy("community")
                .agg(
                    F.collect_set("node_u").alias("node_u_set"),
                    F.collect_set("node_v").alias("node_v_set"),
                    F.collect_set(F.col("common_neighbor").cast("int")).alias("common_neighbors_set")  # Ensure INT type
                )
                .withColumn("affected_nodes", F.array_union(F.col("node_u_set"), F.col("node_v_set")))
                .withColumn("affected_nodes", F.array_union(F.col("affected_nodes"), F.col("common_neighbors_set")))
                .select("community", "affected_nodes")
            )

            # coms_to_change_df = (
            #     shared_coms_df
            #     .select("community", "node_u", "node_v")
            #     .union(
            #         neighbors_df.withColumnRenamed("common_neighbor", "node_u")
            #         .withColumn("node_v", F.lit(None).cast(StringType()))
            #         .select("community", "node_u", "node_v")
            #     )
            #     .union(
            #         neighbors_df.withColumnRenamed("common_neighbor", "node_v")
            #         .withColumn("node_u", F.lit(None).cast(StringType()))
            #         .select("community", "node_u", "node_v")
            #     )
            #     .withColumn("node_u", F.col("node_u").cast(StringType()))
            #     .withColumn("node_v", F.col("node_v").cast(StringType()))
            #     .groupby("community")
            #     .agg(
            #         F.collect_set("node_u").alias("affected_nodes_u"),
            #         F.collect_set("node_v").alias("affected_nodes_v")
            #     )
            #     .withColumn(
            #         "affected_nodes",
            #         F.array_union(F.col("affected_nodes_u"), F.col("affected_nodes_v"))
            #     )
            #     .drop("affected_nodes_u", "affected_nodes_v")
            # )

            # printTrace("coms_to_change_df", coms_to_change_df)
    else:
        removeFromComDfU = exploded_only_u_communities.filter(F.expr("size(common_u_neighbor) < 2"))
        removeFromComDfV = exploded_only_v_communities.filter(F.expr("size(common_v_neighbor) < 2"))
        all_vertices, saved_communityTags, saved_communities = remove_from_community(self, removeFromComDfU, saved_vertices=all_vertices, communityTagsDf=saved_communityTags, communitiesDf=saved_communities)
        all_vertices, saved_communityTags, saved_communities = remove_from_community(self, removeFromComDfV, saved_vertices=all_vertices, communityTagsDf=saved_communityTags, communitiesDf=saved_communities) # TODO SHOULD CHECK THOSE

    # Remove edge
    edges_state = saved_edges
    # edges_state = loadState(self, pathToload=self.edges_path, schemaToCreate=self.edges_schema)
    filtered_edges_state = edges_state.join(
        existing_edges,
        on=["src", "dst"],
        how="left_anti"  # Keep only edges that do not match existing_edges
    )

    all_vertices, saved_communityTags, saved_communities = update_shared_coms(self, coms_to_change_df, existing_edges, saved_edges=saved_edges, saved_vertices=all_vertices,
                       communityTagsDf=saved_communityTags, saved_communities=saved_communities)

    return filtered_edges_state, all_vertices, saved_communityTags, saved_communities
    # TODO here we should first remove the edges and then execute update_shared_coms
    # printTrace("Updated edges_state after removal(Before save):", filtered_edges_state)
    # saveState(filtered_edges_state, self.edges_path)

    # edges_state_latest = loadState(self, pathToload=self.edges_path, schemaToCreate=self.edges_schema)
    # edges_test = edges_state_latest
    # printTrace("remove_edge, edges_state_latest(After load):", edges_test)


    printMsg("end of remove edge")
        # Remove from community

def update_shared_coms(self, shared_coms:DataFrame, existing_edges:DataFrame, saved_edges: DataFrame, saved_vertices: DataFrame, communityTagsDf: DataFrame, saved_communities: DataFrame):
    printMsg("Running update_shared_coms...")
    if not shared_coms:
        printMsg("Skipped update_shared_coms")
        return saved_vertices, communityTagsDf, saved_communities

    shared_coms.cache()
    existing_edges.cache()

    printTrace("[update_shared_coms] shared_coms", shared_coms)

    # saved_communities = loadState(self, pathToload=self.communities_path, schemaToCreate=self.communities_schema)

    printTrace("[update_shared_coms] saved_communities", saved_communities)

    preexisting_coms = (shared_coms.join(saved_communities,
                                         saved_communities["cid"] == shared_coms["community"], "inner")
                        .select(F.col("community"), F.col("affected_nodes")).distinct())
    # Alias DataFrames to avoid ambiguity
    preexisting_coms_alias = preexisting_coms.alias("pre")
    saved_communities_alias = saved_communities.alias("saved")


    printTrace("[update_shared_coms] preexisting_coms_alias", preexisting_coms_alias)

    # Perform a left join using explicit aliases
    updated_coms_df = (
        saved_communities_alias
        .join(preexisting_coms_alias,
            F.col("pre.community") == F.col("saved.cid"),
            "left"
        )
        .select(F.col("pre.community"), F.col("saved.nodes").alias("nodes"))
    ).cache()

    printTrace("[update_shared_coms] updated_coms_df", updated_coms_df)

    # If a community doesn't exist in saved_communities, provide an empty list instead of NULL
    updated_coms_df = updated_coms_df.withColumn(
        "nodes", F.when(F.col("nodes").isNotNull(), F.col("nodes")).otherwise(F.array())
    )

    filtered_coms_df = updated_coms_df.filter(F.expr("size(nodes) > 3"))

    to_destroy_coms = updated_coms_df.filter(F.expr("size(nodes) < 3"))
    saved_communities = destroy_communities(self, to_destroy_coms, saved_communities=saved_communities)

    exploded_coms_df = filtered_coms_df.withColumn("node", F.explode(F.col("nodes"))).select("community", "node")

    subgraph_edges_df = (
        existing_edges.alias("edges")
        .join(
            exploded_coms_df.alias("nodes_src"),
            F.col("edges.src") == F.col("nodes_src.node"),
            "inner"
        )
        .join(
            exploded_coms_df.alias("nodes_dst"),
            (F.col("edges.dst") == F.col("nodes_dst.node")) &
            (F.col("nodes_src.community") == F.col("nodes_dst.community")),  # Ensure same community
            "inner"
        )
        .select("edges.src", "edges.dst", "edges.weight", "edges.tags", "edges.timestamp", "nodes_src.community")
        .distinct()  # Remove any remaining duplicates
    )
    from graphframes import GraphFrame

    node_communities_df = (
        exploded_coms_df
        .groupBy("node")
        .agg(F.collect_set("community").alias("communities"))
    )

    subgraph_edges_df_agg = subgraph_edges_df.groupBy("src", "dst", "weight", "tags", "timestamp").agg(
        F.collect_set("community").alias("communities"))

    vertices_df = node_communities_df.withColumnRenamed("node", "id")

    vertices_df.cache()
    vertices_df.count()
    subgraph_edges_df_agg.cache()
    subgraph_edges_df_agg.count()

    subgraph = GraphFrame(vertices_df, subgraph_edges_df_agg)

    # Compute connected components
    printMsg("before connectedComponents calc")
    connected_components = subgraph.connectedComponents()
    printMsg("after connectedComponents calc")

    printTrace("Connected components:", connected_components)

    connected_components = (
        connected_components.alias("cc")
        .join(
            exploded_coms_df.alias("ec"),
            F.col("cc.id") == F.col("ec.node"),
            "inner"
        )
        .select(F.col("ec.community"), F.col("cc.id"), F.col("cc.component"), F.col("node"))
    ).cache()

    community_components = (
        connected_components.groupBy("community")
        .agg(
            F.countDistinct("component").alias("num_components"),
            F.collect_set("component").alias("components"),
            F.collect_set("id").alias("nodes")
        )
    ).cache()

    unbroken_coms_df = community_components.filter(F.col("num_components") == 1)
    printTrace("Unbroken communities unbroken_coms_df:", unbroken_coms_df)

    broken_coms_df = community_components.filter(F.col("num_components") != 1)
    printTrace("Broken communities broken_coms_df:", broken_coms_df)

    all_node_neighbors = get_all_neighbors(self, all_edges=saved_edges)

    # c_components == 1 -> unbroken community
    if not unbroken_coms_df.isEmpty():
        printMsg("-- Processing unrboken communities --")
        exploded_shared_coms = shared_coms.withColumn("affected_node", F.explode(F.col("affected_nodes")))
        aggregated_shared_coms = (
            exploded_shared_coms
            .groupby("affected_node")
            .agg(F.collect_set("community").alias("communities_from_shared_coms"))
        )
        subGraphVertices: DataFrame = subgraph.vertices

        exploded_unbroken = unbroken_coms_df.withColumn("node", F.explode(F.col("nodes")))
        aggregated_unbroken_coms = (
            exploded_unbroken
            .groupby("node")
            .agg(F.collect_set("community").alias("communities_from_unbroken"))
        )

        node_of_graph_and_shared_coms = (
        subGraphVertices.alias("sub")  # check if i might need existing_edges here
            .join(
                aggregated_shared_coms.alias("expl_nodes"),
                F.col("sub.id") == F.col("expl_nodes.affected_node"),
                how="inner"
            ).alias("sub_shared")
            .join(
                aggregated_unbroken_coms.alias("unbroken"),
                F.col("sub_shared.id") == F.col("unbroken.node"),
                how="inner"
            )
        ).select("sub_shared.id", "unbroken.communities_from_unbroken")

        printTrace("[shared_coms - unbroken community] node_of_graph_and_shared_coms", node_of_graph_and_shared_coms)
        ### Centrality Test (start) ###

        ### Neighbor Detection (start) ###

        subNode_neighbors = (node_of_graph_and_shared_coms.withColumnRenamed("id", "node")
                                    .join(all_node_neighbors, "node", "left_outer")) # CHECK IF THIS OUTER IS WORKING AS EXPECTED

        ### Neighbor Detection (end) ###

        valid_nodes = subNode_neighbors.filter(F.size("neighbors") > 1)

        exploded_neigh = valid_nodes.select(F.col("node").alias("TheNode"), F.explode("neighbors").alias("TheNode_neighbor"), F.col("neighbors").alias("TheNode_neighbors"))
        # printTrace("[shared_coms] neigh detection exploded_neigh1", exploded_neigh)

        exploded_neigh = exploded_neigh.filter(F.col("TheNode") > F.col("TheNode_neighbor"))
        # printTrace("[shared_coms] neigh detection exploded_neigh2", exploded_neigh)

        neighbors_neighbor = (exploded_neigh.withColumnRenamed("TheNode_neighbor", "node")
                                    .join(all_node_neighbors, "node", "left_outer"))

        neighbors_neighbor = neighbors_neighbor.withColumnsRenamed({'node': 'TheNode_neighbor', 'neighbors':'neighs_of_neigh'}).select("TheNode", "TheNode_neighbors", "TheNode_neighbor", "neighs_of_neigh")

        # printTrace("[shared_coms] neigh detection neighbors_neighbor", neighbors_neighbor)

        common_neighbors_df = neighbors_neighbor.withColumn(
            "common_neighbors",
            F.array_intersect(F.col("TheNode_neighbors"), F.col("neighs_of_neigh"))
        )
        common_neighbors_df = common_neighbors_df.filter(F.size("common_neighbors") > 0)
        # printTrace("[shared_coms] common_neighbors_df ", common_neighbors_df)

        central_nodes = common_neighbors_df.select(
            F.explode(F.array_union(F.array("TheNode", "TheNode_neighbor"), F.col("common_neighbors"))).alias(
                "central_node")
        ).distinct()

        printTrace("[shared_coms] central_nodes of unbroken com", central_nodes)
        ### Centrality Test (end)   ###


        aggr_nodes_per_community = ((node_of_graph_and_shared_coms.withColumn("community_from_unbroken", F.explode("communities_from_unbroken"))
                                    ).drop("communities_from_unbroken").groupBy("community_from_unbroken")
                                    .agg(F.collect_set("id").alias("com_nodes")))

        central_nodes = central_nodes.agg(F.collect_list("central_node").alias("central_nodes"))
        destroy_com_central = central_nodes.filter(F.expr("size(central_nodes) < 3"))
        if not destroy_com_central.isEmpty():
            saved_communities = destroy_communities(self, aggr_nodes_per_community, saved_communities=saved_communities)

        exploded_central_nodes = central_nodes.withColumn("central_node", F.explode("central_nodes"))
        not_central_nodes = (node_of_graph_and_shared_coms.alias("node_of_graph")
                             .join(exploded_central_nodes.alias("expl_central"),
                                   F.col("node_of_graph.id") == F.col("expl_central.central_node"), "left_anti")).withColumnRenamed("communities_from_unbroken", "community")
        printTrace("[shared_coms] not_central_nodes", not_central_nodes)

        saved_vertices, communityTagsDf, saved_communities = remove_from_community(self, not_central_nodes,
                                                                                   saved_vertices=saved_vertices, communityTagsDf=communityTagsDf,
                                                                                        communitiesDf=saved_communities, isCentralDf=True)

        to_remove_unbr_com = (aggr_nodes_per_community.alias("agg_nodes_shared_coms")
                              .join(subgraph_edges_df.alias("sub_edges"),
                                    on=F.col("agg_nodes_shared_coms.community_from_unbroken") == F.col("sub_edges.community"),
                                    how="inner")
                              .select(F.col("src").alias("node_u"), F.col("dst").alias("node_v"), "tags", "weight", "timestamp",
                                      "agg_nodes_shared_coms.community_from_unbroken",
                                      "agg_nodes_shared_coms.com_nodes"))

        to_remove_unbr_com = to_remove_unbr_com.withColumnRenamed("community_from_unbroken", "community")
        printTrace("to_remove_unbr_com", to_remove_unbr_com)
        saved_vertices, communityTagsDf, saved_communities = remove_from_community(self, to_remove_unbr_com,
                                                                                   saved_vertices=saved_vertices, communityTagsDf=communityTagsDf,
                                                                                        communitiesDf=saved_communities)

        printMsg("-- Processing unrboken communities (END) --")

        ### Modify After Removal (End)    ###
    if not broken_coms_df.isEmpty():
        printMsg("-- Processing broken communities --")
        agg_comps = (
            connected_components.groupBy("component")
            .agg(
                F.collect_set("id").alias("nodes"),
                F.countDistinct("id").alias("num_nodes"),
                F.collect_set("community").alias("communities")
            )
        )
        printTrace("agg_comps", agg_comps)

        #
        biggest_component = agg_comps.orderBy(F.desc("num_nodes")).limit(1)

        to_destroy_coms = biggest_component.filter(F.expr("size(nodes) < 3")).withColumn("community", F.explode("communities"))
        saved_communities = destroy_communities(self, to_destroy_coms, saved_communities=saved_communities) # should skip the following code part as in else

        to_modify_coms_of_biggest_comp = biggest_component.filter(F.expr("size(nodes) > 2"))
        to_modify_coms_of_biggest_comp_exploded_coms = to_modify_coms_of_biggest_comp.withColumn("community", F.explode(F.col("communities")))
        # printTrace("to_modify_coms_of_biggest_comp_exploded_coms", to_modify_coms_of_biggest_comp_exploded_coms)

        shared_nodes_of_biggest_comp = \
            (to_modify_coms_of_biggest_comp_exploded_coms.alias("big_to_mod_expl")
            .join(shared_coms.alias("shared_coms"), on=F.col("shared_coms.community") == F.col("big_to_mod_expl.community"), how="inner"
        ).select("big_to_mod_expl.nodes", "big_to_mod_expl.communities", "big_to_mod_expl.community").alias("bigMod_sharedComs")
            .join(broken_coms_df.alias("broken_coms_df"),on=F.col("bigMod_sharedComs.community") == F.col("broken_coms_df.community"), how="left_outer" #check if needs outer here
        ).withColumn("shared_nodes", F.array_intersect(F.col("bigMod_sharedComs.nodes"), F.col("broken_coms_df.nodes"))).select(
            "bigMod_sharedComs.community", "shared_nodes", "bigMod_sharedComs.communities"))
        # printTrace("- In shared_coms shared_nodes_of_biggest_comp", shared_nodes_of_biggest_comp)

        # LEFT HERE
        # Need to adjust modify after removal -> centrality test into accepting a unified dataframe to be used multiple times
        # shared_nodes_of_biggest_comp has the nodes coms and com
        # I need to find the neighbors of each node (from above nodes)
        # and somehow get the current tags for modify after removal
        # CHECK if the neighbors method is working, last time something was wrong throwing huge stacktrace
        # then check central nodes if working and then proceed

        existing_edges_join_big_comp = (shared_nodes_of_biggest_comp.alias("shared_big")
                                        .join(subgraph_edges_df.alias("sub_edges"),
                                              on=F.col("shared_big.community") == F.col("sub_edges.community"),
                                              how="inner")
                                        .select("src", "dst", "tags", "weight", "timestamp",
                                                "shared_big.community", "communities", "shared_big.shared_nodes"))
        printTrace("[shared cms - biggest comp] existing_edges_join_big_comp", existing_edges_join_big_comp)

        ### Centrality Test (start) ###

        ### Neighbor Detection (start) ###

        subNode_neighbors_big = existing_edges_join_big_comp.withColumn("node", F.explode("shared_nodes")).join(all_node_neighbors, "node", "left_outer")

        ### Neighbor Detection (end) ###

        valid_nodes_big = subNode_neighbors_big.filter(F.size("neighbors") > 1)

        exploded_neigh_big = valid_nodes_big.select(F.col("node").alias("TheNode"), F.explode("neighbors").alias("TheNode_neighbor"), F.col("neighbors").alias("TheNode_neighbors"))
        # printTrace("[shared_coms] neigh detection exploded_neigh1", exploded_neigh)

        exploded_neigh_big = exploded_neigh_big.filter(F.col("TheNode") > F.col("TheNode_neighbor"))
        # printTrace("[shared_coms] neigh detection exploded_neigh2", exploded_neigh)

        neighbors_neighbor_big = (exploded_neigh_big.withColumnRenamed("TheNode_neighbor", "node")
                                    .join(all_node_neighbors, "node", "left_outer"))

        neighbors_neighbor_big = neighbors_neighbor_big.withColumnsRenamed({'node': 'TheNode_neighbor', 'neighbors':'neighs_of_neigh'}).select("TheNode", "TheNode_neighbors", "TheNode_neighbor", "neighs_of_neigh")

        # printTrace("[shared_coms] neigh detection neighbors_neighbor", neighbors_neighbor)

        common_neighbors_df_big = neighbors_neighbor_big.withColumn(
            "common_neighbors",
            F.array_intersect(F.col("TheNode_neighbors"), F.col("neighs_of_neigh"))
        )
        common_neighbors_df_big = common_neighbors_df_big.filter(F.size("common_neighbors") > 0)
        # printTrace("[shared_coms] common_neighbors_df_big ", common_neighbors_df_big)

        exploded_central_nodes_big = common_neighbors_df_big.select(
            F.explode(F.array_union(F.array("TheNode", "TheNode_neighbor"), F.col("common_neighbors"))).alias("central_node")
        ).distinct()

        central_nodes_big = exploded_central_nodes_big.agg(F.collect_list("central_node").alias("central_nodes"))
        destroy_com_central_big = central_nodes_big.filter(F.expr("size(central_nodes) < 3"))
        if not destroy_com_central_big.isEmpty():
            saved_communities = destroy_communities(self, existing_edges_join_big_comp, saved_communities=saved_communities)

        existing_edges_join_big_comp_central = existing_edges_join_big_comp.select(F.explode(F.array(F.col("src"), F.col("dst")))
                                                                                   .alias("id"), F.col("tags"), F.col("weight"), "timestamp",
                                                                                   "community", "communities", "shared_nodes")
        not_central_nodes_big = (existing_edges_join_big_comp_central.alias("node_of_graph").join(exploded_central_nodes_big.alias("expl_central"),
                                   F.col("node_of_graph.id") == F.col("expl_central.central_node"), "left_anti")
                                 ).withColumnRenamed("communities_from_unbroken", "community")

        printTrace("[shared_coms] not_central_nodes_big", not_central_nodes_big)

        saved_vertices, communityTagsDf, saved_communities = remove_from_community(self, not_central_nodes_big,
                                                                                   saved_vertices=saved_vertices, communityTagsDf=communityTagsDf,
                                                                                        communitiesDf=saved_communities, isCentralDf=True)



        # printTrace("[shared_coms] central_nodes_big of unbroken com", exploded_central_nodes_big)
        ### Centrality Test (end) ###

        to_remove_broken_cm = existing_edges_join_big_comp.select(F.col("src").alias("node_u"), F.col("dst").alias("node_v"), "tags", "weight", "timestamp", "community")
        saved_vertices, communityTagsDf, saved_communities =  remove_from_community(self, to_remove_broken_cm,
                                                                                   saved_vertices=saved_vertices, communityTagsDf=communityTagsDf,
                                                                                        communitiesDf=saved_communities)

        other_component = agg_comps.alias("agg_comps").join(biggest_component.alias("big"), F.col("agg_comps.component") == F.col("big.component"),
                                                            "left_anti")

        other_component = other_component.filter(F.expr("size(nodes) > 3")).withColumn("community", F.explode("communities"))

        to_modify_coms_of_other_comp_exploded_coms = other_component.withColumn("community", F.explode(
            F.col("communities")))
        # printTrace("to_modify_coms_of_biggest_comp_exploded_coms", to_modify_coms_of_biggest_comp_exploded_coms)

        shared_nodes_of_other_comp = \
            (to_modify_coms_of_other_comp_exploded_coms.alias("other_to_mod_expl")
            .join(shared_coms.alias("shared_coms"),
                  on=F.col("shared_coms.community") == F.col("other_to_mod_expl.community"), how="inner"
                  ).select("other_to_mod_expl.nodes", "other_to_mod_expl.communities", "other_to_mod_expl.community").alias(
                "otherMod_sharedComs")
            .join(broken_coms_df.alias("broken_coms_df"),
                  on=F.col("otherMod_sharedComs.community") == F.col("broken_coms_df.community"), how="left_outer"
                  # check if needs outer here
                  ).withColumn("shared_nodes", F.array_intersect(F.col("otherMod_sharedComs.nodes"),
                                                                 F.col("otherMod_sharedComs.nodes"))).select(
                "otherMod_sharedComs.community", "shared_nodes", "otherMod_sharedComs.communities"))

        existing_edges_join_other_comp = (shared_nodes_of_other_comp.alias("shared_other")
                                        .join(subgraph_edges_df.alias("sub_edges"),
                                              on=F.col("shared_other.community") == F.col("sub_edges.community"),
                                              how="inner")
                                        .select("src", "dst", "tags", "weight", "timestamp",
                                                "shared_other.community", "communities", "shared_other.shared_nodes"))
        printTrace("[shared cms - other comp] existing_edges_join_other_comp", existing_edges_join_other_comp)

        ### Centrality Test (start) ###

        ### Neighbor Detection (start) ###

        subNode_neighbors_other = existing_edges_join_other_comp.withColumn("node", F.explode("shared_nodes")).join(
            all_node_neighbors, "node", "left_outer")

        ### Neighbor Detection (end) ###

        valid_nodes_other = subNode_neighbors_other.filter(F.size("neighbors") > 1)

        exploded_neigh_other = valid_nodes_other.select(F.col("node").alias("TheNode"),
                                                    F.explode("neighbors").alias("TheNode_neighbor"),
                                                    F.col("neighbors").alias("TheNode_neighbors"))
        # printTrace("[shared_coms] neigh detection exploded_neigh1", exploded_neigh)

        exploded_neigh_other = exploded_neigh_other.filter(F.col("TheNode") > F.col("TheNode_neighbor"))
        # printTrace("[shared_coms] neigh detection exploded_neigh2", exploded_neigh)

        neighbors_neighbor_other = (exploded_neigh_other.withColumnRenamed("TheNode_neighbor", "node")
                                  .join(all_node_neighbors, "node", "left_outer"))

        neighbors_neighbor_other = neighbors_neighbor_other.withColumnsRenamed(
            {'node': 'TheNode_neighbor', 'neighbors': 'neighs_of_neigh'}).select("TheNode", "TheNode_neighbors",
                                                                                 "TheNode_neighbor", "neighs_of_neigh")

        # printTrace("[shared_coms] neigh detection neighbors_neighbor", neighbors_neighbor)

        common_neighbors_df_other = neighbors_neighbor_other.withColumn(
            "common_neighbors",
            F.array_intersect(F.col("TheNode_neighbors"), F.col("neighs_of_neigh"))
        )
        common_neighbors_df_other = common_neighbors_df_other.filter(F.size("common_neighbors") > 0)
        # printTrace("[shared_coms] common_neighbors_df_big ", common_neighbors_df_big)

        exploded_central_nodes_other = common_neighbors_df_other.select(
            F.explode(F.array_union(F.array("TheNode", "TheNode_neighbor"), F.col("common_neighbors"))).alias(
                "central_node")
        ).distinct()

        central_nodes_other = exploded_central_nodes_other.agg(F.collect_list("central_node").alias("central_nodes"))

        create_com_central_other = central_nodes_other.filter(F.expr("size(central_nodes) >= 3"))

        existing_edges_join_other_comp_central = existing_edges_join_other_comp.select(
            F.explode(F.array(F.col("src"), F.col("dst")))
            .alias("id"), F.col("tags"), F.col("weight"), "timestamp",
            "community", "communities", "shared_nodes")

        create_com_central_other = create_com_central_other.withColumn("central_node", F.explode("central_nodes"))
        central_nodes_other_full = (existing_edges_join_other_comp_central.alias("node_of_graph").join(
            create_com_central_other.alias("expl_central"),F.col("node_of_graph.id") == F.col("expl_central.central_node"), "inner")
            ).withColumnRenamed("communities_from_unbroken", "community").withColumn("new_community_id", F.expr("uuid()"))

        communityTagsDf, saved_communities, saved_vertices = (
            add_to_community_streaming(self, saved_vertices, central_nodes_other_full, saved_communities, communityTagsDf))
        # if not destroy_com_central_other.isEmpty():
        #     saved_communities = destroy_communities(self, existing_edges_join_other_comp,
        #                                             saved_communities=saved_communities)
        #

        # not_central_nodes_other = (existing_edges_join_other_comp_central.alias("node_of_graph").join(
        #     exploded_central_nodes_other.alias("expl_central"),
        #     F.col("node_of_graph.id") == F.col("expl_central.central_node"), "left_anti")
        #                          ).withColumnRenamed("communities_from_unbroken", "community")
        #
        # printTrace("[shared_coms] not_central_nodes_big", not_central_nodes_other)
        #
        # saved_vertices, communityTagsDf, saved_communities = remove_from_community(self, not_central_nodes_other,
        #                                                                            saved_vertices=saved_vertices,
        #                                                                            communityTagsDf=communityTagsDf,
        #                                                                            communitiesDf=saved_communities,
        #                                                                            isCentralDf=True)

        # printTrace("[shared_coms] central_nodes_big of unbroken com", exploded_central_nodes_big)
        ### Centrality Test (end) ###



        return saved_vertices, communityTagsDf, saved_communities

        printMsg("Should run modify_after_removal for big comp df")


        # all_edges = loadState(self=self, pathToload=self.edges_path, schemaToCreate=self.edges_schema)
        # modify_after_removal(self, all_edges, shared_nodes_of_biggest_comp)
        # neighs_of_biggest_comp = common_neighbor_detection_of_nodes(all_edges, shared_nodes_of_biggest_comp)
        # printTrace("neighs_of_biggest_comp", neighs_of_biggest_comp)

        # central_nodes = find_central_nodes(all_edges, neighs_of_biggest_comp)
        # printTrace("central_nodes of biggest components", central_nodes)
        #
        #
        # exploded_shared_coms = shared_coms.withColumn("affected_node", F.explode(F.col("affected_nodes")))
        # aggregated_shared_coms = (
        #     exploded_shared_coms
        #     .groupby("affected_node")
        #     .agg(F.collect_set("community").alias("communities_from_shared_coms"))
        # )
        # node_of_graph_and_shared_coms = (
        #     broken_coms_df.alias("sub")
        #     .join(
        #         aggregated_shared_coms.alias("expl_nodes"),
        #         F.col("sub.id") == F.col("expl_nodes.affected_node"),
        #         how="inner"
        #     ).select(F.col("sub.id"), F.col("communities_from_shared_coms"))
        # )
        #
        #
        # modify_after_removal()
        #
        #
        # first_comps_df = broken_coms_df.select(
        #     "community", "num_nodes",
        #     F.expr("components[0]").alias("component")
        # )
        # first_joined_comps = agg_comps.alias("agg_comps").join(first_comps_df.alias("first"),
        #                                                  on=F.col("agg_comps.component") == F.col("first.component"),
        #                                                  how="inner")
        # printTrace("first_joined_comps (first node extraction)", first_joined_comps)
        #
        #
        # to_destroy_coms = first_joined_comps.filter(F.expr("size(nodes) < 3"))
        # to_destroy_coms = to_destroy_coms.withColumn("community", F.explode("communities"))
        #
        # first_to_mod_comps = first_joined_comps.filter(F.expr("size(nodes) >= 3"))
        # printTrace("first_to_mod_comps", first_to_mod_comps)
        #
        # if not to_destroy_coms.isEmpty():
        #     destroy_communities(self, to_destroy_coms)
        # else:
        #     exploded_shared_coms = shared_coms.withColumn("affected_node", F.explode(F.col("affected_nodes")))
        #
        #     aggregated_shared_coms = (exploded_shared_coms.groupby("affected_node")
        #                               .agg(F.collect_set("community").alias("communities_from_shared_coms")))
        #
        #     printTrace("aggregated_shared_coms", aggregated_shared_coms)
        #
        #     exploded_first_comps_df = first_to_mod_comps.withColumn("node", F.explode("nodes"))
        #     nodes_for_modify = aggregated_shared_coms.join(
        #         exploded_first_comps_df, F.col("node") == F.col("affected_node"),
        #         "inner"
        #     ).select("community", "node").distinct()
        #
        #     printTrace("nodes_for_modify", nodes_for_modify)
        #     modify_after_removal(self, nodes_for_modify, common_neighbors_df)
        #
        #
        # agg_comps = (connected_components.groupBy("component").agg(F.collect_set("community").alias("communities"),
        #                                                            F.collect_set("id").alias("nodes"))
        #              .select("component", "nodes", "communities"))
        # exploded_broken_coms_df = broken_coms_df.withColumn("component", F.explode("components"))
        #
        # joined_comps = agg_comps.alias("agg_comps").join(exploded_broken_coms_df.alias("broken"),
        #                                                  on=F.col("agg_comps.component") == F.col("broken.component"),
        #                                                  how="inner")
        #
        # except_first_comp = joined_comps.select(
        #     "community", "num_components",
        #     F.expr("slice(components, 2, size(components) - 1)").alias("remaining_nodes")  # Keep all except first
        # )
        # remaining_comps = except_first_comp.filter(F.expr("size(remaining_nodes) > 3"))
        # # remaining_comps = remaining_comps.withColumn("remaining_node", F.explode(F.col("remaining_nodes")))
        #
        # exploded_shared_coms = shared_coms.withColumn("affected_node", F.explode(F.col("affected_nodes")))
        #
        # aggregated_shared_coms = (exploded_shared_coms.groupby("affected_node")
        #                           .agg(F.collect_set("community").alias("communities_from_shared_coms")))
        #
        # # printTrace("exploded_shared_coms", exploded_shared_coms)
        # nodes_for_modify = aggregated_shared_coms.join(
        #     remaining_comps, F.col("remaining_node") == F.col("affected_node"),
        #     "inner"
        # ).select("community", "affected_node").distinct()
        #
        # printTrace("nodes_for_modify", nodes_for_modify)
        #
        #
        # central = centrality_test(self, nodes_for_modify, common_neighbors_df)
        # printTrace("inside update_shared_coms - central", central)


    # TODO
    # before exploding, perhaps check for 'len(com) < 3' logic to destroy community
    # need to also check if shared_coms are actually correct cuz i see many duplicate 'affected_nodes'
    # then figure oute the modify_after_removal logic
    # try avoid graphframe if possible, too much performance degradation

    # exploded_components_broken_coms = broken_coms_df.withColumn("exploded_components", F.explode(F.col("components")))


def get_all_neighbors(self, all_edges: DataFrame):
    # all_edges = loadState(self=self, pathToload=self.edges_path, schemaToCreate=self.edges_schema)

    # neighbors = all_edges.select(
    #     F.col("src").alias("node"),
    #     F.col("dst").alias("neighbor")
    # ).union(
    #     all_edges.select(
    #         F.col("dst").alias("node"),
    #         F.col("src").alias("neighbor")
    #     )
    # ).distinct()
    neighbors = (
        all_edges.select(F.explode(F.array(F.struct(F.col("src").alias("node"), F.col("dst").alias("neighbor")),
                                           F.struct(F.col("dst").alias("node"), F.col("src").alias("neighbor")))))
        .select("col.*")
        .distinct()
    )

    # Step 2: Aggregate to get list of neighbors per node
    node_neighbors = neighbors.groupBy("node").agg(F.collect_set("neighbor").alias("neighbors"))

    return node_neighbors

def destroy_communities(self, communitiesToRemove: DataFrame, saved_communities: DataFrame):
    if communitiesToRemove.isEmpty():
        return saved_communities
    printMsg("- Destroying communities")
    # saved_communities = loadState(self, pathToload=self.communities_path, schemaToCreate=self.communities_schema)
    printTrace("-  communitiesToRemove", communitiesToRemove)
    printTrace("-  saved_communities", saved_communities)
    updated_communities = saved_communities.alias("saved_communities").join(
            communitiesToRemove.alias("communitiesToRemove"),
            (F.col("communitiesToRemove.community") == F.col("saved_communities.cid")),
            how="left_anti"
        )

    printTrace("- destroy_communities updated_communities:", updated_communities)
    # saveState(updated_communities, self.communities_path)
    # saved_communities = loadState(self, pathToload=self.communities_path, schemaToCreate=self.communities_schema)
    # printTrace("- saved_communities after destroying", saved_communities)
    printMsg("- Finished destroying communities")
    return updated_communities

def modify_after_removal(self, central_nodes, sub_Nodes, sub_edges):
    print("[modify_after_removal] started processing")

# def modify_after_removal(self, subgraph_edges_df:DataFrame, dfWithNodesArray:DataFrame):
#     # all_edges = loadState(self=self, pathToload=self.edges_path, schemaToCreate=self.edges_schema)
#     central_nodes = find_central_nodes(subgraph_edges_df, dfWithNodesArray)
#     printTrace(" - in mod_after_removal central_nodes", central_nodes)


def find_central_nodes(all_edges: DataFrame, dfWithNodesArray: DataFrame):
    neighbors = common_neighbor_detection_of_nodes(all_edges, dfWithNodesArray)

    valid_nodes = neighbors.filter(F.size("neighbors") > 1)

    # Step 2: Create node pairs (u, v) ensuring u > v
    node_pairs = valid_nodes.alias("u").join(
        valid_nodes.alias("v"),
        (F.col("u.node") > F.col("v.node")),
        "inner"
    ).select(
        F.col("u.node").alias("node_u"),
        F.col("v.node").alias("node_v")
    )

    # Step 3: Find common neighbors (cn = neighbors_u & neighbors_v)
    common_neighbors = node_pairs.join(
        valid_nodes.alias("nu"), F.col("node_u") == F.col("nu.node"), "inner"
    ).join(
        valid_nodes.alias("nv"), F.col("node_v") == F.col("nv.node"), "inner"
    ).select(
        F.col("node_u"),
        F.col("node_v"),
        F.array_intersect("nu.neighbors", "nv.neighbors").alias("common_neighbors")
    ).filter(F.size("common_neighbors") > 0)

    # Step 4: Collect all central nodes (u, v, and common neighbors)
    central_nodes = common_neighbors.selectExpr("node_u as central_node").union(
        common_neighbors.selectExpr("node_v as central_node")
    ).union(
        common_neighbors.selectExpr("explode(common_neighbors) as central_node")
    ).distinct()

    return central_nodes

def centrality_test(self, subgraph_edges_df:DataFrame, neighbors_df:DataFrame):
    if subgraph_edges_df.isEmpty():
        return subgraph_edges_df
    # Step 1: Extract neighbors for each node in the subgraph
    subgraph_neighbors = (
        subgraph_edges_df.alias("edges")
        .join(neighbors_df.alias("nbrs"), F.col("edges.id") == F.col("nbrs.node"), "inner")
        # .join(neighbors_df.alias("nbrs"), F.col("edges.src") == F.col("nbrs.node"), "inner")
        .select(F.col("edges.id").alias("node"), F.col("nbrs.neighbor").alias("neighbor"))
        .union(
            subgraph_edges_df.alias("edges")
            .join(neighbors_df.alias("nbrs"), F.col("edges.dst") == F.col("nbrs.node"), "inner")
            .select(F.col("edges.dst").alias("node"), F.col("nbrs.neighbor").alias("neighbor"))
        )
    )

    # Step 2: Find shared neighbors (cn = neighbors_u & neighbors_v)
    common_neighbors_df = (
        subgraph_neighbors.alias("n1")
        .join(subgraph_neighbors.alias("n2"),
              (F.col("n1.neighbor") == F.col("n2.neighbor")) & (F.col("n1.node") > F.col("n2.node")),
              "inner")
        .select(F.col("n1.node").alias("u"), F.col("n2.node").alias("v"), F.col("n1.neighbor").alias("common_neighbor"))
    )

    # Step 3: Extract central nodes (u, v, and cn)
    central_nodes_df = (
        common_neighbors_df
        .select("u").union(common_neighbors_df.select("v")).union(common_neighbors_df.select("common_neighbor"))
        .distinct()
    )
    printTrace("central_nodes_df", central_nodes_df)
    return central_nodes_df

def remove_from_community(self, df:DataFrame, saved_vertices: DataFrame, communityTagsDf: DataFrame, communitiesDf: DataFrame, isCentralDf= False):
    if not df or df.isEmpty():
        return saved_vertices, communityTagsDf, communitiesDf

    printMsg("removing from community")
    # nodes_to_remove_comms = (df.select(F.col("node_u").alias("node"), "tags", "weight", "timestamp", "community")
    #                          .union(df.select(F.col("node_v").alias("node"), "tags", "weight", "timestamp", "community")).distinct())
    if not isCentralDf:
        nodes_to_remove_comms = df.select(F.explode(F.array(F.col("node_u"), F.col("node_v"))).alias("node"), "tags", "weight", "timestamp", "community").distinct()
    else:
        nodes_to_remove_comms = df.withColumnRenamed("id", "node")

    printTrace("[remove com] nodes_to_remove_comms", nodes_to_remove_comms)
    # saved_vertices = loadState(self=self, pathToload=self.vertices_path, schemaToCreate=self.vertices_schema)
    current_vertices_with_coms = nodes_to_remove_comms.join(saved_vertices, F.col("node") == F.col("id"), "left_outer")
    # exploded_current_vertices_with_coms = current_vertices_with_coms.withColumn("c_com", F.explode("c_coms"))

    # to_remove_coms = nodes_to_remove_comms.alias("to_remove").join(exploded_current_vertices_with_coms.alias("exploded_vertices"), F.col("to_remove.community") == F.col("exploded_vertices.c_com"), "left_outer")
    # printTrace("to_remove_coms", to_remove_coms)

    ## Community - Vertices removal ##
    # printTrace("[remove com] current_vertices_with_coms", current_vertices_with_coms)

    nodes_with_removal_comms = (current_vertices_with_coms.distinct()
        .groupBy("node")
        .agg(F.collect_set("community").alias("communities_to_remove"))
    )
    printTrace("[remove com] nodes_with_removal_comms", nodes_with_removal_comms)

    updated_vertices = (
        saved_vertices.alias("sv")
        .join(nodes_with_removal_comms.alias("r"), F.col("sv.id") == F.col("r.node"), "left_outer")
        .withColumn(
            "c_coms",
            F.when(
                F.col("r.communities_to_remove").isNotNull(),
                # Filter out any community that appears in the removal list
                F.expr("filter(c_coms, x -> not array_contains(communities_to_remove, x))")
            ).otherwise(F.col("c_coms"))
        )
        .drop("node", "communities_to_remove")
    )
    printTrace("[remove com] updated_vertices (after removal of coms)", updated_vertices)
    printTrace("[remove com] saved_vertices (all vertices)", saved_vertices)
    ## Community - Vertices removal ##

    # Community - Tags removal ##
    removal_tags_df = (
        current_vertices_with_coms
        # nodes_to_remove_comms
        .select("community", F.explode("tags").alias("tag"))
        .groupBy("community")
        .agg(F.collect_set("tag").alias("removal_tags"))
    )
    printTrace("[remove com] removal_tags_df", removal_tags_df)

    # communityTagsDf = loadState(self=self, pathToload=self.communityTags_path, schemaToCreate=self.communityTags_schema)
    printMsg("[remove com] Loaded communityTags fully shown")
    communityTagsDf.show(n=500, truncate=False)

    updated_communityTagsDf = (
        communityTagsDf.alias("ct")
        .join(removal_tags_df.alias("r"), F.col("ct.cid") == F.col("r.community"), "left_outer")
        .withColumn(
            "tags",
            F.when(
                F.col("r.removal_tags").isNotNull(),
                # Remove each tag x if it appears in removal_tags
                F.expr("filter(tags, x -> NOT array_contains(r.removal_tags, x))")
            ).otherwise(F.col("tags"))
        )
        .select("ct.cid", "tags")
    )
    printTrace("[remove com] updated_communityTagsDf", updated_communityTagsDf)
    printTrace("[remove com] all community tags", communityTagsDf)
    # Community - Tags removal ##

    ## Community - Nodes removal ##
    # communitiesDf = loadState(self=self, pathToload=self.communities_path, schemaToCreate=self.communities_schema)
    removalNodesDf = (
        current_vertices_with_coms
        # nodes_to_remove_comms
        .select(F.col("node").cast("string"), F.col("community"))
        .distinct()
    )

    removalNodesAgg = (
        removalNodesDf.groupBy("community")
        .agg(F.collect_set("node").alias("nodes_to_remove"))
    )
    printTrace("[remove com] removalNodesAgg", removalNodesAgg)

    updated_communitiesDf = (
        communitiesDf.alias("comm")
        .join(removalNodesAgg.alias("rm"), F.col("comm.cid") == F.col("rm.community"), "left_outer")
        .withColumn(
            "nodes",
            F.expr("filter(nodes, x -> rm.nodes_to_remove is null or NOT array_contains(rm.nodes_to_remove, x))")
        )
        .select("cid", "nodes")
    )
    printTrace("[remove com] updated_communitiesDf (after removed)", updated_communitiesDf)
    printTrace("[remove com] all communities (communitiesDf)", communitiesDf)
    ## Community - Nodes removal ##

    # saveState(updated_vertices, self.vertices_path)
    # saveState(updated_communityTagsDf, self.communityTags_path)
    # saveState(updated_communitiesDf, self.communities_path)

    printMsg("finished removing edge(after save)")

    updated_vertices.cache()
    updated_vertices.count()
    updated_communityTagsDf.cache()
    updated_communityTagsDf.count()
    updated_communitiesDf.cache()
    updated_communitiesDf.count()

    return updated_vertices, updated_communityTagsDf, updated_communitiesDf


    # Step 2: First explode (breaks array into separate rows)
    # exploded_tags_df = current_vertices_with_coms.withColumn("tag_string", F.explode("tags"))

    # Step 3: Split comma-separated values into an array and explode again
    # exploded_tags_df = exploded_tags_df.withColumn("tag", F.explode(F.split(F.col("tag_string"), ","))).drop(
    #     "tag_string")

    # # Step 2: Explode tags
    # exploded_tags_df = nodes_to_remove_comms.withColumn("tag", F.explode(F.split(F.col("tags"), ",")))


    # communityTagsDf = communityTagsDf.withColumn("tag_string", F.explode("tags"))
    # communityTagsDf = communityTagsDf.withColumn("tag", F.explode(F.split(F.col("tag_string"), ","))).drop(
    #     "tag_string")

    # Step 3: Remove from CommunityTags - filter and remove tags
    # updated_communityTags_df = exploded_tags_df.join(
    #     communityTagsDf, exploded_tags_df["tag"] == communityTagsDf["tag"], "left_outer"
    # ).filter(F.col("community_id").isNotNull()).select("community_id", "tag").distinct()
    #
    # printTrace("remove from com updated_communityTags_df:", updated_communityTags_df)
    #
    # communityTags_removed_df = communityTagsDf.join(
    #     updated_communityTags_df,
    #     ["community_id", "tag"],
    #     "left_anti"  # Keep only the rows where community_id + tag does not match
    # )

    # Step 4: Remove nodes from communities
    # updated_communities_df_u = communitiesDf.join(
    #     updated_communityTags_df,
    #     (communitiesDf["node_id"] == df["node_u"]) & (communitiesDf["cid"] == updated_communityTags_df["community_id"]),
    #     "left_anti"  # Use left_anti to remove matching rows
    # )
    # printTrace("remove from com updated_communities_df_u:", updated_communities_df_u)
    #
    # updated_communities_df_v = communitiesDf.join(
    #     updated_communityTags_df,
    #     (communitiesDf["node_id"] == df["node_v"]) & (communitiesDf["cid"] == updated_communityTags_df["community_id"]),
    #     "left_anti"  # Use left_anti to remove matching rows
    # )
    # printTrace("remove from com updated_communities_df_v:", updated_communities_df_v)
    #
    # updated_communities_df = updated_communities_df_u.union(updated_communities_df_v).distinct()

    # printTrace("remove from com communityTags_removed_df:", communityTags_removed_df)
    # printTrace("remove from com updated_communities_df:", updated_communities_df)

    # saveState(communityTags_removed_df, self.communityTags_path)
    # saveState(updated_communities_df, self.communities_path)








def normalize_edges(edges: DataFrame):
    """
    Normalize edges such that (u, v) and (v, u) are treated as equivalent.
    Always store the edge as (min(u, v), max(u, v)).
    To maintain the undirected nature
    """
    return (edges.withColumn("src_normalized", F.least(F.col("src"), F.col("dst")))
            .withColumn("dst_normalized", F.greatest(F.col("src"), F.col("dst")))
            .drop("src", "dst")
            .withColumnRenamed("src_normalized", "src")
            .withColumnRenamed("dst_normalized", "dst").select(F.col("src"), F.col("dst"), F.col("timestamp"), F.col("tags"), F.col("weight")))

def update_graph_with_edges(all_edges: DataFrame, new_edges: DataFrame):
    normalized_new_edges = normalize_edges(new_edges)
    normalized_all_edges = normalize_edges(all_edges)

    # Step 1: Identify preexisting edges and count their occurrences in `normalized_new_edges`
    normalized_new_edges_renamed = (normalized_new_edges.withColumnRenamed("weight", "new_weight")
                                    .withColumnRenamed("timestamp", "new_timestamp")
                                    .withColumnRenamed("tags", "new_tags")
                                    )


    printTrace("normalized_new_edges_renamed", normalized_new_edges_renamed)

    new_edges_grouped = normalized_new_edges_renamed.groupBy("src", "dst").agg(
        F.count("*").alias("new_occurrences"),
        F.max(F.struct("new_timestamp", "new_tags")).alias("latest_entry")
    ).select("src", "dst", "new_occurrences",
        F.col("latest_entry.new_timestamp").alias("latest_timestamp"),
        F.col("latest_entry.new_tags").alias("latest_tags")
    )

    printTrace("new_edges_grouped", new_edges_grouped)

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

    totalEdges = updated_preexisting_edges.unionByName(normalized_all_edges).dropDuplicates(["src", "dst"]).sort("timestamp")

    return totalEdges

def saveStateVerticesAndEdges(self, vertices: DataFrame = None, edges: DataFrame = None):
    """
    Saves the current state of vertices and edges to Parquet files.
    """
    saveState(dataframeToBeSaved=vertices, pathToBeSaved=self.vertices_path)
    saveState(dataframeToBeSaved=edges, pathToBeSaved=self.edges_path)

def saveState(dataframeToBeSaved: DataFrame, pathToBeSaved: str):
    if dataframeToBeSaved is not None:
        dataframeToBeSaved.cache()
        dataframeToBeSaved.count()
        dataframeToBeSaved.write.mode("overwrite").format("parquet").save(pathToBeSaved)
        dataframeToBeSaved.unpersist()
        # dataframeToBeSaved.count()

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
        loadedDataframe.cache()
        loadedDataframe.count()
    except Exception:  # Handle case where file does not exist
        loadedDataframe = self.spark.createDataFrame([], schemaToCreate)

    return loadedDataframe


def loadStateVerticesAndEdges(self):
    vertices = loadState(self=self, pathToload=self.vertices_path, schemaToCreate=self.vertices_schema)
    edges = loadState(self=self, pathToload=self.edges_path, schemaToCreate=self.edges_schema)
    return vertices, edges