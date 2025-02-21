# if self.actual_time is None and self.last_break is None:
#     first_row = batch_df.first()
#     if first_row:
#         self.actual_time = datetime.datetime.fromtimestamp(first_row['timestamp'])
#         self.last_break = self.actual_time


def common_neighbors_analysis(self, u, v, common_neighbors, tags):
    if len(common_neighbors) < 1:
        return
    else:
        shared_coms = set(self.g.nodes[v]['c_coms'].keys()) & set(self.g.nodes[u]['c_coms'].keys())
        only_u = set(self.g.nodes[u]['c_coms'].keys()) - set(self.g.nodes[v]['c_coms'].keys())
        only_v = set(self.g.nodes[v]['c_coms'].keys()) - set(self.g.nodes[u]['c_coms'].keys())
        propagated = False
        for z in common_neighbors:
            for c in self.g.nodes[z]['c_coms'].keys():
                if c in only_v:
                    self.add_to_community(u, c, tags)
                    propagated = True
                if c in only_u:
                    self.add_to_community(v, c, tags)
                    propagated = True
            for c in shared_coms:
                if c not in self.g.nodes[z]['c_coms']:
                    self.add_to_community(z, c, tags)
                    propagated = True
        else:
            if not propagated:
                actual_cid = self.new_community_id
                self.add_to_community(u, actual_cid, tags)
                self.add_to_community(v, actual_cid, tags)
                for z in common_neighbors:
                    self.add_to_community(z, actual_cid, tags)

def add_to_community(self, node, cid, tag):
    flag = 1
    for x in tag.split(','):
        for tg in tag.split(','):
            self.CommunityTags[cid].append(tg)
        flag = 1
    if flag == 1:
        self.g.nodes[node]['c_coms'][cid] = None
        if cid in self.communities:
            self.communities[cid][node] = None
        else:
            self.communities[cid] = {node: None}

        # Check if the GraphFrame already exists (initialized in the driver)
        # if self.vertices is None or self.edges is None:
        #     if not self.vertices:
        #         self.vertices = new_nodes
        #     if not self.edges:
        #         self.edges = new_edges
        # else:
        #     # Filter new vertices
        #     new_nodes_filtered = new_nodes.join(self.vertices.select("id"), on="id", how="left_anti").dropDuplicates()
        #
        #     # Filter new edges
        #     new_edges_filtered = new_edges.join(self.edges.select("src", "dst"), on=["src", "dst"], how="left_anti").dropDuplicates()
        #
        #     printTrace("new_nodes_filtered", new_nodes_filtered)
        #     printTrace("new_edges_filtered", new_edges_filtered)
        #
        #     # Update vertices and edges
        #     updated_vertices = self.vertices.unionByName(new_nodes_filtered).distinct()
        #     updated_edges = self.edges.unionByName(new_edges_filtered)
        #
        #     printTrace("updated_vertices", updated_vertices)
        #     printTrace("updated_edges", updated_edges)
        #
            # self.vertices = updated_vertices
            # self.edges = updated_edges


            # # Get existing vertices from the current graph
            # # existing_vertices = self.g.vertices.alias("existing")
            #
            # # Perform a left anti-join to find new nodes not already in the graph
            # new_nodes_to_add = new_nodes.alias("new").join(
            #     self.g.vertices.alias("existing"),
            #     on="id",
            #     how="left_anti"
            # )
            # # If there are new nodes to add, update the GraphFrame vertices
            # # if not new_nodes_to_add.isEmpty():
            # printMsg("new_nodes_to_add - nodes that are new:")
            # # new_nodes_to_add.show()
            # updated_vertices = self.g.vertices.union(new_nodes_to_add)
            # self.g = GraphFrame(updated_vertices, self.g.edges)

        # new_nodes_u = theDataframe.select(F.col("nodeU").alias("id"))
        # new_nodes_v = theDataframe.select(F.col("nodeV").alias("id"))
        # new_nodes = new_nodes_u.union(new_nodes_v).withColumn("c_coms", F.array()).distinct()


        # if not self.g:
        #     self.g: GraphFrame = GraphFrame(new_nodes, new_edges)
        # else:
        #     # Filter new vertices
        #     existing_vertex_ids = self.g.vertices.select("id")
        #     new_nodes_filtered = new_nodes.join(existing_vertex_ids, on="id", how="left_anti")
        #
        #     # Filter new edges
        #     existing_edges = self.g.edges.select("src", "dst")
        #     new_edges_filtered = new_edges.join(
        #         existing_edges, on=["src", "dst"], how="left_anti"
        #     )
        #     # Update vertices and edges
        #     updated_vertices = self.g.vertices.unionByName(new_nodes_filtered).distinct()
        #     updated_edges = self.g.edges.unionByName(new_edges_filtered)
        #
        #     self.g = GraphFrame(updated_vertices, updated_edges)


from pyspark.sql import functions as F
from pyspark.sql.streaming import GroupState
import rocksdb  # Python RocksDB library


class TILES:
    def __init__(self, db_path):
        self.db_path = db_path
        self.db = rocksdb.DB(db_path, rocksdb.Options(create_if_missing=True))

    def get_state(self, key):
        """Fetch state from RocksDB."""
        try:
            value = self.db.get(key.encode())
            if value:
                return value.decode()
            else:
                return None
        except KeyError:
            return None

    def update_state(self, key, value):
        """Update state in RocksDB."""
        self.db.put(key.encode(), value.encode())

    def process_vertices(self, batch_df):
        """Process vertices and save to RocksDB."""
        vertices = batch_df.select("id").distinct()
        # Retrieve the state from RocksDB
        existing_state = self.get_state("vertices") or []
        # Merge with new vertices
        updated_state = list(set(existing_state + vertices.collect()))
        self.update_state("vertices", updated_state)

        return updated_state

    def process_edges(self, batch_df):
        """Process edges and save to RocksDB."""
        edges = batch_df.select("src", "dst").distinct()
        # Retrieve the state from RocksDB
        existing_state = self.get_state("edges") or []
        # Merge with new edges
        updated_state = list(set(existing_state + edges.collect()))
        self.update_state("edges", updated_state)

        return updated_state

    def execute(self, batch_df, batch_id):
        # Filter valid edges
        filtered_batch_df = batch_df.filter(F.col("nodeU") != F.col("nodeV"))

        # Create new vertices
        new_vertices = (
            filtered_batch_df
            .select(F.col("nodeU").alias("id"))
            .union(filtered_batch_df.select(F.col("nodeV").alias("id")))
            .distinct()
        )

        # Create new edges
        new_edges = filtered_batch_df.select(
            F.col("nodeU").alias("src"),
            F.col("nodeV").alias("dst"),
            F.col("timestamp"),
            F.col("tags"),
            F.lit(1).alias("weight")
        )

        # Process vertices and edges
        updated_vertices = self.process_vertices(new_vertices)
        updated_edges = self.process_edges(new_edges)

        # Debugging
        print(f"Batch ID: {batch_id}")
        print("Updated vertices:")
        print(updated_vertices)
        print("Updated edges:")
        print(updated_edges)

        # def initialize_views(self):
        #     """Initialize empty temporary views for vertices and edges if not already created."""
        #     if not self.spark.catalog.tableExists(self.vertices_view_name):
        #         empty_vertices_df = self.spark.createDataFrame([], self.vertices_schema)
        #         empty_vertices_df.createOrReplaceTempView(self.vertices_view_name)
        #
        #     if not self.spark.catalog.tableExists(self.edges_view_name):
        #         empty_edges_df = self.spark.createDataFrame([], self.edges_schema)
        #         empty_edges_df.createOrReplaceTempView(self.edges_view_name)
        #
        # def save_state(self, updated_vertices, updated_edges):
        #     """Update the temporary views with the new state of vertices and edges."""
        #     updated_vertices.createOrReplaceTempView(self.vertices_view_name)
        #     updated_edges.createOrReplaceTempView(self.edges_view_name)
        #
        # def load_state(self):
        #     """Load the current state of vertices and edges from temporary views."""
        #     vertices = self.spark.sql(f"SELECT * FROM {self.vertices_view_name}")
        #     edges = self.spark.sql(f"SELECT * FROM {self.edges_view_name}")
        #     return vertices, edges

        # def get_vertices(self):
        #     """Get the current state of vertices."""
        #     return self.spark.sql(f"SELECT * FROM {self.vertices_view_name}")
        #



        # def get_edges(self):
        #     """Get the current state of edges."""
        #     return self.spark.sql(f"SELECT * FROM {self.edges_view_name}")

        # self.vertices_view_name = "vertices_view"
        # self.edges_view_name = "edges_view"
        #
        # # Initialize schemas for vertices and edges
        # self.vertices_schema = StructType([
        #     StructField("id", StringType(), True),
        #     StructField("c_coms", ArrayType(StringType()), True)
        # ])
        # self.edges_schema = StructType([
        #     StructField("src", StringType(), True),
        #     StructField("dst", StringType(), True),
        #     StructField("timestamp", IntegerType(), True),
        #     StructField("tags", ArrayType(StringType()), True),
        #     StructField("weight", IntegerType(), True)
        # ])
        # self.vertices_view = "vertices_view"
        # self.edges_view = "edges_view"

        # Initialize empty vertices and edges
        # self.spark.sql(f"CREATE OR REPLACE TEMP VIEW {self.vertices_view} AS SELECT * FROM (SELECT NULL AS id, ARRAY() AS c_coms) WHERE false")
        # self.spark.sql(f"CREATE OR REPLACE TEMP VIEW {self.edges_view} AS SELECT * FROM (SELECT NULL AS src, NULL AS dst, NULL AS timestamp, ARRAY() AS tags, NULL AS weight) WHERE false")


        # current_vertices = self.spark.sql(f"SELECT * FROM {self.vertices_view}")
        # current_edges = self.spark.sql(f"SELECT * FROM {self.edges_view}")

        # updated_vertices.createOrReplaceTempView(self.vertices_view)
        # updated_edges.createOrReplaceTempView(self.edges_view)


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
        # updated_community_tags = communityTagsDf.alias("old").join(
        #     new_tags.alias("new"),
        #     F.col("old.cid") == F.col("new.cid"),
        #     "outer"
        # ).select(
        #     F.coalesce(F.col("new.cid"), F.col("old.cid")).alias("cid"),
        #     F.array_union(F.col("old.tags"), F.col("new.tags")).alias("tags")
            # ,F.when(
            #     F.col("old.tags").isNotNull() & F.col("new.tags").isNotNull(),
            #     F.array_union(F.col("old.tags"), F.col("new.tags"))
            # )
            # .when(F.col("old.tags").isNotNull(), F.col("old.tags"))
            # .otherwise(F.col("new.tags"))
            # .alias("tags")
        # )
        # updated_community_tags = communityTagsDf.join(
        #     new_tags,
        #     F.col("communityTagsDf.cid") == F.col("new_tags.cid"),
        #     "outer"
        # ).select(
        #     F.coalesce(F.col("new_tags.cid"), F.col("communityTagsDf.cid")).alias("cid"),
        #     F.coalesce(F.col("new_tags.tags"), F.col("communityTagsDf.tags")).alias("tags")
        # )

# community_nodes = new_community_edges.select(
#     F.explode(F.array("node_u", "node_v")).alias("id"),
#     F.col("new_community_id")
# ).distinct().select(
#     F.col("id").cast("string"),
#     F.col("new_community_id").cast("string")
# )


    #TODO CHECK THIS IF NEEDED
    # when saving into parquet, due to action, the following sum results to wrong weight calculation
    # see if i can just use normalized_new_edges_renamed and normalized_all_edges_renamed only.
    # preexisting_edges_updates = normalized_new_edges_renamed.join(
    #     normalized_all_edges_renamed,
    #     on=["src", "dst"],
    #     how="left" if normalized_all_edges_renamed.isEmpty() else "inner"  # Only keep edges that exist in both DataFrames or else the new_edges (first batch)
    # ).groupBy("src", "dst").agg(
    #     F.sum("new_weight").alias("total_weight_increment"),  # Sum up the weights of the new occurrences
    #     F.max(F.struct("new_timestamp", "new_tags")).alias("latest_entry")  # Get the latest entry (timestamp and tags)
    # ).select(
    #     "src", "dst",  "total_weight_increment",
    #     F.col("latest_entry.new_timestamp").alias("latest_timestamp"),
    #     F.col("latest_entry.new_tags").alias("latest_tags")
    # )


        # updated_preexisting_edges = normalized_all_edges.join(
        #     normalized_new_edges.withColumnRenamed("weight", "new_weight")
        #     .withColumnRenamed("timestamp", "new_timestamp")
        #     .withColumnRenamed("tags", "new_tags"),
        #     on=["src", "dst"],
        #     how="left"
        # ).withColumn("weight", F.coalesce(F.col("weight"), F.lit(0)) + F.coalesce(F.col("new_weight"), F.lit(0))
        # ).withColumn("tags", F.coalesce(F.col("new_tags"), F.col("tags"))
        # ).withColumn("timestamp", F.coalesce(F.col("new_timestamp"), F.col("timestamp"))
        # ).drop("new_weight", "new_timestamp", "new_tags")

        # # Step 2: Update preexisting edges
        # updated_preexisting_edges = (preexisting_edges_updates.join(
        #     normalized_all_edges,
        #     on=["src", "dst"],
        #     how="left"
        # ).withColumn("weight", F.coalesce(F.col("weight"), F.lit(0)) + F.coalesce(F.col("total_weight_increment"), F.lit(0))  # Handle NULL weights
        # ).withColumn("tags", F.coalesce(F.col("latest_tags"), F.col("tags"))  # Replace tags with the latest ones
        # ).withColumn("timestamp", F.coalesce(F.col("latest_timestamp"), F.col("timestamp"))  # Update the timestamp if needed
        # ).drop("total_weight_increment", "latest_timestamp", "latest_tags"))

# check these files only 2 communities detected somethings very wrong
# communitiesDf1 = loadState(self=self, pathToload=self.communities_path, schemaToCreate=self.communities_schema)
# communityTagsDf1 = loadState(self=self, pathToload=self.communityTags_path,
#                             schemaToCreate=self.communityTags_schema)
# updated_community_tags1, updated_communities1, updated_vertices1 = (add_to_community_streaming(self, all_vertices, shared_com_not_in_common_neigh_community, communitiesDf1, communityTagsDf1))
# print("Following 3 DFs after shared_com_not_in_common_neigh_community was added")
# printTrace("updated_community_tags:", updated_community_tags1)
# printTrace("updated_communities:", updated_communities1)
# printTrace("updated_vertices:", updated_vertices1)
#
# saveState(updated_community_tags1, self.communityTags_path)
# saveState(updated_communities1, self.communities_path)
# saveState(updated_vertices1, self.vertices_path)

        # printTrace("new_community_edges in if: ", new_community_edges)
        # communitiesDf2 = loadState(self=self, pathToload=self.communities_path, schemaToCreate=self.communities_schema)
        # communityTagsDf2 = loadState(self=self, pathToload=self.communityTags_path,
        #                             schemaToCreate=self.communityTags_schema)

        # updated_community_tags2, updated_communities2, updated_vertices2 = (add_to_community_streaming(self, all_vertices, new_community_edges, communitiesDf2, communityTagsDf2))

        # print("Following 3 DFs after new_community_edges was added")
        # printTrace("updated_community_tags:", updated_community_tags2)
        # printTrace("updated_communities:", updated_communities2)
        # printTrace("updated_vertices:", updated_vertices2)
        #
        # saveState(updated_community_tags2, self.communityTags_path)
        # saveState(updated_communities2, self.communities_path)
        # saveState(updated_vertices2, self.vertices_path)

    # communitiesDf3 = loadState(self=self, pathToload=self.communities_path, schemaToCreate=self.communities_schema)
    # communityTagsDf3 = loadState(self=self, pathToload=self.communityTags_path,
    #                             schemaToCreate=self.communityTags_schema)

    # updated_community_tags3, updated_communities3, updated_vertices3 = (add_to_community_streaming(self, all_vertices, only_u_coms_with_neigh_coms, communitiesDf3, communityTagsDf3))


    # print("Following 3 DFs after only_u_coms_with_neigh_coms was added")
    # printTrace("updated_community_tags:", updated_community_tags3)
    # printTrace("updated_communities:", updated_communities3)
    # printTrace("updated_vertices:", updated_vertices3)
    #
    # saveState(updated_community_tags3, self.communityTags_path)
    # saveState(updated_communities3, self.communities_path)
    # saveState(updated_vertices3, self.vertices_path)

        # should add the shared coms id to the common neighbor of above df

    # join exploded_only_u_communities with exploded_shared_coms_communities - add the u node
    # join exploded_only_v_communities with exploded_shared_coms_communities - add the v node

    # perhaps add to communities inside this method instead of returning dfs and passing them into different method
    # if added inside this method i have to keep updated the state of required dfs


# check only_u and only_v columns of both dfs something's sus there
# need to add also only_v-related df
# check if i ADD the correct edges, for only_u i should add the common_neighbors community it to V edge !! and vice versa for only_v -> u

# something is wrong with saving the comTags and communities


# communitiesDf4 = loadState(self=self, pathToload=self.communities_path, schemaToCreate=self.communities_schema)
    # communityTagsDf4 = loadState(self=self, pathToload=self.communityTags_path,
    #                             schemaToCreate=self.communityTags_schema)

    # updated_community_tags4, updated_communities4, updated_vertices4 = (add_to_community_streaming(self, all_vertices, only_v_coms_with_neigh_coms, communitiesDf4, communityTagsDf4))

    # print("Following 3 DFs after only_v_coms_with_neigh_coms was added")
    # printTrace("updated_community_tags:", updated_community_tags4)
    # printTrace("updated_communities:", updated_communities4)
    # printTrace("updated_vertices:", updated_vertices4)
    #
    # saveState(updated_community_tags4, self.communityTags_path)
    # saveState(updated_communities4, self.communities_path)
    # saveState(updated_vertices4, self.vertices_path)



