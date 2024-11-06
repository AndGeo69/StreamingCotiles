import datetime
import gzip
import os
import sys
import time

import networkx as nx

if sys.version_info > (2, 7):
    from io import StringIO
    from queue import PriorityQueue
else:
    from cStringIO import StringIO
    from Queue import PriorityQueue


__author__ = "Giulio Rossetti"
__contact__ = "giulio.rossetti@gmail.com"
__website__ = "about.giuliorossetti.net"
__license__ = "BSD"

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


class TILES:
    def __init__(self, stream=None, g=nx.Graph(), ttl=float('inf'), obs=7, path="", start=None, end=None):
        """
        Constructor
        :param g: networkx graph
        :param ttl: edge time to live (days)
        :param obs: observation window (days)
        :param path: Path where to generate the results and find the edge file
        :param start: starting date
        :param end: ending date
        """
        self.path = path
        self.ttl = ttl
        self.cid = 0
        self.actual_slice = 0
        self.g = g
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

    def execute(self, batch_df: DataFrame, batch_id):
        """
        Execute TILES algorithm on streaming data using foreachBatch.
        """
        self.status.write(u"Started! (%s) \n\n" % str(time.asctime(time.localtime(time.time()))))
        self.status.flush()

        # Priority queue for TTL handling
        qr = PriorityQueue()
        self.actual_time = None
        self.last_break = None

        # Process each micro-batch of streaming data
        # Initialize actual_time and last_break if not already set
        if self.actual_time is None and self.last_break is None:
            first_row = batch_df.first()
            if first_row:
                self.actual_time = datetime.datetime.fromtimestamp(float(first_row['timestamp']))
                self.last_break = self.actual_time

        # Apply transformations to process each row in the micro-batch
        self.added += 1
        e = {}

        dfNodeU = (batch_df.withColumn("nodeU", col("nodeU").cast("int"))
                   .withColumn("nodeV", col("nodeV").cast("int")))

        u = int(batch_df.select("nodeU"))
        v = int(batch_df.select("nodeV"))
        dt = datetime.datetime.fromtimestamp(float(batch_df['timestamp']))
        tags = batch_df['tags']
        e['weight'] = 1
        e["u"] = u
        e["v"] = v
        e["t"] = tags

        # Observations
        gap = dt - self.last_break
        dif = gap.days

        # Create a new slice if the observation window has passed
        if dif >= self.obs:
            self.last_break = dt
            print("New slice. Starting Day: %s" % dt)
            self.status.write(u"Saving Slice %s: Starting %s ending %s - (%s)\n" %
                              (self.actual_slice, self.actual_time, dt,
                               str(time.asctime(time.localtime(time.time())))))

            self.status.write(u"Edge Added: %d\tEdge removed: %d\n" % (self.added, self.removed))
            self.added = 0
            self.removed = 0

            self.actual_time = dt
            self.status.flush()

            self.splits = gzip.open("%s/%s/Draft3/splitting-%d.gz" % (self.base, self.path, self.actual_slice),
                                    "wt", 3)
            self.splits.write(self.spl.getvalue())
            self.splits.flush()
            self.splits.close()
            self.spl = StringIO()

            # self.print_communities()
            self.status.write(
                u"\nStarted Slice %s (%s)\n" % (self.actual_slice, str(datetime.datetime.now().time())))

        if u == v:
            return

        # Edge removal based on TTL
        if self.ttl != float('inf'):
            qr.put((dt, (int(e['u']), int(e['v']), int(e['weight']), e['t'])))
            # self.remove(dt, qr)

        # Add nodes if they don’t exist in the graph
        if not self.g.has_node(u):
            self.g.add_node(u)
            self.g.nodes[u]['c_coms'] = {}  # central

        if not self.g.has_node(v):
            self.g.add_node(v)
            self.g.nodes[v]['c_coms'] = {}

        # Update or add the edge in the graph
        if self.g.has_edge(u, v):
            w = self.g.adj[u][v]["weight"]
            self.g.adj[u][v]["weight"] = w + e['weight']
            self.g.adj[u][v]["t"] = e['t']
        else:
            self.g.add_edge(u, v)
            self.g.adj[u][v]["weight"] = e['weight']
            self.g.adj[u][v]["t"] = e['t']

        # Neighbor analysis
        u_n = list(self.g.neighbors(u))
        v_n = list(self.g.neighbors(v))

        # Evolution analysis
        if len(u_n) > 1 and len(v_n) > 1:
            common_neighbors = set(u_n) & set(v_n)
            # self.common_neighbors_analysis(u, v, common_neighbors, e['t'])

        # Final slice update
        self.status.write(u"Slice %s: Starting %s ending %s - (%s)\n" %
                          (self.actual_slice, self.actual_time, self.actual_time,
                           str(time.asctime(time.localtime(time.time())))))
        self.status.write(u"Edge Added: %d\tEdge removed: %d\n" % (self.added, self.removed))
        self.added = 0
        self.removed = 0

        # self.print_communities()
        self.status.write(u"Finished! (%s)" % str(time.asctime(time.localtime(time.time()))))
        self.status.flush()
        self.status.close()


