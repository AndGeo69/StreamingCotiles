"""
    Created on 11/feb/2015
    @author: Giulio Rossetti
"""
import networkx as nx
import gzip
import datetime
import time
from future.utils import iteritems
import os
from collections import Counter

import sys
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

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType


class TILES:
    """
    TILES: Algorithm for evolutionary community discovery with streaming support
    """

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

    def execute(self):
        """
            Execute TILES algorithm
        """
        self.status.write(u"Started! (%s) \n\n" % str(time.asctime(time.localtime(time.time()))))
        self.status.flush()

        qr = PriorityQueue()

        with open(self.filename, 'r') as f:
            first_line = f.readline()

        actual_time = datetime.datetime.fromtimestamp(float(first_line.split("\t")[2]))
        last_break = actual_time
        f.close()

        count = 0

        #################################################
        #                   Main Cycle                  #
        #################################################

        f = open(self.filename)
        for l in f:
            l = l.split("\t")
            self.added += 1
            e = {}
            u = int(l[0])
            v = int(l[1])
            dt = datetime.datetime.fromtimestamp(float(l[2]))
            tags = l[3]
            u["t"] = l[3]
            v["t"] = l[3]
            #print(tags)

            e['weight'] = 1
            e["u"] = l[0]
            e["v"] = l[1]
            e["t"] = l[3]
            #print(e["t"])
            # month = dt.month

            #############################################
            #               Observations                #
            #############################################

            gap = dt - last_break
            dif = gap.days

            if dif >= self.obs:
                last_break = dt

                print("New slice. Starting Day: %s" % dt)
                self.status.write(u"Saving Slice %s: Starting %s ending %s - (%s)\n" %
                                  (self.actual_slice, actual_time, dt,
                                   str(time.asctime(time.localtime(time.time())))))

                self.status.write(u"Edge Added: %d\tEdge removed: %d\n" % (self.added, self.removed))
                self.added = 0
                self.removed = 0

                actual_time = dt
                self.status.flush()

                self.splits = gzip.open("%s/%s/Draft3/splitting-%d.gz" % (self.base, self.path, self.actual_slice), "wt", 3)
                self.splits.write(self.spl.getvalue())
                self.splits.flush()
                self.splits.close()
                self.spl = StringIO()

                self.print_communities()
                self.status.write(
                    u"\nStarted Slice %s (%s)\n" % (self.actual_slice, str(datetime.datetime.now().time())))

            if u == v:
                continue

            # Check if edge removal is required
            if self.ttl != float('inf'):
                qr.put((dt, (int(e['u']), int(e['v']), int(e['weight']), e['t'])))
                #print("into queue")
                self.remove(dt, qr)

            if not self.g.has_node(u):
                self.g.add_node(u)
                self.g.nodes[u]['c_coms'] = {}  # central

            if not self.g.has_node(v):
                self.g.add_node(v)
                self.g.nodes[v]['c_coms'] = {}

            if self.g.has_edge(u, v):
                w = self.g.adj[u][v]["weight"]
                self.g.adj[u][v]["weight"] = w + e['weight']
                self.g.adj[u][v]["t"] = e['t']
                continue
            else:
                self.g.add_edge(u, v)
                self.g.adj[u][v]["weight"] = e['weight']
                self.g.adj[u][v]["t"] = e['t']

            u_n = list(self.g.neighbors(u))
            #self.representation.write("List of neighbours of %s: %s\n"%(u, u_n))
            v_n = list(self.g.neighbors(v))
            #self.representation.write("List of neighbours of %s: %s\n" %(v, v_n))

            #############################################
            #               Evolution                   #
            #############################################

            # new community of peripheral nodes (new nodes)
            if len(u_n) > 1 and len(v_n) > 1:
                common_neighbors = set(u_n) & set(v_n)
                self.common_neighbors_analysis(u, v, common_neighbors,e['t'])

            count += 1

        #  Last writing
        self.status.write(u"Slice %s: Starting %s ending %s - (%s)\n" %
                          (self.actual_slice, actual_time, actual_time,
                           str(time.asctime(time.localtime(time.time())))))
        self.status.write(u"Edge Added: %d\tEdge removed: %d\n" % (self.added, self.removed))
        self.added = 0
        self.removed = 0

        self.print_communities()
        self.status.write(u"Finished! (%s)" % str(time.asctime(time.localtime(time.time()))))
        self.status.flush()
        self.status.close()

    @property
    def new_community_id(self):
        """
            Return a new community identifier
            :return: new community id
        """
        self.cid += 1
        self.communities[self.cid] = {}
        return self.cid

    def remove(self, actual_time, qr):
        """
            Edge removal procedure
            :param actual_time: timestamp of the last inserted edge
            :param qr: Priority Queue containing the edges to be removed ordered by their timestamps
        """

        coms_to_change = {}
        at = actual_time

        # main cycle on the removal queue
        if not qr.empty():

            t = qr.get()
            timestamp = t[0]
            e = (t[1][0],  t[1][1], t[1][2], t[1][3])

            delta = at - timestamp
            displacement = delta.days

            if displacement < self.ttl:
                qr.put((timestamp, t[1]))

            else:
                while self.ttl <= displacement:

                    self.removed += 1
                    u = int(e[0])
                    v = int(e[1])
                    u["t"]=e[3]
                    t = e[3]
                    if self.g.has_edge(u, v):

                        w = self.g.adj[u][v]["weight"]

                        # decreasing link weight if greater than one
                        # (multiple occurrence of the edge: remove only the oldest)
                        if w > 1:
                            self.g.adj[u][v]["weight"] = w - 1
                            e = (u,  v, w-1, t)
                            #print(e)
                            qr.put((at, e))

                        else:
                            # u and v shared communities
                            if len(list(self.g.neighbors(u))) > 1 and len(list(self.g.neighbors(v))) > 1:
                                coms = set(self.g.nodes[u]['c_coms'].keys()) & set(self.g.nodes[v]['c_coms'].keys())

                                for c in coms:
                                    if c not in coms_to_change:
                                        cn = set(self.g.neighbors(u)) & set(self.g.neighbors(v))
                                        coms_to_change[c] = [u, v]
                                        coms_to_change[c].extend(list(cn))
                                    else:
                                        cn = set(self.g.neighbors(u)) & set(self.g.neighbors(v))
                                        coms_to_change[c].extend(list(cn))
                                        coms_to_change[c].extend([u, v])
                                        ctc = set(coms_to_change[c])
                                        coms_to_change[c] = list(ctc)
                            else:
                                if len(list(self.g.neighbors(u))) < 2:
                                    coms_u = [x for x in self.g.nodes[u]['c_coms'].keys()]
                                    for cid in coms_u:
                                        self.remove_from_community(u, cid)

                                if len(list(self.g.neighbors(v))) < 2:
                                    coms_v = [x for x in self.g.nodes[v]['c_coms'].keys()]
                                    for cid in coms_v:
                                        self.remove_from_community(v, cid)

                            self.g.remove_edge(u, v)

                    if not qr.empty():

                        t = qr.get()

                        timestamp = t[0]
                        delta = at - timestamp
                        displacement = delta.days

                        e = t[1]

        # update of shared communities
        self.update_shared_coms(coms_to_change, t)

    def update_shared_coms(self, coms_to_change, tags):
        # update of shared communities
        for c in coms_to_change:
            if c not in self.communities:
                continue

            c_nodes = self.communities[c].keys()

            if len(c_nodes) > 3:

                sub_c = self.g.subgraph(c_nodes)
                c_components = nx.number_connected_components(sub_c)

                # unbroken community
                if c_components == 1:
                    to_mod = sub_c.subgraph(coms_to_change[c])
                    self.modify_after_removal(to_mod, c, tags)

                # broken community: bigger one maintains the id, the others obtain a new one
                else:
                    new_ids = []

                    first = True
                    components = nx.connected_components(sub_c)
                    for com in components:
                        if first:
                            if len(com) < 3:
                                self.destroy_community(c)
                            else:
                                to_mod = list(set(com) & set(coms_to_change[c]))
                                sub_c = self.g.subgraph(to_mod)
                                self.modify_after_removal(sub_c, c, tags)
                            first = False

                        else:
                            if len(com) > 3:
                                # update the memberships: remove the old ones and add the new one
                                to_mod = list(set(com) & set(coms_to_change[c]))
                                sub_c = self.g.subgraph(to_mod)

                                central = self.centrality_test(sub_c).keys()
                                if len(central) >= 3:
                                    actual_id = self.new_community_id
                                    new_ids.append(actual_id)
                                    for n in central:
                                        self.add_to_community(n, actual_id, tags)
                                       # self.representation.write(
                                        #    "------------------------------Add %s to community %s-----------------------------------" % (
                                         #   n, actual_id))


                    # splits
                    if len(new_ids) > 0 and self.actual_slice > 0:
                        self.spl.write(u"%s\t%s\n" % (c, str(new_ids)))
            else:
                self.destroy_community(c)

    def modify_after_removal(self, sub_c, c, tags):
        """
            Maintain the clustering coefficient invariant after the edge removal phase
            :param sub_c: sub-community to evaluate
            :param c: community id
        """
        central = self.centrality_test(sub_c).keys()

        # in case of previous splits, update for the actual nodes
        remove_node = set(self.communities[c].keys()) - set(sub_c.nodes())

        for rm in remove_node:
            self.remove_from_community(rm, c, tags)

        if len(central) < 3:
            self.destroy_community(c)
        else:
            not_central = set(sub_c.nodes()) - set(central)
            for n in not_central:
                self.remove_from_community(n, c, tags)

    def common_neighbors_analysis(self, u, v, common_neighbors, tags):
        """
            General case in which both the nodes are already present in the net.
            :param u: a node
            :param v: a node
            :param common_neighbors: common neighbors of the two nodes
        """


        # no shared neighbors
        if len(common_neighbors) < 1:
            return

        else:


            shared_coms = set(self.g.nodes[v]['c_coms'].keys()) & set(self.g.nodes[u]['c_coms'].keys())
            #self.representation.write("...............................Shared communities of %s and %s are: %s\n"%(u, v, shared_coms))
            only_u = set(self.g.nodes[u]['c_coms'].keys()) - set(self.g.nodes[v]['c_coms'].keys())
            #self.representation.write("...............................Communities where only %s belongs:  %s\n"%(u, only_u))
            only_v = set(self.g.nodes[v]['c_coms'].keys()) - set(self.g.nodes[u]['c_coms'].keys())
            #self.representation.write("...............................Communities where only %s belongs:  %s\n"%(v, only_v))
            #print(" ")
            # community propagation: a community is propagated iff at least two of [u, v, z] are central
            propagated = False

            for z in common_neighbors:
                for c in self.g.nodes[z]['c_coms'].keys():
                    #if c in only_v and len(only_v) < 5:
                    if c in only_v:
                        self.add_to_community(u, c, tags )
                        #self.representation.write("------------------------------Add %s to community %s-----------------------------------\n"%(u,c))
                        propagated = True

                    #if c in only_u and len(only_u)<5:
                    if c in only_u:
                        self.add_to_community(v, c, tags )
                        #self.representation.write("------------------------------Add %s to community %s-----------------------------------\n" % (
                          #  v, c))
                        propagated = True

                for c in shared_coms:
                    if c not in self.g.nodes[z]['c_coms']:
                        self.add_to_community(z, c, tags )
                        propagated = True

            else:
                if not propagated:
                    # new community
                    #self.representation.write("Not propagated\n")
                    actual_cid = self.new_community_id
                    self.add_to_community(u, actual_cid, tags )
                    #self.representation.write("------------------------------Add %s to community %s-----------------------------------\n" % (
    #                        u, actual_cid))
                    self.add_to_community(v, actual_cid, tags )
                    #self.representation.write("------------------------------Add %s to community %s-----------------------------------\n" % (
                       # v, actual_cid))

                    for z in common_neighbors:
                        self.add_to_community(z, actual_cid, tags )
                        #self.representation.write(
                         #   "------------------------------Add %s to community %s-----------------------------------\n" % (
                          #      z, actual_cid))

    def print_communities(self):
        """
            Print the actual communities
        """

        out_file_coms = gzip.open("%s/%s/strong-communities-%d.gz" % (self.base, self.path, self.actual_slice), "wt", 3)
        com_string = StringIO()

        nodes_to_coms = {}
        merge = {}
        coms_to_remove = []
        drop_c = []


        self.status.write(u"Writing Communities (%s)\n" % str(time.asctime(time.localtime(time.time()))))
        self.status.flush()
        for idc, comk in iteritems(self.communities):

            com = comk.keys()

            if self.communities[idc] is not None:
                if len(com) > 2:
                    key = tuple(sorted(com))

                    # Collision check and merge index build (maintaining the lowest id)
                    if key not in nodes_to_coms:
                        nodes_to_coms[key] = idc
                    else:
                        old_id = nodes_to_coms[key]
                        drop = idc
                        if idc < old_id:
                            drop = old_id
                            nodes_to_coms[key] = idc

                        # merged to remove
                        coms_to_remove.append(drop)
                        if not nodes_to_coms[key] in merge:
                            merge[nodes_to_coms[key]] = [idc]
                        else:
                            merge[nodes_to_coms[key]].append(idc)
                else:
                    drop_c.append(idc)
            else:
                drop_c.append(idc)

        write_count = 0
        for k, idk in iteritems(nodes_to_coms):
            write_count += 1
            if write_count % 50000 == 0:
                out_file_coms.write(com_string.getvalue())
                out_file_coms.flush()
                com_string = StringIO()
                write_count = 0
            com_string.write(u"%d\t%s\t\t%s\t%s\t\t'%s\n" % (idk, str(list(k)), len(dict(Counter(self.CommunityTags[idk]))), len(self.CommunityTags[idk]),  dict(Counter(self.CommunityTags[idk]).most_common())))

        for dc in drop_c:
            self.destroy_community(dc)

        out_file_coms.write(com_string.getvalue())
        out_file_coms.flush()
        out_file_coms.close()

        # write the graph
        self.status.write(u"Writing actual graph status (%s)\n" % str(time.asctime(time.localtime(time.time()))))
        self.status.flush()
        out_file_graph = gzip.open("%s/%s/graph-%d.gz" % (self.base, self.path, self.actual_slice), "wt", 3)
        g_string = StringIO()
        for e in self.g.edges():
            g_string.write(u"%d\t%s\t%d\t%s\n" % (e[0], e[1], self.g.adj[e[0]][e[1]]['weight'], self.g.adj[e[0]][e[1]]['t']))

        out_file_graph.write(g_string.getvalue())
        out_file_graph.flush()
        out_file_graph.close()

        # Write merge status
        self.status.write(u"Writing merging file (%s)\n" % str(time.asctime(time.localtime(time.time()))))
        self.status.flush()
        out_file_merge = gzip.open("%s/%s/merging-%d.gz" % (self.base, self.path, self.actual_slice), "wt", 3)
        m_string = StringIO()
        for comid, c_val in iteritems(merge):
            # maintain minimum community after merge
            c_val.append(comid)
            k = min(c_val)
            c_val.remove(k)
            m_string.write(u"%d\t%s\n" % (k, str(c_val)))
        out_file_merge.write(m_string.getvalue())
        out_file_merge.flush()
        out_file_merge.close()

        # Community Cleaning
        m = 0
        for c in coms_to_remove:
            self.destroy_community(c)
            m += 1

        self.status.write(u"Merged communities: %d (%s)\n" % (m, str(time.asctime(time.localtime(time.time())))))

        self.actual_slice += 1
        self.status.write(u"Total Communities %d (%s)\n" % (len(self.communities.keys()),
                                                           str(time.asctime(time.localtime(time.time())))))
        self.status.flush()

    def destroy_community(self, cid):
        nodes = [x for x in self.communities[cid].keys()]
        #for n in nodes:
            #self.remove_from_community(n, cid, tg)
        self.communities.pop(cid, None)

    def add_to_community(self, node, cid, tag):

        flag = 1
        for x in tag.split(','):
            for tg in tag.split(','):
                self.CommunityTags[cid].append(tg)
            flag = 1
            # print(self.CommunityTags[cid])

        if flag == 1:
            self.g.nodes[node]['c_coms'][cid] = None
            if cid in self.communities:
                self.communities[cid][node] = None
            else:
                self.communities[cid] = {node: None}

    def remove_from_community(self, node, cid, tags):
        if cid in self.g.nodes[node]['c_coms']:
            self.g.nodes[node]['c_coms'].pop(cid, None)
            for tg in tags.split(','):
                if tg in self.CommunityTags[cid]:
                    self.CommunityTags[cid].remove(tg)
            if cid in self.communities and node in self.communities[cid]:
                self.communities[cid].pop(node, None)


    def centrality_test(self, subgraph):
        central = {}

        for u in subgraph.nodes():
            if u not in central:
                cflag = False
                neighbors_u = set(self.g.neighbors(u))
                if len(neighbors_u) > 1:
                    for v in neighbors_u:
                        if u > v:
                            if cflag:
                                break
                            else:
                                neighbors_v = set(self.g.neighbors(v))
                                cn = neighbors_v & neighbors_v
                                if len(cn) > 0:
                                    central[u] = None
                                    central[v] = None
                                    for n in cn:
                                        central[n] = None
                                    cflag = True
        return central
