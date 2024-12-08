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