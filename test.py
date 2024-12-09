import networkx as nx
import pickle
import numpy as np
import scipy as sp
import gzip



# with gzip.open("link_graph.pkl", 'rb') as bf:
#     link_graph = pickle.load(bf)
# # print(link_graph)
# # # 计算 PageRank
# pagerank_scores = nx.pagerank(link_graph)
# print(pagerank_scores)
# # #
# # # # 计算 HITS
# # hits_scores = nx.hits(link_graph)
# # print(hits_scores[0])