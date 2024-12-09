import json
from nltk.stem import PorterStemmer
import re
import time
from Util import bitmap, scoresComputation, index_batch
import pickle
import os
import pyroaring
from functools import reduce
import gzip
import networkx as nx

class search_engine:
    def __init__(self):
        with open("index_bookkeeping.pkl", 'rb') as bf:
            self.term_positions = pickle.load(bf)
        with gzip.open("link_graph.pkl", 'rb') as bf:
            self.link_graph = pickle.load(bf)
        self.page_rank_scores = nx.pagerank(self.link_graph)
        self.hits_scores = nx.hits(self.link_graph)
        self.file_handlers = self.open_file("batches")
        self.docid_dict = self.load_json_file("docid_dict.json")
        self.len_docid = len(self.docid_dict)
        self.ps = PorterStemmer()
        self.bitmaps = {}

    def load_json_file(self,file_path):
        with open(file_path, "r", encoding = "utf-8") as file:
            data = json.load(file)
        return data

    def open_file(self, dir_path):
        """
        打开指定目录下的所有 .json 文件，并返回包含文件句柄的列表。
        :param dir_path: str，目标目录路径
        :return: list，包含所有文件句柄的列表
        """
        file_handlers = []  # 用于存储文件句柄的列表
        for root, dirs, files in os.walk(dir_path):
            for file in files:
                if file.endswith(".pkl"):
                    # 使用绝对路径打开文件并保存文件句柄
                    file_handler = open(os.path.join(root, file), "rb")
                    file_handlers.append(file_handler)  # 将文件句柄加入列表
        return file_handlers

    def close_file(self, file_handlers):
        """
        关闭所有文件句柄。
        :param file_handlers: list，包含所有文件句柄的列表
        """
        for file_handler in file_handlers:
            file_handler.close()

    def search(self, query):
        inputs = query.lower()
        if inputs == "/exit":
            return None
        # token the input
        terms = inputs.split(" ")

        # delete any non-alphanumeric and non-space characters
        terms = [re.sub(r'[^a-zA-Z0-9\s]', '', term) for term in terms]
        trigrams = [tuple(terms[i:i + 3]) for i in range(len(terms) - 2)]
        terms = [self.ps.stem(term) for term in terms]
        all_terms = terms + [' '.join(trigram) for trigram in trigrams]

        terms_postings = {}
        for term in all_terms:
            if term not in self.term_positions:
                continue
            terms_postings.update(index_batch.load_inverted_index_for_term(term, "bookkeeping.pkl",
                                                                           self.term_positions[term],
                                                                           self.file_handlers))

        bitmap.preprocess_bitmap(self.bitmaps, terms_postings)

        # for every term in term_dict, output the intersection of their doc_numbers using bitmap
        bitmap_list = [self.bitmaps[term] for term in all_terms if term in self.bitmaps]
        intersection_bitmap = reduce(lambda x, y: x & y,
                                     bitmap_list if bitmap_list else [pyroaring.BitMap()])

        #
        # # 计算 TF-IDF 分数并排序
        scores = scoresComputation.compute_tf_idf(self.len_docid, terms_postings, intersection_bitmap)
        # # 计算 HITS 和 PageRank 分数
        scoresComputation.compute_HITS_PR(intersection_bitmap, self.page_rank_scores, self.hits_scores[0],
                                          scores)
        # # 计算 Proximity Position 分
        scoresComputation.compute_proximity_position(intersection_bitmap, terms_postings, 5, scores)
        scoresComputation.compute_anchor(intersection_bitmap, terms_postings, scores)
        ranked_docs = sorted(scores.items(), key = lambda x: x[1], reverse = True)

        terms_postings.clear()
        # for
        result = ranked_docs[:100]
        # for every entry in result, add corresponding doc url to the result
        for i in range(len(result)):
            result[i] = (result[i][0], self.docid_dict[str(result[i][0])],result[i][0])
        return result