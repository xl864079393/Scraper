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

def load_json_file(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        data = json.load(file)
    return data

def open_file(dir_path):
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

def close_file(file_handlers):
    """
    关闭所有文件句柄。
    :param file_handlers: list，包含所有文件句柄的列表
    """
    for file_handler in file_handlers:
        file_handler.close()


def search_for_test():
    # data = load_json_file(file_path)
    with open("index_bookkeeping.pkl", 'rb') as bf:
        term_positions = pickle.load(bf)
    with gzip.open("link_graph.pkl", 'rb') as bf:
        link_graph = pickle.load(bf)
    page_rank_scores = nx.pagerank(link_graph)
    hits_scores = nx.hits(link_graph)
    file_handlers = open_file("batches")
    docid_dict = load_json_file("docid_dict.json")
    len_docid = len(docid_dict)
    ps = PorterStemmer()
    bitmaps = {}
    while True:
        # get the term to search
        inputs = input("Enter the term to search: ")
        start_time = time.time()
        inputs = inputs.lower()
        if inputs == "/exit":
            break

        #token the input
        terms = inputs.split(" ")

        # delete any non-alphanumeric and non-space characters
        terms = [re.sub(r'[^a-zA-Z0-9\s]', '', term) for term in terms]
        trigrams = [tuple(terms[i:i + 3]) for i in range(len(terms) - 2)]
        terms = [ps.stem(term) for term in terms]
        all_terms = terms + [' '.join(trigram) for trigram in trigrams]

        terms_postings = {}
        for term in all_terms:
            if term not in term_positions:
                continue
            terms_postings.update(index_batch.load_inverted_index_for_term(term, "bookkeeping.pkl", term_positions[term], file_handlers))

        bitmap.preprocess_bitmap(bitmaps, terms_postings)

        # for every term in term_dict, output the intersection of their doc_numbers using bitmap
        bitmap_list = [bitmaps[term] for term in all_terms if term in bitmaps]
        intersection_bitmap = reduce(lambda x, y: x & y, bitmap_list if bitmap_list else [pyroaring.BitMap()])

        #
        # # 计算 TF-IDF 分数并排序
        scores = scoresComputation.compute_tf_idf(len_docid, terms_postings, intersection_bitmap)
        # # 计算 HITS 和 PageRank 分数
        scoresComputation.compute_HITS_PR(intersection_bitmap, page_rank_scores, hits_scores[0], scores)
        # # 计算 Proximity Position 分
        scoresComputation.compute_proximity_position(intersection_bitmap, terms_postings,5, scores)
        # # 计算anchor分数
        scoresComputation.compute_anchor(intersection_bitmap, terms_postings, scores)
        ranked_docs = sorted(scores.items(), key = lambda x: x[1], reverse = True)
        end_time = time.time()
        print(f"Time taken: {end_time - start_time}")

        #print top 10 documents
        for doc_id, score in ranked_docs[:10]:
            print(f"Document number: {doc_id}")
            print(f"File path: {docid_dict[str(doc_id)]}")
            print(f"Score: {score}")
            print()

        terms_postings.clear()
    close_file(file_handlers)


if __name__ == "__main__":
    search_for_test()