import json
import os
from nltk.stem import PorterStemmer
import pyroaring
import re
import time
from functools import reduce
import math

def load_json_file(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        data = json.load(file)
    return data

def search_term(json_data, term):

    if term not in json_data:
        return f"The term '{term}' does not exist in the data."

    results = json_data[term]
    doc_and_tf = {result[0]: result[1] for result in results}
    return doc_and_tf

def create_bitmap(word, doc_ids, bitmaps):
    bitmap = pyroaring.BitMap(doc_ids)
    bitmaps[word] = bitmap

def preprocess_bitmap(bitmaps, data):
    for term, postings in data.items():
        doc_ids = [posting[0] for posting in postings]
        create_bitmap(term, doc_ids, bitmaps)

def compute_tf_idf(data, terms_dict, intersection_docs):
    N = len(data)  # 总文档数
    scores = {}

    for term in terms_dict:
        term_data = terms_dict[term] # 获取当前词的倒排索引信息
        df = len(term_data)  # 包含该词的文档数量
        idf = math.log(N / (df + 1))  # 防止除以 0
        print(f"Term: {term}, DF: {df}, IDF: {idf}")

        for doc_id in intersection_docs:
            tf = term_data[doc_id]  # 当前文档的词频
            print(f"Doc ID: {doc_id}, TF: {tf}")
            tf_idf = tf * idf  # 计算 TF-IDF
            scores[doc_id] = scores.get(doc_id, 0) + tf_idf  # 累加多个词的 TF-IDF

    return scores


def search():
    file_path = "raw_result.json"
    data = load_json_file(file_path)
    docid_dict = load_json_file("docid_dict.json")
    term_dict = {}
    ps = PorterStemmer()
    bitmap = {}
    preprocess_bitmap(bitmap, data)
    
    while True:
        # get the term to search
        inputs = input("Enter the term to search: ")
        inputs = inputs.lower()

        #token the input
        terms = inputs.split(" ")

        # delete any non-alphanumeric and non-space characters
        terms = [re.sub(r'[^a-zA-Z0-9\s]', '', term) for term in terms]
        terms = [ps.stem(term) for term in terms]

        # store the doc_numbers and term_frequency of each term in term_dict
        for term in terms:
            doc_and_tf = search_term(data, term)
            if isinstance(doc_and_tf, str):
                print(doc_and_tf)
            else:
                term_dict[term] = doc_and_tf


        # for every term in term_dict, output the intersection of their doc_numbers using bitmap
        terms_bitmap = [bitmap[term] for term in terms if term in bitmap]
        intersection_bitmap = reduce(lambda x, y: x & y, terms_bitmap if terms_bitmap else [pyroaring.BitMap()])

        # 计算 TF-IDF 分数并排序
        tf_idf_scores = compute_tf_idf(data, term_dict, intersection_bitmap)
        print(f"TF-IDF scores: {tf_idf_scores}")
        ranked_docs = sorted(tf_idf_scores.items(), key = lambda x: x[1], reverse = True)
        print(f"Ranked documents: {ranked_docs}")


        # get the file path of the intersection
        # for doc_number in intersection_bitmap:
        #     print(f"Document number: {doc_number}")
        #     print(f"File path: {docid_dict[str(doc_number)]}")
        #     print()
        # print(len(intersection))
        term_dict.clear()


if __name__ == "__main__":
    search()
