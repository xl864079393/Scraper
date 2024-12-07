import json
from nltk.stem import PorterStemmer
import pyroaring
import re
import time
from functools import reduce
from Util import bitmap, scoresComputation, index_batch

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


def search():
    file_path = "raw_result.json"
    # data = load_json_file(file_path)
    docid_dict = load_json_file("docid_dict.json")
    term_dict = {}
    ps = PorterStemmer()
    bitmaps = {}
    # bitmap.preprocess_bitmap(bitmaps, data)
    
    while True:
        # get the term to search
        inputs = input("Enter the term to search: ")
        inputs = inputs.lower()

        #token the input
        terms = inputs.split(" ")

        # delete any non-alphanumeric and non-space characters
        terms = [re.sub(r'[^a-zA-Z0-9\s]', '', term) for term in terms]
        trigrams = [tuple(terms[i:i + 3]) for i in range(len(terms) - 2)]
        terms = [ps.stem(term) for term in terms]
        all_terms = terms + [' '.join(trigram) for trigram in trigrams]

        result = {}
        for term in all_terms:
            result.update(index_batch.load_inverted_index_for_term(term, "bookkeeping_file.pkl"))
        print(result)

        #bitmap.preprocess_bitmap(bitmaps, result)


        # store the doc_numbers and term_frequency of each term in term_dict
        # for term in all_terms:
        #     doc_and_tf = search_term(data, term)
        #     if isinstance(doc_and_tf, str):
        #         print(doc_and_tf)
        #     else:
        #         term_dict[term] = doc_and_tf
        #
        #
        # # for every term in term_dict, output the intersection of their doc_numbers using bitmap
        # terms_bitmap = [bitmap[term] for term in all_terms if term in bitmap]
        # intersection_bitmap = reduce(lambda x, y: x & y, terms_bitmap if terms_bitmap else [pyroaring.BitMap()])
        #
        # # 计算 TF-IDF 分数并排序
        # tf_idf_scores = scoresComputation.compute_tf_idf(data, term_dict, intersection_bitmap)
        # print(f"TF-IDF scores: {tf_idf_scores}")
        # ranked_docs = sorted(tf_idf_scores.items(), key = lambda x: x[1], reverse = True)
        # print(f"Ranked documents: {ranked_docs}")


        # get the file path of the intersection
        # for doc_number in intersection_bitmap:
        #     print(f"Document number: {doc_number}")
        #     print(f"File path: {docid_dict[str(doc_number)]}")
        #     print()
        # print(len(intersection))
        term_dict.clear()


if __name__ == "__main__":
    search()
