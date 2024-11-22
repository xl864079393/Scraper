import json
import os
from nltk.stem import PorterStemmer
import pyroaring
import re
import time
from functools import reduce

def load_json_file(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        data = json.load(file)
    return data

def search_term(json_data, term):

    if term not in json_data:
        return f"The term '{term}' does not exist in the data."
    
    results = json_data[term]
    doc_numbers = [entry[0] for entry in results]
    return doc_numbers

def create_bitmap(word, doc_ids, bitmaps):
    # bitmap = bitarray(total_docs + 1)
    # bitmap.setall(0)
    #
    # for doc_id in doc_ids:
    #     bitmap[doc_id] = 1
    bitmap = pyroaring.BitMap(doc_ids)

    bitmaps[word] = bitmap

def preprocess_bitmap(bitmaps, data):
    for term, postings in data.items():
        doc_ids = [posting[0] for posting in postings]
        create_bitmap(term, doc_ids, bitmaps)


def search():

    file_path = "raw_result.json"
    data = load_json_file(file_path)
    docid_dict = load_json_file("docid_dict.json")
    term_dict = {}
    ps = PorterStemmer()
    bitmap = {}
    preprocess_bitmap(bitmap, data)
    
    while True:
        inputs = input("Enter the term to search: ")
        inputs = inputs.lower()
        #token the input
        terms = inputs.split(" ")
        # delete any non-alphanumeric and non-space characters
        terms = [re.sub(r'[^a-zA-Z0-9\s]', '', term) for term in terms]
        terms = [ps.stem(term) for term in terms]


        start_time = time.time()
        for term in terms:
            doc_numbers = search_term(data, term)
            if isinstance(doc_numbers, str):
                print(doc_numbers)
            else:
                term_dict[term] = doc_numbers

        # for every term in term_dict, output the intersection of their doc_numbers
        intersection = set(term_dict[terms[0]] if terms[0] in term_dict else [])
        for term in terms:
            if term in term_dict:
                intersection = intersection.intersection(term_dict[term])
        print(len(intersection))
        print("Method1 Time taken: ", time.time() - start_time)

        start_time = time.time()
        terms_bitmap = [bitmap[term] for term in terms if term in bitmap]
        intersection_bitmap = reduce(lambda x, y: x & y, terms_bitmap if terms_bitmap else [pyroaring.BitMap()])
        print(len(intersection_bitmap))
        print("Method2 Time taken: ", time.time() - start_time)

        # get the file path of the intersection
        # for doc_number in intersection:
        #     print(f"Document number: {doc_number}")
        #     print(f"File path: {docid_dict[str(doc_number)]}")
        #     print()
        # print(len(intersection))
        term_dict.clear()


if __name__ == "__main__":
    search()

"""
{"do": [[1, 2, [1807, 1426], 4439]], 
"doctype": [[1, 1, [0, 0], 4439]], 
"doctoral": [[1, 1, [3055, 0], 4439]], 
"document": [[1, 4, [334, 3902], 4439]], 
"donald": [[1, 6, [116, 2851], 4439], [2, 3, [8, 41], 116]], 
"done": [[1, 1, [4383, 0], 4439]], 
"domready": [[1, 2, [305, 3], 4439]], 
"domcontentloaded": [[1, 1, [315, 0], 4439]], 
"download": [[1, 1, [3281, 0], 4439]], 
"doubt": [[1, 1, [3522, 0], 4439]], 
"de": [[1, 3, [3820, 41], 4439]], 
"description": [[1, 1, [13, 0], 4439], [2, 1, [75, 0], 116]], 
"describe": [[1, 1, [3220, 0], 4439]], 
"descending": [[1, 1, [4267, 0], 4439]], 
"design": [[1, 2, [2580, 1333], 4439]],
"designed": [[1, 1, [3875, 0], 4439]], 
"department": [[1, 6, [22, 938], 4439], [2, 3, [4, 41], 116]], 
"dept_header": [[1, 1, [783, 0], 4439]], 
"dept_link": [[1, 1, [785, 0], 4439]],
"""


# "{'special':[(2,1,[3425,3425],4495)]}"
# document 2 fequency 1 第一个位置， 最后一个位置 ， total length
# "{'splitpgbreakandparamark':[(1,1,[204,204],3050),(1,1,[208,208],4495),
# (1,1,[204,204],2947),(1,1,[208,208],4097),(1,1,[204,204],3258),(1,1,[204,204],4677)]}"