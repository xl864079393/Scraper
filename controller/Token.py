import os
import json
from collections import defaultdict, Counter
from bean.InvertedIdx import InvertedIndex
import re
from bs4 import BeautifulSoup
from nltk.stem import PorterStemmer
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import threading
import gc



def tokenize(text):
    return re.findall(r'\b\w+\b', text.lower())


def process_document(document_id, json_content):

    ps = PorterStemmer()

    def extract_and_stem(text):
        tokens = re.findall(r'\b\w+\b', text.lower())
        return [ps.stem(token) for token in tokens]

    json_content = json.loads(json_content)

    if not isinstance(json_content, dict) or json_content["content"] is None:
        raise ValueError("Input JSON must contain a 'content' field.")

    content = json_content["content"]

    soup = BeautifulSoup(content, "html.parser")

    body_text = soup.get_text()
    tokens = extract_and_stem(body_text)

    return document_id, tokens


# def process_document_with_timeout(num, json_content, timeout_seconds):
#     result = [None]
#
#     def target():
#         try:
#             result[0] = process_document(num, json_content)
#         except Exception as e:
#             result[0] = None
#
#
#     thread = threading.Thread(target = target)
#     thread.start()
#     thread.join(timeout_seconds)
#     if thread.is_alive():
#         return None
#
#     return result[0]

def build_from_json_files(folder_path, inverted_index):
    docid_dict = {}
    num = 1
    all_files = [
        os.path.join(folder_path, folder_name, file_name)
        for folder_name in os.listdir(folder_path)
        for file_name in os.listdir(os.path.join(folder_path, folder_name))
        if file_name.endswith(".json")
    ]

    for file_path in all_files:
        with open(file_path, "r", encoding = "utf-8") as file:
            json_content = file.read()

        document = process_document(num, json_content)

        if document:
            inverted_index.add_document(*document)

        if num % 3000 == 0:
            # print("Initiating all data")
            # inverted_index.Init_all_data()
            gc.collect()  # Manually trigger garbage collection
        # if num == 13909:
        #     # print the file name
        #     print(file_path)

        docid_dict[num] = file_path
        num += 1

    with open("docid_dict.json", "w", encoding="utf-8") as f:
        json.dump(docid_dict, f, ensure_ascii=False)
    # num = 1
    # for folder_name in os.listdir(folder_path):
    #     for file_name in os.listdir(os.path.join(folder_path, folder_name)):
    #         # if num % 3000 == 0:
    #         #     print("Initiating all data")
    #         #     inverted_index.Init_all_data()
    #         if file_name.endswith(".json"):
    #             file_path = os.path.join(folder_path,folder_name, file_name)
    #             with open(file_path, "r", encoding = "utf-8") as file:
    #                 start_time = time.time()
    #                 json_content = file.read()
    #                 print("Time read file: ", time.time() - start_time)
    #             document = process_document_with_timeout(num, json_content, timeout_seconds)
    #
    #             if document:  # If document was processed, add it to the inverted index
    #                 start_time = time.time()
    #                 inverted_index.add_document(*document)
    #                 print("Time add doc: ", time.time() - start_time)
    #             num += 1
    # inverted_index.Init_all_data()