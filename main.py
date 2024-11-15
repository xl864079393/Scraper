from controller import InvertController
from bean import InvertedIdx
from dao import to_File
import json

folder_path = "ANALYST"
inverted = InvertedIdx.InvertedIndex()

InvertController.start(folder_path, inverted)

with open("raw_result.json", 'w',encoding="utf-8") as f:
    terms = inverted.get_all_terms()
    print("Total term:" + str(len(terms)))
    print("Total doc:" + str(len(inverted.total_doc)))
    for term in terms:
        postings = inverted.get_raw_postings(term)
        json.dump(str(postings).replace(" ", ""), f, ensure_ascii = False)

