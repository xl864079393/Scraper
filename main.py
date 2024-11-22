from controller import InvertController
from bean import InvertedIdx
from dao import to_File
from collections import defaultdict
import json


folder_path = "DEV"
inverted = InvertedIdx.InvertedIndex()

InvertController.start(folder_path, inverted)

with open("raw_result.json", 'w',encoding="utf-8") as f:
    terms = inverted.container.dict.keys()
    print("Total term:" + str(len(terms)))
    print("Total doc:" + str(inverted.total_doc))
    json.dump(dict(inverted.container.dict), f, ensure_ascii=False)

# with open("bitmap.json", 'w',encoding="utf-8") as f:
#     json.dump(dict(inverted.container.bitmap), f, ensure_ascii=False)





