from controller import InvertController
from bean import InvertedIdx
from dao import to_File
from collections import defaultdict
import json
import time

# 文件夹路径
folder_path = "DEV"

# 创建倒排索引
inverted = InvertedIdx.InvertedIndex()

# 开始构建倒排索引
start_time = time.time()
InvertController.start(folder_path, inverted)
end_time = time.time()
print(f"Time to build inverted index: {end_time - start_time}")

terms = inverted.container.dict.keys()
print("Total term:" + str(len(terms)))
print("Total doc:" + str(inverted.total_doc))
    # json.dump(dict(inverted.container.dict), f, ensure_ascii=False)

# with open("bitmap.json", 'w',encoding="utf-8") as f:
#     json.dump(dict(inverted.container.bitmap), f, ensure_ascii=False)





