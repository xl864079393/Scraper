import pickle
import os
import time

# 将倒排索引批次写入文件并返回偏移量
def save_inverted_index_batch(inverted_index_file, batch):
    """
    保存倒排索引批次到文件，并记录每个术语的偏移量。

    :param batch: dict，倒排索引批次
    :param inverted_index_file: str，倒排索引文件路径
    :return: dict，记录每个术语在文件中的起始和结束偏移量
    """
    term_offsets = {}  # 记录每个术语的偏移量

    # 确保以追加二进制模式写入
    with open(inverted_index_file, 'ab') as f:
        for term, postings in batch.items():
            start_pos = f.tell()  # 获取当前文件偏移量

            # 将单个术语及其倒排列表序列化并写入文件
            pickle.dump({term: postings}, f)

            end_pos = f.tell()  # 获取写入后的偏移量

            # 记录术语的偏移量
            term_offsets[term] = (start_pos, end_pos)

    return term_offsets

# 更新书籍管理文件，记录每个术语在某个文件中的偏移量
def update_bookkeeping_file(bookkeeping_file, term, file_name, start_pos, end_pos):
    entry = (term, file_name, start_pos, end_pos)  # 定义记录条目

    with open(bookkeeping_file, 'ab') as bf:  # 以二进制追加模式打开
        pickle.dump(entry, bf)  # 使用 pickle 序列化条目

# 批次处理函数：处理多个倒排索引批次并更新管理文件
def process_and_save_batches(inverted_index_batche, bookkeeping_file, batch_id):
    inverted_index_file = f"batches/batch_{batch_id}_index.pkl"

    # 将倒排索引批次保存到文件并返回偏移量
    start_time = time.time()
    term_offsets = save_inverted_index_batch(inverted_index_file,inverted_index_batche)
    end_time = time.time()
    print(f"Time to save batch {batch_id}: {end_time - start_time}")

    # 更新书籍管理文件
    start_time = time.time()
    for term, (start_pos, end_pos) in term_offsets.items():
        update_bookkeeping_file(bookkeeping_file, term, inverted_index_file, start_pos, end_pos)
    end_time = time.time()
    print(f"Time to update bookkeeping file for batch {batch_id}: {end_time - start_time}")


def load_inverted_index_for_term(term, bookkeeping_file):
    inverted_index = []

    # 打开书籍管理文件查找术语的所有记录
    with open(bookkeeping_file, 'r') as bf:
        for line in bf:
            term_in_file, file_name, start_pos, end_pos = line.strip().split()
            if term_in_file == term:
                start_pos, end_pos = int(start_pos), int(end_pos)

                # 根据偏移量读取倒排索引文件
                with open("sampledata/"+file_name, 'rb') as f:
                    f.seek(start_pos)
                    inverted_index_batch = pickle.load(f)
                    inverted_index += inverted_index_batch.get(term, [])

    return inverted_index

bookkeeping_files = "sampledata/bookkeeping.txt"
lopes_index = load_inverted_index_for_term("lopes", bookkeeping_files)
print(lopes_index)