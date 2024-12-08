import pickle
import os
import time
import gzip

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
    with gzip.open(inverted_index_file, 'ab') as f:
        for term, postings in batch.items():
            start_pos = f.tell()  # 获取当前文件偏移量

            # 将单个术语及其倒排列表序列化并写入文件
            pickle.dump({term: postings}, f)

            end_pos = f.tell()  # 获取写入后的偏移量

            # 记录术语的偏移量
            term_offsets[term] = (start_pos, end_pos)

    return term_offsets

# 更新书籍管理文件，记录每个术语在某个文件中的偏移量
def update_bookkeeping_file(bf, term, file_name, start_pos, end_pos, index_terms):
    entry = (term, file_name, start_pos, end_pos)  # 定义记录条目
    offset = bf.tell()  # 获取当前文件偏移量
    pickle.dump(entry, bf)  # 将记录条目写入书籍管理文件（以二进制格式）
    if term in index_terms:
        index_terms[term].append(offset)
    else:
        index_terms[term] = [offset]

# 批次处理函数：处理多个倒排索引批次并更新管理文件
def process_and_save_batches(inverted_index_batche, index_terms, batch_id):
    inverted_index_file = f"batches/batch_{batch_id}_index.pkl"

    # 将倒排索引批次保存到文件并返回偏移量
    start_time = time.time()
    term_offsets = save_inverted_index_batch(inverted_index_file,inverted_index_batche)
    end_time = time.time()
    print(f"Time to save batch {batch_id}: {end_time - start_time}")

    # 更新书籍管理文件
    start_time = time.time()
    with gzip.open("bookkeeping.pkl", 'ab') as bf:
        for term, (start_pos, end_pos) in term_offsets.items():
            update_bookkeeping_file(bf, term, batch_id, start_pos, end_pos, index_terms)
    end_time = time.time()
    print(f"Time to update bookkeeping file for batch {batch_id}: {end_time - start_time}")


def load_inverted_index_for_term(term, bookkeeping_file, positions, file_handler):
    """
    根据术语从书籍管理文件中查找偏移量，并加载倒排索引。

    :param term: str，目标术语
    :param bookkeeping_file: str，书籍管理文件路径（二进制格式）
    :return: list，包含术语在所有批次中的倒排索引条目
    """
    inverted_index = {}  # 用于存储与术语相关的倒排索引
    bookkeeping_datas = []

    try:
        with open(bookkeeping_file, 'rb') as bf:
            for position in positions:
                bf.seek(position)
                bookkeeping_datas.append(pickle.load(bf))
        # 假设 `bookkeeping_data` 是一个列表，每项为 (term, file_name, start_pos, end_pos)
        for term_in_file, batch_id, start_pos, end_pos in bookkeeping_datas:
            if term_in_file == term:
                start_pos, end_pos = int(start_pos), int(end_pos)
                # 根据偏移量读取倒排索引文件
                f = file_handler[batch_id-1]
                f.seek(start_pos)
                inverted_index_batch = pickle.load(f)


                # 合并当前批次中与术语相关的倒排索引
                for term, postings in inverted_index_batch.items():
                    if term in inverted_index:
                        inverted_index[term].extend(postings)
                    else:
                        inverted_index[term] = postings
    except FileNotFoundError as e:
        print(f"File not found: {e}")
    except Exception as e:
        print(f"Error loading bookkeeping file or index files: {e}")

    return inverted_index





