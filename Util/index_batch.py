import pickle

# 将倒排索引批次写入文件并返回偏移量
def save_inverted_index_batch(batch, inverted_index_file):
    with open(inverted_index_file, 'ab') as f:
        start_pos = f.tell()  # 获取当前文件的偏移量

        # 序列化倒排索引批次，并写入文件
        pickle.dump(batch, f)

        end_pos = f.tell()  # 获取写入后的文件偏移量
    return start_pos, end_pos  # 返回写入的起始和结束偏移量

# 更新书籍管理文件，记录每个术语在某个文件中的偏移量
def update_bookkeeping_file(bookkeeping_file, term, file_name, start_pos, end_pos):
    entry = (term, file_name, start_pos, end_pos)  # 定义记录条目
    with open(bookkeeping_file, 'ab') as bf:  # 以二进制追加模式打开
        pickle.dump(entry, bf)  # 使用 pickle 序列化条目

# 批次处理函数：处理多个倒排索引批次并更新管理文件
def process_and_save_batches(inverted_index_batche, bookkeeping_file, batch_id):
    inverted_index_file = f"batches/batch_{batch_id}_index.pkl"

    # 将倒排索引批次保存到文件并返回偏移量
    start_pos, end_pos = save_inverted_index_batch(inverted_index_batche, inverted_index_file)

    # 更新书籍管理文件
    for term in inverted_index_batche.keys():
        update_bookkeeping_file(bookkeeping_file, term, inverted_index_file, start_pos, end_pos)
