from Util import Trie
import json
import re
import ast

def write_to_file(file, data):
    with open(file, 'w') as f:
        f.write(json.dumps(data))


def backup_to_file(file, root):
    with open(file, 'w') as f:
        serialized_data = _serialize(root)
        f.write(json.dumps(serialized_data))

def _serialize(node):
    if not node:
        return None
    node_data = {}
    for char, child_node in node.children.items():
        node_data[char] = _serialize(child_node)
    return {'children': node_data, 'is_end_of_word': node.is_end_of_word}

def read_file_to_dict(file_path, target_dict):
    """
    Reads data from a file containing JSON-like objects and loads it into a dictionary.

    :param target_dict:
    :param file_path: Path to the file.
    :return: A list of dictionaries parsed from the file.
    """
    try:
        with open(file_path, 'r') as file:
            content = file.read()
            # Split the content into JSON-like objects using regex
            items = re.findall(r'\{.*?\}', content)
            # Convert each item to a dictionary using `ast.literal_eval`
            for item in items:
                parsed_data = ast.literal_eval(item)  # 安全解析为 Python 字典
                for key, value in parsed_data.items():
                    # 如果键存在，追加到列表；否则初始化为列表
                    if key in target_dict:
                        target_dict[key].append(value)
                    else:
                        target_dict[key] = [value]
            return target_dict
    except Exception as e:
        print("An error occurred:", e)
        return []


# file_path = 'raw_result.json'  # Replace with your file's path
# target_dict = defaultdict(list)
# data_dict = to_File.read_file_to_dict(file_path, target_dict)
# print(data_dict)