from Util import Container
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
    try:
        with open(file_path, 'r') as file:
            content = file.read()
            items = re.findall(r'\{.*?\}', content)
            for item in items:
                parsed_data = ast.literal_eval(item)
                for key, value in parsed_data.items():
                    if key in target_dict:
                        target_dict[key].append(value)
                    else:
                        target_dict[key] = [value]
            return target_dict
    except Exception as e:
        print("An error occurred:", e)
        return []