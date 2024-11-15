from Util import Trie
import json

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