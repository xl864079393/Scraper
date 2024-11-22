import RLE as rle
import json
import time
from bitarray import bitarray
import pyroaring

def rle_encode(data):
    return rle.encode(data)


def rle_decode(data, count):
    return rle.decode(data, count)


