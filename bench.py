#!/usr/bin/env python
# -*- coding: utf-8 -*-

# %%
import io
import os
import random
import re
from pathlib import Path

import lmdb
from PIL import Image
from tqdm import tqdm

RE_IMG = re.compile(r".*\.(jpe?g|png|bmp)$", re.IGNORECASE)
IMAGES = "/home/psvoboda/celeba"
LMDB = "/home/psvoboda/lmdb-db"


def walk_files(dir_path, re_match=RE_IMG):
    """Yields files in dir_path based on re_img"""
    for root, dirs, files in os.walk(dir_path):
        for entry in files:
            if re_match.match(entry):
                yield Path(root, entry).as_posix()


# %%
# Create the DB
env = lmdb.open(LMDB, map_size=10737418240)
with env.begin(write=True) as txn:
    for path in tqdm(walk_files(IMAGES)):
        with open(path, "rb") as data:
            bdata = data.read(-1)
            txn.put(path.encode("utf8"), bdata)
env.close()


# %%
# Benchmark random read from DB
paths = [path for path in walk_files(IMAGES)]
random.shuffle(paths)

env = lmdb.open(LMDB, map_size=10737418240)
with env.begin(buffers=False) as txn:
    for path in tqdm(paths):
        data = txn.get(path.encode("utf8"))
env.close()


# %%
# Benchmark random read from DB shadowed by image decoding
paths = [path for path in walk_files(IMAGES)]
random.shuffle(paths)

env = lmdb.open(LMDB, map_size=10737418240)
with env.begin(buffers=False) as txn:
    for path in tqdm(paths):
        data = txn.get(path.encode("utf8"))
        img = Image.open(io.BytesIO(data))
env.close()

