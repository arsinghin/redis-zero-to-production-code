"""Part 9b: the same menu search with the Redis Query Engine (FT.* commands).

NOT RUN BY THE BOOK'S TEST SUITE. The Query Engine ships with Redis Open
Source 8 builds that include it (for example the official `redis:8` Docker
image), but not with every build. Check first:  redis-cli FT._LIST
"""
import numpy as np
from redis.commands.search.field import NumericField, TagField, TextField, VectorField
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import Query

from common import get_client

r = get_client()   # binary vectors: decode_responses stays True for normal fields

MENU = {
    "pizza:margherita": ("Margherita",     [0.9, 0.1, 0.4, 0.0], 12, "veg"),
    "pizza:pepperoni":  ("Pepperoni",      [0.7, 0.6, 0.0, 0.9], 14, "meat"),
    "pizza:veggie":     ("Veggie Supreme", [0.4, 0.2, 0.9, 0.0], 13, "veg"),
}

# 1. Create an index over every hash whose key starts with "pizza:".
try:
    r.ft("idx:menu").dropindex(delete_documents=False)
except Exception:
    pass
r.ft("idx:menu").create_index(
    fields=[
        TextField("name"),
        NumericField("price"),
        TagField("type"),
        VectorField("vector", "HNSW",
                    {"TYPE": "FLOAT32", "DIM": 4, "DISTANCE_METRIC": "COSINE"}),
    ],
    definition=IndexDefinition(prefix=["pizza:"], index_type=IndexType.HASH),
)

# 2. Store each pizza as a hash. In a hash, the vector must be raw FLOAT32 bytes.
for key, (name, vector, price, kind) in MENU.items():
    r.hset(key, mapping={"name": name, "price": price, "type": kind,
                         "vector": np.array(vector, dtype=np.float32).tobytes()})

# 3. KNN search, optionally mixed with normal filters.
query_vec = np.array([0.0, 0.0, 1.0, 0.0], dtype=np.float32).tobytes()
q = (Query("(@type:{veg} @price:[0 13])=>[KNN 2 @vector $vec AS distance]")
     .sort_by("distance")
     .return_fields("name", "price", "distance")
     .dialect(2))
for doc in r.ft("idx:menu").search(q, query_params={"vec": query_vec}).docs:
    # COSINE distance = 1 - cosine similarity (smaller is closer)
    print(doc.name, doc.price, doc.distance)
