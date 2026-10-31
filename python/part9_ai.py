"""Part 9: Redis for AI. Chat memory, vector search with Vector Sets,
a semantic cache, and a tiny retrieval step for RAG.

To keep the math checkable by hand, embed() below is a TOY: it turns text
into 4 numbers (cheesy, spicy, healthy, meaty). In a real app you would
call an embedding model instead and get 384-3072 numbers back.
"""
import json
import math
import time

from common import get_client, reset, title

r = get_client()
reset(r, "chat:*", "menu:vec", "semcache", "semcache:*", "kb:vec")

# ----------------------------------------------------------- toy embedding
WORDS = {
    0: {"cheese", "cheesy", "mozzarella", "classic", "margherita"},
    1: {"spicy", "spiciest", "hot", "chilli", "jalapeno", "fiery"},
    2: {"healthy", "greens", "veggie", "vegetables", "light", "salad", "vegan"},
    3: {"meat", "meaty", "pepperoni", "chicken", "bbq", "bacon"},
}


def embed(text):
    words = text.lower().replace("?", " ").replace(",", " ").split()
    v = [sum(w in WORDS[d] for w in words) for d in range(4)]
    norm = math.sqrt(sum(x * x for x in v)) or 1.0
    return [x / norm for x in v]


def cosine(a, b):
    return sum(x * y for x, y in zip(a, b)) / (
        math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(y * y for y in b)))


# --------------------------------------------------------- 1. chat memory
title("Chat memory: keep the last 6 messages, forget idle chats after 30 min")


def remember(session, role, text, keep=6, idle_ttl=1800):
    key = f"chat:{session}"
    pipe = r.pipeline()                                  # MULTI/EXEC
    pipe.rpush(key, json.dumps({"role": role, "text": text}))
    pipe.ltrim(key, -keep, -1)                           # cap the list
    pipe.expire(key, idle_ttl)                           # reset the idle timer
    pipe.execute()


def history(session):
    return [json.loads(m) for m in r.lrange(f"chat:{session}", 0, -1)]


remember("99", "user", "I want a large pepperoni.")
remember("99", "ai", "Added a large Pepperoni. Anything else?")
remember("99", "user", "Make it extra spicy.")
for m in history("99"):
    print(f"  {m['role']:4}: {m['text']}")
for i in range(10):
    remember("99", "user", f"filler message {i}")
print("  messages kept after 13 sends:", r.llen("chat:99"), "| TTL:", r.ttl("chat:99"), "s")

# ------------------------------------------------- 2. vector search (Vector Sets)
title("Vector Sets: search the menu by meaning")
MENU = {
    "Margherita":     ([0.9, 0.1, 0.4, 0.0], {"price": 12, "veg": True}),
    "Pepperoni":      ([0.7, 0.6, 0.0, 0.9], {"price": 14, "veg": False}),
    "Veggie Supreme": ([0.4, 0.2, 0.9, 0.0], {"price": 13, "veg": True}),
    "BBQ Chicken":    ([0.5, 0.3, 0.1, 0.8], {"price": 15, "veg": False}),
    "Spicy Paneer":   ([0.5, 0.9, 0.5, 0.0], {"price": 13, "veg": True}),
}
vs = r.vset()
for name, (vector, attrs) in MENU.items():
    vs.vadd("menu:vec", vector, name, attributes=attrs)

query = embed("something healthy with greens")
print("  query vector:", query)
for name, score in vs.vsim("menu:vec", query, with_scores=True, count=3).items():
    print(f"  {name:15} score={score:.4f}   cosine={cosine(query, MENU[name][0]):+.4f}")

print("  with a filter (veg and under $13):",
      list(vs.vsim("menu:vec", query, count=3, filter=".veg and .price < 13")))

# ---------------------------------------------------- 3. semantic cache
title("Semantic cache: don't pay the LLM twice for the same question")
llm_calls = 0


def fake_llm(question):
    global llm_calls
    llm_calls += 1
    time.sleep(0.3)                                     # LLMs are slow (and cost money)
    return "Try the Veggie Supreme: lots of vegetables, $13."


def ask(question, threshold=0.95, ttl=3600):
    q = embed(question)
    if r.exists("semcache"):
        hits = vs.vsim("semcache", q, with_scores=True, count=1)
        for cached_q, score in hits.items():
            answer = r.get(f"semcache:answer:{cached_q}")
            if score >= threshold and answer is not None:
                return answer, f"cache hit (score {score:.3f})"
    answer = fake_llm(question)
    vs.vadd("semcache", q, question)
    r.set(f"semcache:answer:{question}", answer, ex=ttl)
    return answer, "LLM call"


for question in ["Do you have a healthy veggie pizza?",
                 "Any healthy pizza with vegetables?",
                 "What's your spiciest pizza?"]:
    start = time.perf_counter()
    answer, how = ask(question)
    print(f"  {question:38} -> {how:24} {(time.perf_counter() - start) * 1000:5.0f} ms")
print("  LLM calls:", llm_calls)

# ------------------------------------------------ 4. retrieval for RAG
title("RAG: fetch the right facts, then build the prompt")
FACTS = {
    "kb:delivery": "Delivery is free for orders over $25 within 5 km.",
    "kb:vegan":    "All veggie pizzas can be made vegan with cashew cheese, healthy and light.",
    "kb:spicy":    "Spicy Paneer uses fresh jalapeno and chilli oil; it is our hottest pizza.",
    "kb:meat":     "BBQ Chicken and Pepperoni use halal meat.",
}
for fact_id, text in FACTS.items():
    vec = embed(text)
    if any(vec):                                         # the toy can't embed every fact
        vs.vadd("kb:vec", vec, fact_id)

question = "Can I get a vegan veggie pizza?"
found = vs.vsim("kb:vec", embed(question), with_scores=True, count=2)
print("  top 2, no cutoff:", {f: round(s, 3) for f, s in found.items()})
top = [f for f, score in found.items() if score >= 0.75]   # keep only real matches
print("  after a 0.75 cutoff:", top)
context = "\n".join(f"- {FACTS[f]}" for f in top)
prompt = f"Answer using only these facts:\n{context}\n\nQuestion: {question}"
print(prompt)
