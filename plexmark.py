#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pickle, os, time, concurrent, asyncio, functools
from glob import glob
import cachetools
import markovify
import aiopg

model_cache = cachetools.LFUCache(6)

BEGIN = "___BEGIN__"
END = "___END__"
DBNAME = "plx"
USER = "yang"
DATA_DIR = os.path.join('data')

async def init():
    global pool, executor
    pool = await aiopg.create_pool("dbname={} user={}".format(DBNAME, USER), minsize=1, maxsize=5)
    executor = concurrent.futures.ProcessPoolExecutor(max_workers=3)

async def run_in_process(*args, **kwargs):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, functools.partial(*args, **kwargs))

class PLText(markovify.Text):

    def __init__(self, input_text, state_size=2, chain=None, parsed_sentences=None):
        self.state_size = state_size
        self.parsed_sentences = parsed_sentences or list(self.generate_corpus(input_text))
        self.rejoined_text = {''.join(ex[0]) for ex in self.parsed_sentences}
        self.chain = chain or PLChain(self.parsed_sentences, state_size)

    def sentence_split(self, ex_list):
        return ex_list

    def word_split(self, sentence):
        return list(sentence)

    def word_join(self, words):
        return "".join(words)

    def test_sentence_input(self, sentence):
        return True

    def test_sentence_output(self, words, *args, **kwargs):
        return ''.join(words) not in self.rejoined_text

    def generate_corpus(self, text):
        return map(lambda x: (list(x[0]), x[1]), text)

class PLChain(markovify.Chain):
    def build(self, corpus, state_size):
        model = {}

        for run, score in corpus:
            items = ([ BEGIN ] * state_size) + run + [ END ]
            for i in range(len(run) + 1):
                state = tuple(items[i:i+state_size])
                follow = items[i+state_size]
                if state not in model:
                    model[state] = {}

                if follow not in model[state]:
                    model[state][follow] = 0

                model[state][follow] += score
        return model

async def pull_expr_from_db(uid):
    query = """
        SELECT expr.txt, grp_quality_score(array_agg(denotationx.grp), array_agg(denotationx.quality))
        FROM expr
        JOIN denotationx ON (denotationx.expr = expr.id)
        WHERE expr.langvar = uid_langvar(%s)
        GROUP BY expr.id
        """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query, (uid,))
            return await cur.fetchall()

async def pull_expr(uid):
    print('fetching expressions for {}'.format(uid))
    expr_score_list = await pull_expr_from_db(uid)
    parsed_sentences = [(list(ex[0]), ex[1]) for ex in expr_score_list]
    try:
        pickle.dump(parsed_sentences, open(os.path.join(DATA_DIR, uid, 'expr_score_list.pickle'), 'wb'), pickle.HIGHEST_PROTOCOL)
    except FileNotFoundError:
        os.makedirs(os.path.join(DATA_DIR, uid))
        pickle.dump(parsed_sentences, open(os.path.join(DATA_DIR, uid, 'expr_score_list.pickle'), 'wb'), pickle.HIGHEST_PROTOCOL)
    return parsed_sentences

async def generate_model(uid, state_size):
    try:
        parsed_sentences = pickle.load(open(os.path.join(DATA_DIR, uid, 'expr_score_list.pickle'), 'rb'))
    except (FileNotFoundError, EOFError):
        parsed_sentences = await pull_expr(uid)
    print('building model for {}, state size: {}'.format(uid, state_size))
    return await run_in_process(PLText, '', state_size, parsed_sentences=parsed_sentences)

async def pull_model(uid, state_size):
    try:
        pltext = model_cache[(uid, state_size)]
    except KeyError:
        try:
            pltext = pickle.load(open(os.path.join(DATA_DIR, uid, str(state_size) + '.pickle'), 'rb'))
        except (FileNotFoundError, EOFError):
            pltext = await generate_model(uid, state_size)
            pickle.dump(pltext, open(os.path.join(DATA_DIR, uid, str(state_size) + '.pickle'), 'wb'), pickle.HIGHEST_PROTOCOL)
        model_cache[(uid, state_size)] = pltext
    return pltext

def cleanup(max_age=604800):
    uid_list = [os.path.basename(filename) for filename in glob(os.path.join(DATA_DIR, '*'))]
    try:
        file_age = time.time() - os.path.getmtime(os.path.join(DATA_DIR, uid, 'expr_score_list.pickle'))
        if file_age > max_age: # 7 days
            for filename in glob(os.path.join(DATA_DIR, uid, '*.pickle')):
                os.remove(filename)
            for key in model_cache.keys():
                if key[0] == uid:
                    model_cache.popitem(key)
    except FileNotFoundError:
        pass

async def generate_words(uid, state_size, count):
    model = await pull_model(uid, state_size)
    expr_list = [model.make_sentence(tries=100) for _ in range(count)]
    return [expr for expr in expr_list if expr]
