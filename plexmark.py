#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pickle, os, time, asyncio, concurrent, functools
from glob import glob
import config
import cachetools
import markovify
import aiopg

model_cache = cachetools.LFUCache(10)

BEGIN = "___BEGIN__"
END = "___END__"

async def init():
    global pool, executor
    pool = await aiopg.create_pool("dbname={} user={}".format(config.DB_NAME, config.DB_USER), minsize=config.DB_POOL_MIN, maxsize=config.DB_POOL_MAX)
    executor = concurrent.futures.ProcessPoolExecutor(max_workers=config.MAX_WORKERS)

async def run_in_process(*args, **kwargs):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, functools.partial(*args, **kwargs))

def _pickle_load(path):
    return pickle.load(open(path, 'rb'))

async def pickle_load(*args):
    return await run_in_process(_pickle_load, *args)

def _pickle_dump(obj, path):
    pickle.dump(obj, open(path, 'wb'), pickle.HIGHEST_PROTOCOL)

async def pickle_dump(*args):
    return await run_in_process(_pickle_dump, *args)

def rejoin_text(parsed_sentences, uid):
    try:
        rejoined_text = model_cache[uid]
    except KeyError:
        rejoined_text = {''.join(ex[0]) for ex in parsed_sentences}
        model_cache[uid] = rejoined_text
    return rejoined_text

class PLText(markovify.Text):

    # def __init__(self, input_text, state_size=2, chain=None, parsed_sentences=None):
    def __init__(self, uid, state_size, parsed_sentences)
        self.uid = uid
        self.state_size = state_size
        self.parsed_sentences = parsed_sentences
        # self.rejoined_text = {''.join(ex[0]) for ex in self.parsed_sentences}
        self.rejoined_text = rejoin_text(self.parsed_sentences, self.uid)
        self.chain = PLChain(self.parsed_sentences, state_size)

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
            await cur.execute(query, (uid,), timeout=config.REQUEST_TIMEOUT)
            return await cur.fetchall()

async def pull_expr(uid):
    try:
        parsed_sentences = await pickle_load(os.path.join(config.DATA_DIR, uid, 'expr_score_list.pickle'))
    except (FileNotFoundError, EOFError):
        print('fetching expressions for {}'.format(uid))
        expr_score_list = await pull_expr_from_db(uid)
        parsed_sentences = [(list(ex[0]), ex[1]) for ex in expr_score_list]
        asyncio.ensure_future(pickle_expr(uid, parsed_sentences))
    return parsed_sentences

async def pickle_expr(uid, parsed_sentences):
    os.makedirs(os.path.join(config.DATA_DIR, uid), exist_ok=True)
    await pickle_dump(parsed_sentences, os.path.join(config.DATA_DIR, uid, 'expr_score_list.pickle'))

async def generate_model(uid, state_size):
    parsed_sentences = await pull_expr(uid)
    print('building model for {}, state size: {}'.format(uid, state_size))
    return await run_in_process(PLText, uid=uid, state_size=state_size, parsed_sentences=parsed_sentences)

async def pull_model(uid, state_size):
    try:
        pltext = model_cache[(uid, state_size)]
    except KeyError:
        try:
            pltext = await pickle_load(os.path.join(config.DATA_DIR, uid, str(state_size) + '.pickle'))
        except (FileNotFoundError, EOFError):
            pltext = await generate_model(uid, state_size)
            asyncio.ensure_future(pickle_model(uid, state_size, pltext))
        model_cache[(uid, state_size)] = pltext
    return pltext

async def pickle_model(uid, state_size, pltext):
    os.makedirs(os.path.join(config.DATA_DIR, uid), exist_ok=True)
    await pickle_dump(pltext, os.path.join(config.DATA_DIR, uid, str(state_size) + '.pickle'))

def clear_uid_dir(uid):
    for filename in glob(os.path.join(config.DATA_DIR, uid, '*.pickle')):
        os.remove(filename)

async def cleanup(max_age):
    uid_list = [os.path.basename(filename) for filename in glob(os.path.join(config.DATA_DIR, '*'))]
    now = time.time()
    try:
        for uid in uid_list:
            file_age = now - os.path.getmtime(os.path.join(config.DATA_DIR, uid, 'expr_score_list.pickle'))
            if file_age > max_age:
                await run_in_process(clear_uid_dir, uid)
                for key in model_cache.keys():
                    if key[0] == uid:
                        del model_cache[key]
    except FileNotFoundError:
        pass

async def generate_words(uid, state_size, count):
    model = await pull_model(uid, state_size)
    expr_list = [model.make_sentence(tries=100) for _ in range(count)]
    return [expr for expr in expr_list if expr]
