#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pickle, os, time, asyncio, concurrent, functools, bisect, random
from glob import glob
from itertools import accumulate
import config
import cachetools
# import markovify
import aiopg

model_cache = cachetools.LFUCache(10)

BEGIN = "\u0002" # Start of Text
END = "\u0003" # End of Text

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

class PLText:
    
    def __init__(self, uid, state_size, expr_score_list, chain=None):
        self.uid = uid
        self.state_size = state_size
        self.expr_set = {ex[0] for ex in expr_score_list}
        self.chain = chain or PLChain(expr_score_list, state_size)

    def test_expr_output(self, expr):
        return expr not in self.expr_set

    def make_sentence(self, init_state=None, tries=10, test_output=True, max_chars=None, probability=False):
        for _ in range(tries):
            if init_state:
                if init_state[0] == BEGIN:
                    prefix = init_state[1:]
                else:
                    prefix = init_state
            else:
                prefix = ''
            if probability:
                expr, prob = self.chain.walk(init_state, probability)
                expr = prefix + expr
            else:
                expr = prefix + self.chain.walk(init_state, probability)
            if max_chars and len(expr) > max_chars:
                continue
            if test_output:
                if self.test_expr_output(expr):
                    if probability:
                        return expr, prob
                    else:
                        return expr
            else:
                if probability:
                    return expr, prob
                else:
                    return expr
        
class PLChain:
    def __init__(self, corpus, state_size, model=None):
        self.state_size = state_size
        self.model = model or self.build(corpus, self.state_size)
        self.precompute_begin_state()
    
    def build(self, corpus, state_size):
        model = {}
        for run, score in corpus:
            items = (BEGIN * state_size) + run + END
            for i in range(len(run) + 1):
                state = items[i:i+state_size]
                follow = items[i+state_size]
                if state not in model:
                    model[state] = {}
                if follow not in model[state]:
                    model[state][follow] = 0
                model[state][follow] += score
        return model

    def precompute_begin_state(self):
        begin_state = BEGIN * self.state_size
        choices, weights = zip(*self.model[begin_state].items())
        cumdist = list(accumulate(weights))
        self.begin_cumdist = cumdist
        self.begin_choices = choices
        self.begin_weights = weights

    def move(self, state, probability=False):
        if state == BEGIN * self.state_size:
            choices = self.begin_choices
            cumdist = self.begin_cumdist
            weights = self.begin_weights
        else:
            choices, weights = zip(*self.model[state].items())
            cumdist = list(accumulate(weights))
        r = random.random() * cumdist[-1]
        index = bisect.bisect(cumdist, r)
        selection = choices[index]
        if probability:
            prob = weights[index] / cumdist[-1]
            return selection, prob
        return selection

    def gen(self, init_state=None, probability=False):
        state = init_state or BEGIN * self.state_size
        while True:
            next_char = self.move(state)
            if next_char == END: break
            yield next_char
            state = state[1:] + next_char

    def walk(self, init_state=None, probability=False):
        if probability:
            state = init_state or BEGIN * self.state_size
            output = ''
            output_prob = 1
            while True:
                next_char, prob = self.move(state, probability)
                output_prob *= prob
                if next_char == END: break
                output += next_char
                state = state[1:] + next_char
            return output, output_prob
        else:
            return ''.join(list(self.gen(init_state, probability)))

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
        expr_score_list = await pickle_load(os.path.join(config.DATA_DIR, uid, 'expr_score_list.pickle'))
    except (FileNotFoundError, EOFError):
        print('fetching expressions for {}'.format(uid))
        expr_score_list = await pull_expr_from_db(uid)
        asyncio.ensure_future(pickle_expr(uid, expr_score_list))
    return expr_score_list

async def pickle_expr(uid, expr_score_list):
    os.makedirs(os.path.join(config.DATA_DIR, uid), exist_ok=True)
    await pickle_dump(expr_score_list, os.path.join(config.DATA_DIR, uid, 'expr_score_list.pickle'))

async def generate_model(uid, state_size):
    expr_score_list = await pull_expr(uid)
    print('building model for {}, state size: {}'.format(uid, state_size))
    return await run_in_process(PLText, uid=uid, state_size=state_size, expr_score_list=expr_score_list)

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
