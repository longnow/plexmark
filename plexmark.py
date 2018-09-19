#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pickle, os, time, asyncio, concurrent, functools, bisect, random, shutil
from glob import glob
from itertools import accumulate
from collections import defaultdict
import unicodedata2 as unicodedata
import regex as re
import config
import cachetools
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
        return unicodedata.normalize("NFC", expr) not in self.expr_set

    def make_expr(self, init_state=None, tries=10, test_output=True, skip_re=r"", probability=False):
        found = False
        for _ in range(tries):
            if init_state:
                init_state = unicodedata.normalize("NFD", init_state)
                prefix = init_state.strip(BEGIN)
                init_state = init_state.rjust(self.state_size, BEGIN)[-self.state_size:]
            else:
                prefix = ''
            try:
                if probability:
                    expr, prob = self.chain.walk(init_state, probability)
                    expr = prefix + expr
                else:
                    expr = prefix + self.chain.walk(init_state, probability)
            except KeyError:
                expr, prob = "", 0
            if test_output:
                if self.test_expr_output(expr):
                    if skip_re:
                        if not re.search(unicodedata.normalize("NFD", skip_re), expr):
                            found = True
                    else:
                        found = True
            else:
                found = True
            if found:
                if probability:
                    return expr, prob
                else:
                    return expr

    def expr_prob(self, expr):
        prepped_expr = BEGIN * self.state_size + expr + END
        output = 1
        for i in range(len(expr) + 1):
            output *= self.chain.prob(prepped_expr[i:i+self.state_size], prepped_expr[i+self.state_size])
        return output

class PLChain:
    def __init__(self, corpus, state_size, model=None):
        self.state_size = state_size
        self.model = model or self.build(corpus, self.state_size)
        self.precompute_begin_state()
    
    def build(self, corpus, state_size):
        model = {}
        model = defaultdict(lambda: defaultdict(int))
        for run, score in corpus:
            norm_run = unicodedata.normalize("NFD", run)
            items = (BEGIN * state_size) + norm_run + END
            for i in range(len(norm_run) + 1):
                state = items[i:i+state_size]
                follow = items[i+state_size]
                model[state][follow] += score
        model = dict({k: dict(model[k]) for k in model})
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

    def prob(self, state, char):
        try:
            total_score = sum(self.model[state].values())
            return self.model[state][char] / total_score
        except KeyError:
            return 0.0

async def pull_expr(uid, cache=True):
    if cache:
        query = """
   	        SELECT txt, score
   	        FROM exprx
   	        WHERE langvar = uid_langvar(%s)
   	        """
    else:
	    query = """
	       SELECT expr.txt, grp_quality_score(array_agg(denotationx.grp), array_agg(denotationx.quality))
	       FROM expr
	       JOIN denotationx ON (denotationx.expr = expr.id)
	       WHERE expr.langvar = uid_langvar(%s)
	       GROUP BY expr.id
	       """
    print('fetching expressions for {}'.format(uid))
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query, (uid,), timeout=config.REQUEST_TIMEOUT)
            return await cur.fetchall()

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
    for uid in uid_list:
        try:
            # file_age = now - os.path.getmtime(os.path.join(config.DATA_DIR, uid, 'expr_score_list.pickle'))
            file_age = now - os.path.getctime(os.path.join(config.DATA_DIR, uid))
            print("{} age is {}".format(uid, file_age))
            if file_age > max_age:
                print(uid + " is old. clearing...")
                # await run_in_process(clear_uid_dir, uid)
                shutil.rmtree(os.path.join(config.DATA_DIR, uid))
                for key in model_cache.keys():
                    if key[0] == uid:
                        del model_cache[key]
        except FileNotFoundError:
            pass

async def generate_words(uid, state_size, count, init_state=None, skip_re=r""):
    model = await pull_model(uid, state_size)
    expr_list = [model.make_expr(init_state=init_state, tries=100, skip_re=skip_re) for _ in range(count)]
    return [expr for expr in expr_list if expr]

