#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import markovify, pickle, os, time
from glob import glob
from psycopg2.pool import SimpleConnectionPool
from contextlib import contextmanager
import cachetools

model_cache = cachetools.LFUCache(6) # keep max of 6 models, expire after 10 minutes

BEGIN = "___BEGIN__"
END = "___END__"
DBNAME = "plx"
USER = "yang"
DATA_DIR = os.path.join('data')

pool = SimpleConnectionPool(1, 3, database=DBNAME, user=USER)

@contextmanager
def getcursor():
    con = pool.getconn()
    try:
        yield con.cursor()
    finally:
        pool.putconn(con)

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

def all_ex(uid):
    query = """
        SELECT expr.txt, grp_quality_score(array_agg(denotationx.grp), array_agg(denotationx.quality))
        FROM expr
        JOIN denotationx ON (denotationx.expr = expr.id)
        WHERE expr.langvar = uid_langvar(%s)
        GROUP BY expr.id
        """
    with getcursor() as cur:
        cur.execute(query, (uid,))
        return cur.fetchall()

def pull_expr(uid):
    print('fetching expressions for {}'.format(uid))
    expr_score_list = all_ex(uid)
    parsed_sentences = [(list(ex[0]), ex[1]) for ex in expr_score_list]
    try:
        pickle.dump(parsed_sentences, open(os.path.join(DATA_DIR, uid, 'expr_score_list.pickle'), 'wb'), pickle.HIGHEST_PROTOCOL)
    except FileNotFoundError:
        os.makedirs(os.path.join(DATA_DIR, uid))
        pickle.dump(parsed_sentences, open(os.path.join(DATA_DIR, uid, 'expr_score_list.pickle'), 'wb'), pickle.HIGHEST_PROTOCOL)
    return parsed_sentences

def generate_model(uid, state_size):
    try:
        parsed_sentences = pickle.load(open(os.path.join(DATA_DIR, uid, 'expr_score_list.pickle'), 'rb'))
    except (FileNotFoundError, EOFError):
        parsed_sentences = pull_expr(uid)
    print('building model for {}, state size: {}'.format(uid, state_size))
    return PLText('', state_size, parsed_sentences=parsed_sentences)

@cachetools.cached(model_cache)
def pull_model(uid, state_size):
    # try:
    #     file_age = time.time() - os.path.getmtime(os.path.join(DATA_DIR, uid, 'expr_score_list.pickle'))
    #     if file_age > 604800: # 7 days
    #         for filename in glob(os.path.join(DATA_DIR, uid, '*.pickle')):
    #             os.remove(filename)
    # except FileNotFoundError:
    #     pass
    try:
        pltext = pickle.load(open(os.path.join(DATA_DIR, uid, str(state_size) + '.pickle'), 'rb'))
    except (FileNotFoundError, EOFError):
        pltext = generate_model(uid, state_size)
        pickle.dump(pltext, open(os.path.join(DATA_DIR, uid, str(state_size) + '.pickle'), 'wb'), pickle.HIGHEST_PROTOCOL)
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

def generate_words(uid, state_size, count):
    model = pull_model(uid, state_size)
    expr_list = [model.make_sentence(tries=100) for _ in range(count)]
    print(model_cache.keys())
    return [expr for expr in expr_list if expr]
