#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from sanic import Sanic, response
from sanic.response import json
import config
import plexmark

app = Sanic()
app.config.from_object(config)

@app.listener("before_server_start")
async def init(app, loop):
    await plexmark.init()

@app.route("/")
async def generate_words(request):
    args = {a: request.args[a][0] for a in request.args}
    uid, state_size, count = args['uid'], int(args['state_size']), int(args['count'])
    try:
        init_state = args['init_state']
    except KeyError:
        init_state = None
    try:
        skip_re = args['skip_re']
    except KeyError:
        skip_re = r""
    expr_list = await plexmark.generate_words(uid, state_size, count, init_state, skip_re)
    return json(expr_list)

@app.route("/cleanup")
async def cleanup(request):
    args = {a: request.args[a][0] for a in request.args}

    if 'max_age' in args:
        max_age = int(args['max_age'])
    else:
        max_age = config.CLEANUP_MAX_AGE

    await plexmark.cleanup(max_age)
    return response.text('')

if __name__ == "__main__":
    from plexmark import PLText, PLChain
    app.run(port=config.PORT)
