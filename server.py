#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from sanic import Sanic
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
    expr_list = await plexmark.generate_words(args['uid'], int(args['state_size']), int(args['count']))
    return json(expr_list)

@app.route("/cleanup")
async def cleanup(request):
    args = {a: request.args[a][0] for a in request.args}

    if 'max_age' in args:
        max_age = int(args['max_age'])
    else:
        max_age = config.CLEANUP_MAX_AGE

    await plexmark.cleanup(max_age)
    return ''

if __name__ == "__main__":
    from plexmark import PLText, PLChain
    app.run(port=config.PORT)
