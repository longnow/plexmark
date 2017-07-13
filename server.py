#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from sanic import Sanic
from sanic.response import json
import plexmark

app = Sanic()

@app.route("/")
async def generate_words(request):
    args = {a: request.args[a][0] for a in request.args}
    expr_list = plexmark.generate_words(args['uid'], int(args['state_size']), int(args['count']))
    return json(expr_list)

@app.route("/cleanup")
async def cleanup(request):
    args = {a: request.args[a][0] for a in request.args}
    cleanup(int(args['max_age']))


if __name__ == "__main__":
    from plexmark import PLText, PLChain
    app.run(port=3004)