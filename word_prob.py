from plexmark import *
from tqdm import tqdm

asyncio.get_event_loop().run_until_complete(init())

def prob_dist(uid, state_size, model=None):
    if not model: model = asyncio.get_event_loop().run_until_complete(pull_model(uid, state_size))
    return(sorted([(expr, model.expr_prob(expr)) for expr in tqdm(model.expr_set)], key=lambda _: _[1], reverse=True))

def highest_prob(uid, state_size, tries=100):
    model = asyncio.get_event_loop().run_until_complete(pull_model(uid, state_size))
    iteration_number = 0
    output = ('', 0)
    state = BEGIN * model.state_size
    while True:
        new_good_word = False
        current_expr = ''
        current_prob = 1
        while True:
            next_char, prob = model.chain.move(state, probability=True)
            current_prob *= prob
            if current_prob <= output[1]: break
            if next_char == END:
                new_good_word = True
                break
            current_expr += next_char
            state = state[1:] + next_char
        if new_good_word:
            output = (current_expr, current_prob)
            iteration_number = 0
            yield output
        else:
            iteration_number += 1
            if iteration_number > tries:
                break