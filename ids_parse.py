import regex as re
from collections import defaultdict
import unicodedata2 as unicodedata

def parse_ids_file(filename):
    ids_dict = {}
    for line in open(filename).readlines():
        if not line or line.startswith("#") or line.startswith(";;"): continue
        splitline = line.strip().split("\t")
        char = splitline[1]
        ids_set = {re.sub(r"\[.+\]", "", i) for i in set(splitline[2:])}
        ids_dict[char] = ids_set
    return ids_dict

def normalize_ids_dict(ids_dict):
    new_ids_dict = defaultdict(set)
    for k in ids_dict:
        norm_k = unicodedata.normalize("NFC", k)
        norm_ids_set = {unicodedata.normalize("NFC", ids) for ids in ids_dict[k]}
        new_ids_dict[norm_k] = new_ids_dict[norm_k].union(norm_ids_set)
    return new_ids_dict

def recbreak(ids_string, ids_dict, former_output = []):
    if len(ids_string) == 0:
        return former_output
    char = ids_string[0]
    if char == "&":
        char = ids_string[:10]
        new_string = ids_string[10:]
    else:
        new_string = ids_string[1:]
    try:
        output = list(ids_dict[char])
    except KeyError:
        return recbreak(new_string, ids_dict, former_output + [char])
    if len(output) == 1:
        if output[0] == char:
            return recbreak(new_string, ids_dict, former_output + [char])
        else:
            return recbreak(new_string, ids_dict, former_output + recbreak(output[0], ids_dict))
    else:
        return recbreak(new_string, ids_dict, former_output + [[recbreak(ids, ids_dict) for ids in output]])

def flattree(ids_tree, output=[""]):
    if len(ids_tree) == 0:
        return set(output)
    input = ids_tree[0]
    if isinstance(input, str):
        new_output = [o + input for o in output]
    else:
        new_output = []
        for i in input:
            for inner_line in flattree(i):
                for o in output:
                    new_output.append(o + inner_line)
    return flattree(ids_tree[1:], new_output)

if __name__ == "__main__":
    base_ids_dict = {**parse_ids_file("ids-cdp.txt"), **parse_ids_file("ids-ext-cdef.txt")}
    ids_dict = {k: flattree(recbreak(k, base_ids_dict)) for k in normalize_ids_dict(base_ids_dict)}
    reverse_ids_dict = defaultdict(set)
    for k in ids_dict:
        for ids in ids_dict[k]:
            reverse_ids_dict[ids].add(k)