import sys
print("sys.path =", sys.path)
print("sys.executable =", sys.executable)

import mxnet as mx
import random
import argparse
import json
import logging
from rnnlm import AugmentedDataIterator, load_dataset, get_default_context


head = '%(asctime)-15s %(message)s'
logging.basicConfig(level=logging.DEBUG, format=head)

parser = argparse.ArgumentParser(description='Initialize RNNLM.')
parser.add_argument('--inputsym', metavar='FILE', type=str,
                    required=True,
                    help='vocabulary size')
parser.add_argument('--seed', metavar='N', type=int,
                    default=0,
                    help='random seed')
parser.add_argument('--output', metavar='FILE', type=str,
                    help='output file')

args = parser.parse_args()

random.seed(args.seed)
import numpy as np
np.random.seed(args.seed)

context = get_default_context()
syminfo = json.load(open(args.inputsym))

allsyms = dict()
for key, val in syminfo['buckets'].items():
    allsyms[int(key)] = mx.symbol.load_json(json.dumps(val))

def get_syms(key):
    return allsyms[key], ('data', 'prev'), ('softmax_label',)

batchsize = 1
bucketlen = min(allsyms.keys())
sym_pred, data_names, _ = get_syms(bucketlen)

mod = mx.mod.Module(sym_pred, data_names=data_names)

infered_shapes = dict(zip(sym_pred.list_arguments(), sym_pred.infer_shape_partial(data=(batchsize, bucketlen))[0]))
print(infered_shapes)
mod.bind([
    ('data', (batchsize, bucketlen)),
    ('prev', (batchsize, infered_shapes['prev'][1])),
],[
    ('softmax_label', (batchsize, bucketlen))
])
mod.init_params(initializer=mx.init.Xavier(factor_type="in"),
                force_init=True)
mod.save_params(args.output)
