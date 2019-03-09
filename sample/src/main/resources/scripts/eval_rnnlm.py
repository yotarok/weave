import mxnet as mx
import random
import argparse
import json
import sys
from rnnlm import AugmentedDataIterator, load_dataset, get_default_context

import random
random.seed(0)
import numpy as np
np.random.seed(0)

parser = argparse.ArgumentParser(description='Initialize RNNLM.')
parser.add_argument('--inputsym', metavar='FILE', type=str,
                    required=True,
                    help='vocabulary size')
parser.add_argument('--inputparam', metavar='FILE', type=str,
                    required=True,
                    help='vocabulary size')
parser.add_argument('--evaldata', metavar='FILE', type=str,
                    required=True,
                    help='eval data')
parser.add_argument('--contextlen', metavar='N', type=int,
                    default=1,
                    help='contextlength')

args = parser.parse_args()

data, maxlen = load_dataset(args.evaldata, contextlen=args.contextlen)

context = get_default_context()

syminfo = json.load(open(args.inputsym))
allsyms = dict()
for key, val in syminfo['buckets'].items():
    allsyms[int(key)] = mx.symbol.load_json(json.dumps(val))

default_key = max(allsyms.keys())

def get_syms(key):
    return allsyms[key], ('data', 'prev'), ('softmax_label',)

# find unused bucket keys
all_buckets = sorted(allsyms.keys())
used_buckets = set()
import bisect
for sent in data:
    buck = bisect.bisect_left(all_buckets, len(sent))
    if buck == len(all_buckets):
        pass
    else:
        used_buckets.add(all_buckets[buck])
used_buckets = sorted(list(used_buckets))
print("Used buckets = " + str(used_buckets))

batchsize = 10
dataiter = mx.rnn.BucketSentenceIter(
    data, batch_size=batchsize,
    buckets=used_buckets,
    invalid_label=-1, layout='NT')

default_key = max(allsyms.keys())
default_sym, _, _ = get_syms(default_key)
infered_shapes = dict(zip(default_sym.list_arguments(),
                          default_sym.infer_shape_partial(data=(batchsize, default_key))[0]))
stateshape = (batchsize, infered_shapes['prev'][1])

module = mx.mod.BucketingModule(get_syms, default_bucket_key=default_key, context=context)

module.bind([
    ('data', (batchsize, default_key)),
    ('prev', stateshape)
],[
    ('softmax_label', (batchsize, default_key))
])

dataiter = AugmentedDataIterator(
    dataiter,
    [mx.io.DataDesc(name='prev', shape=stateshape, dtype='float32')],
    lambda x: [mx.nd.zeros(stateshape)])

module.load_params(args.inputparam)
metric = mx.metric.Perplexity(-1)

result = module.score(dataiter, metric)
result = dict(result)
print("Perplexity: {}".format(result['perplexity']))
print("Perplexity: {}".format(result['perplexity']), file=sys.stderr)
