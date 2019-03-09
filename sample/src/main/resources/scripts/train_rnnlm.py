import sys
print("sys.path =", sys.path)
print("sys.executable =", sys.executable)

import mxnet as mx
import random
import argparse
import json
import logging
import time
from rnnlm import AugmentedDataIterator, load_dataset, get_default_context


head = '%(asctime)-15s %(message)s'
logging.basicConfig(level=logging.DEBUG, format=head)

parser = argparse.ArgumentParser(description='Initialize RNNLM.')
parser.add_argument('--inputsym', metavar='FILE', type=str,
                    required=True,
                    help='vocabulary size')
parser.add_argument('--inputparam', metavar='FILE', type=str,
                    required=True,
                    help='vocabulary size')
parser.add_argument('--traindata', metavar='FILE', type=str,
                    required=True,
                    help='training data')
parser.add_argument('--seed', metavar='N', type=int,
                    default=0,
                    help='random seed')
parser.add_argument('--optimizer', metavar='NAME', type=str,
                    default="adam",
                    help='optimizer name')
parser.add_argument('--contextlen', metavar='N', type=int,
                    default=1,
                    help='contextlength')
parser.add_argument('--batchsize', metavar='N', type=int,
                    default=100,
                    help='batchsize')
parser.add_argument('--learnrate', metavar='R', type=float,
                    default=0.01,
                    help='learning rate')
parser.add_argument('--output', metavar='FILE', type=str,
                    help='output file')
parser.add_argument('--clip', type=float, default=5.0,
                    help='clipping')

args = parser.parse_args()

random.seed(args.seed)
import numpy as np
np.random.seed(args.seed)


data, maxlen = load_dataset(args.traindata, contextlen=args.contextlen)


context = get_default_context()
syminfo = json.load(open(args.inputsym))

allsyms = dict()
for key, val in syminfo['buckets'].items():
    allsyms[int(key)] = mx.symbol.load_json(json.dumps(val))

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


batchsize = args.batchsize
dataiter = mx.rnn.BucketSentenceIter(
    data, batch_size=batchsize,
    buckets=used_buckets,
    invalid_label=-1, layout='NT')

default_key = max(used_buckets)
default_sym, _, _ = get_syms(default_key)
infered_shapes = dict(zip(default_sym.list_arguments(),
                          default_sym.infer_shape_partial(data=(batchsize, default_key))[0]))
stateshape = (batchsize, infered_shapes['prev'][1])

module = mx.mod.BucketingModule(get_syms, default_bucket_key=default_key, context=context)

opt_params = {
    'learning_rate': args.learnrate,
    'rescale_grad': 1.0 / args.batchsize,
}

metric = mx.metric.Perplexity(-1)

module.bind([
    ('data', (batchsize, default_key)),
    ('prev', stateshape)
],[
    ('softmax_label', (batchsize, default_key))
])
module.load_params(args.inputparam)

optimizer = mx.optimizer.create(
    args.optimizer,
    sym=default_sym,
    **opt_params)

module.init_optimizer(optimizer=optimizer)

dataiter = AugmentedDataIterator(
    dataiter,
    [mx.io.DataDesc(name='prev', shape=stateshape, dtype='float32')],
    lambda x: [mx.nd.zeros(stateshape)])

#module.fit(
#    train_data=dataiter,
#    optimizer=args.optimizer,
#    eval_metric=metric,
#    optimizer_params=opt_params,
#    num_epoch=1,
#    batch_end_callback=mx.callback.Speedometer(batchsize, 100, auto_reset=True))

speedometer = mx.callback.Speedometer(batchsize, 10, auto_reset=True)

logging.info("Training started ... ")

total_loss = 0.0
nbatch = 0
tic = time.time()
for batch in dataiter:
    module.forward(batch)
    module.backward()
    length = batch.data[0].shape[1]

    grad_array = []
    for grad in module._curr_module._exec_group.grad_arrays:
        grad_array += grad
    mx.gluon.utils.clip_global_norm(grad_array, args.clip * length)

    #optimizer.clip_gradient = args.clip * length * batchsize
    module.update()

    module.update_metric(metric, batch.label)
    batch_end_params = mx.model.BatchEndParam(epoch=0,
                                              nbatch=nbatch,
                                              eval_metric=metric,
                                              locals=locals())
    speedometer(batch_end_params)
    nbatch += 1

#for name, val in metric.get_name_value():
#    logging.info('Epoch[%d] Train-%s=%f', 0, name, val)
toc = time.time()
logging.info('Epoch[%d] Time cost=%.3f', 0, (toc-tic))


module.save_params(args.output)
