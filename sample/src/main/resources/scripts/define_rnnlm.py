import sys
import mxnet as mx
import json

import argparse

parser = argparse.ArgumentParser(description='Initialize RNNLM.')
parser.add_argument('--vocabsize', metavar='N', type=int,
                    required=True,
                    help='vocabulary size')
parser.add_argument('--dimembed', metavar='N', type=int,
                    default=64,
                    help='embedding dimensionality')
parser.add_argument('--dimhidden', metavar='N', type=int,
                    default=64,
                    help='LSTM cell dimensionality')
parser.add_argument('--nrnnlayers', metavar='N', type=int,
                    default=2,
                    help='number of LSTM layers')
parser.add_argument('--dropout', metavar='p', type=float,
                    default=0.5,
                    help='Retension rate')
parser.add_argument('--bucketlen', metavar='N1,N2,...', type=str,
                    default='10,20,40,80,160',
                    help='bucket lengths')
parser.add_argument('--output', metavar='FILE', type=str,
                    help='output json')


args = parser.parse_args()

def sym_gen(seq_len):
    data = mx.sym.Variable('data', dtype='int32')
    label = mx.sym.Variable('softmax_label', dtype='int32')
    initial_state = mx.sym.Variable('initial_state')

    embed_weight = mx.sym.Variable('embed_weight', init=mx.init.Uniform(0.1))
    embed = mx.sym.Embedding(data=data, input_dim=args.vocabsize, weight=embed_weight,
                             output_dim=args.dimembed, name='embed')
    embed = mx.sym.Dropout(data=embed, p=(1.0 - args.dropout))

    prev_raw = mx.sym.Variable("prev", shape=(0, 2 * args.nrnnlayers * args.dimhidden))
    prev = prev_raw.reshape((0, 2 * args.nrnnlayers, args.dimhidden))

    stack = mx.rnn.SequentialRNNCell()
    begin_state = []
    for i in range(args.nrnnlayers):
        lstm_cell = mx.rnn.LSTMCell(num_hidden=args.dimhidden, prefix='lstm_l{}_'.format(i))
        stack.add(lstm_cell)
        begin_state += [
            prev.slice_axis(axis=1, begin=i*2, end=i*2+1).flatten(),
            prev.slice_axis(axis=1, begin=i*2+1, end=i*2+2).flatten(),
        ]
        stack.add(mx.rnn.DropoutCell(1.0 - args.dropout))

    rnnoutput, states = stack.unroll(seq_len, inputs=embed,
                                     begin_state=begin_state,
                                     merge_outputs=True, layout='NTC')

    nextst = mx.sym.stack(*states, axis=1).reshape((-1, args.dimhidden * 4))

    rnnoutput = mx.sym.sigmoid(data=rnnoutput)
    rnnoutput = mx.sym.Reshape(rnnoutput, shape=(-1, args.dimhidden))
    label = mx.sym.Reshape(label, shape=(-1,))

    logit = mx.sym.FullyConnected(data=rnnoutput, num_hidden=args.vocabsize,
                                 name='pred')

    pred = mx.sym.SoftmaxOutput(data=logit, label=label, name='softmax',
                                use_ignore=True, normalization='batch',
                                ignore_label=-1)


    return pred, logit, nextst

bucketkeys = [int(s) for s in args.bucketlen.split(',')]
buckets = {}
for key in bucketkeys:
    sym_pred, _, _ = sym_gen(key)
    buckets[key] = json.loads(sym_pred.tojson())

deploy_pred, deploy_logit, deploy_next = sym_gen(1)
output = {
    "buckets": buckets,
    "step": json.loads(mx.sym.Group([deploy_logit, deploy_next, deploy_pred]).tojson())
}
json.dump(output, open(args.output, 'w'))

