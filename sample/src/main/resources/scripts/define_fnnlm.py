import sys
import mxnet as mx
import json

import argparse

parser = argparse.ArgumentParser(description='Initialize Feed-forward NNLM.')
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
parser.add_argument('--contextlen', metavar='N', type=int,
                    default=4,
                    help='context lengths')
parser.add_argument('--output', metavar='FILE', type=str,
                    help='output json')


args = parser.parse_args()

def sym_gen():
    # data: batchsize x contextlen
    data = mx.sym.Variable('data', dtype='int32')
    # label: batchsize
    label = mx.sym.Variable('softmax_label', dtype='int32')

    embed_weight = mx.sym.Variable('embed_weight', init=mx.init.Uniform(0.1))
    embed = mx.sym.Embedding(data=data, input_dim=args.vocabsize, weight=embed_weight,
                             output_dim=args.dimembed, name='embed')
    embed = mx.sym.Dropout(data=embed, p=(1.0 - args.dropout))

    input_fc = mx.sym.reshape(data=embed,
                              shape=(-1, args.contextlen * args.dimembed))

    input_fc = mx.sym.tanh(input_fc)

    hidden = mx.sym.relu(mx.sym.FullyConnected(data=input_fc, num_hidden=args.dimhidden))

    logit = mx.sym.FullyConnected(data=hidden, num_hidden=args.vocabsize)
    pred = mx.sym.SoftmaxOutput(data=logit, label=label, name='softmax')

    return pred

with open(args.output, 'w') as ofile:
    ofile.write(sym_gen().tojson()

