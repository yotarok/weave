import numpy as np
import mxnet as mx
#from mxnet import gluon, autograd, nd
#from mxnet.gluon import nn, rnn
import sys

class AugmentedDataIterator(mx.io.DataIter):
    def __init__(self, baseiter, datadescs, func):
        super(AugmentedDataIterator, self).__init__()
        self.func = func
        self.baseiter = baseiter
        self.datadescs = datadescs

    def next(self):
        basebatch = self.baseiter.next()
        augdata = self.func(basebatch)

        #print(basebatch.data[0].astype('int32').asnumpy()[0,::])
        #print(basebatch.label[0].astype('int32').asnumpy()[0,::])

        data = basebatch.data + list(augdata)
        #print(mx.io.DataBatch(data=data, label=basebatch.label))
        return mx.io.DataBatch(
            bucket_key=basebatch.bucket_key,
            data=data,
            label=basebatch.label,
            provide_data=basebatch.provide_data + list(self.datadescs),
            provide_label=basebatch.provide_label)

    def reset(self):
        self.baseiter.reset()

    @property
    def provide_data(self):
        return self.baseiter.provide_data + self.datadescs

    @property
    def provide_label(self):
        return self.baseiter.provide_label


def load_dataset(path, sentence_start=0, sentence_end=0, contextlen=1):
    ret = []
    maxlen = 0
    it = iter(open(path))
    while True:
        sent = [sentence_start]
        try:
            for n in range(contextlen):
                seq = [int(e) for e in next(it).strip().split()]
                sent += seq + [sentence_end]
        except StopIteration:
            if len(sent) > 1:
                ret.append(sent)
                maxlen = max(maxlen, len(sent))
            break
        ret.append(sent)
        maxlen = max(maxlen, len(sent))

    return ret, maxlen

_GPU_AVAILABLE = None
def is_gpu_available():
    global _GPU_AVAILABLE
    if _GPU_AVAILABLE is None:
        print(" ==== TESTING GPU AVAILABILITY (ignore error below) ====", file=sys.stderr)
        try:
            mx.nd.ones(1).as_in_context(mx.gpu())
            context = mx.gpu()
            print("GPU is used", file=sys.stderr)
            _GPU_AVAILABLE = True
        except mx.MXNetError:
            print("CPU is used", file=sys.stderr)
            _GPU_AVAILABLE = False
        print(" =======================================================", file=sys.stderr)
    return _GPU_AVAILABLE

def get_default_context():
    return mx.gpu() if is_gpu_available() else mx.cpu()
