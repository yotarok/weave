import argparse
import math
import os
import numpy as np
import random
import sys

import mxnet as mx
import mxnet.ndarray as F
from mxnet import gluon
from mxnet.gluon import nn
from mxnet import autograd as ag
from mxnet.test_utils import download
from mxnet.image import RandomCropAug, ResizeAug
from mxnet.io import PrefetchingIter


parser = argparse.ArgumentParser(description='Super-resolution using an efficient sub-pixel convolution neural network.')

parser.add_argument('--datasetdir', type=str,
                    help='dataset directory')
parser.add_argument('--batch-size', type=int, default=4,
                    help='training batch size, per device. default is 4.')
parser.add_argument('--test-batch-size', type=int, default=10,
                    help='test batch size')
parser.add_argument('--epochs', type=int, default=300,
                    help='number of training epochs')
parser.add_argument('--lr', type=float, default=0.001,
                    help='learning Rate. default is 0.001.')
parser.add_argument('--seed', type=int, default=123,
                    help='random seed to use. Default=123')

opt = parser.parse_args()

print(opt)

upscale_factor = 2
batch_size, test_batch_size = opt.batch_size, opt.test_batch_size
color_flag = 0


class ImagePairIter(mx.io.DataIter):
    def __init__(self, filelist, data_shape, label_shape, batch_size=64, common_aug=None, input_aug=None, target_aug=None):
        super(ImagePairIter, self).__init__(batch_size)

        self.data_shape = (batch_size,) + data_shape
        self.label_shape = (batch_size,) + label_shape
        self.common_aug = common_aug
        self.input_aug = input_aug
        self.target_aug = target_aug
        self.provide_data = [('data', self.data_shape)]
        self.provide_label = [('label', self.label_shape)]
        self.filenames = filelist
        self.count = 0
        random.shuffle(self.filenames)

    def next(self):
        from PIL import Image
        if self.count + self.batch_size <= len(self.filenames):
            data = []
            label = []
            for i in range(self.batch_size):
                fn = self.filenames[self.count]
                self.count += 1
                image = Image.open(fn).convert('YCbCr').split()[0]
                if image.size[0] > image.size[1]:
                    image = image.transpose(Image.TRANSPOSE)
                image = mx.nd.expand_dims(mx.nd.array(image), axis=2)

                for aug in self.common_aug:
                    image = aug(image)

                target = image.copy()
                for aug in self.input_aug:
                    image = aug(image)

                for aug in self.target_aug:
                    target = aug(target)

                data.append(image)
                label.append(target)

            data = mx.nd.concat(*[mx.nd.expand_dims(d, axis=0) for d in data], dim=0)
            label = mx.nd.concat(*[mx.nd.expand_dims(d, axis=0) for d in label], dim=0)
            data = [mx.nd.transpose(data, axes=(0, 3, 1, 2)).astype('float32')/255]
            label = [mx.nd.transpose(label, axes=(0, 3, 1, 2)).astype('float32')/255]

            return mx.io.DataBatch(data=data, label=label)
        else:
            raise StopIteration

    def reset(self):
        self.count = 0
        random.shuffle(self.filenames)

def get_dataset(datadir):
    filelistpath = os.path.join(datadir, "filelist.txt")
    valid_files = []
    train_files = []
    for i, l in enumerate(open(filelistpath)):
        l = l.strip()
        if i % 10 == 0:
            valid_files.append(os.path.join(datadir, l))
        else:
            train_files.append(os.path.join(datadir, l))

    crop_size = 256
    input_crop_size = crop_size // upscale_factor

    common_transform = [RandomCropAug((crop_size, crop_size)),
    ]
    input_transform = [ResizeAug(input_crop_size)]
    target_transform = []

    iters = (ImagePairIter(train_files,
                           (input_crop_size, input_crop_size),
                           (crop_size, crop_size),
                           batch_size,
                           common_transform, input_transform, target_transform),
             ImagePairIter(valid_files,
                           (input_crop_size, input_crop_size),
                           (crop_size, crop_size),
                           test_batch_size,
                           common_transform, input_transform, target_transform))

    print ("#Train={}, #Valid={}".format(len(train_files), len(valid_files)))

    return iters

train_data, val_data = get_dataset(opt.datasetdir)

mx.random.seed(opt.seed)


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

ctx = get_default_context()

# define model
def _rearrange(raw, F, upscale_factor):
    # (N, C * r^2, H, W) -> (N, C, r^2, H, W)
    splitted = F.reshape(raw, shape=(0, -4, -1, upscale_factor**2, 0, 0))
    # (N, C, r^2, H, W) -> (N, C, r, r, H, W)
    unflatten = F.reshape(splitted, shape=(0, 0, -4, upscale_factor, upscale_factor, 0, 0))
    # (N, C, r, r, H, W) -> (N, C, H, r, W, r)
    swapped = F.transpose(unflatten, axes=(0, 1, 4, 2, 5, 3))
    # (N, C, H, r, W, r) -> (N, C, H*r, W*r)
    return F.reshape(swapped, shape=(0, 0, -3, -3))


class SuperResolutionNet(gluon.Block):
    def __init__(self, upscale_factor):
        super(SuperResolutionNet, self).__init__()
        with self.name_scope():
            self.input_dropout = nn.Dropout(0.5)
            self.conv1 = nn.Conv2D(64, (5, 5), strides=(1, 1), padding=(2, 2))
            self.conv2 = nn.Conv2D(64, (3, 3), strides=(1, 1), padding=(1, 1))
            self.conv3 = nn.Conv2D(32, (3, 3), strides=(1, 1), padding=(1, 1))
            self.conv4 = nn.Conv2D(upscale_factor ** 2, (3, 3), strides=(1, 1), padding=(1, 1))
        self.upscale_factor = upscale_factor

    def forward(self, x):
        x = self.input_dropout(x)
        x = F.Activation(self.conv1(x), act_type='relu')
        x = F.Activation(self.conv2(x), act_type='relu')
        x = F.Activation(self.conv3(x), act_type='relu')
        return _rearrange(self.conv4(x), F, self.upscale_factor)

net = SuperResolutionNet(upscale_factor)
metric = mx.metric.MSE()

def test(ctx):
    val_data.reset()
    avg_psnr = 0
    batches = 0
    for batch in val_data:
        batches += 1
        data = gluon.utils.split_and_load(batch.data[0], ctx_list=ctx, batch_axis=0)
        label = gluon.utils.split_and_load(batch.label[0], ctx_list=ctx, batch_axis=0)
        outputs = []
        for x in data:
            outputs.append(net(x))
        metric.update(label, outputs)
        avg_psnr += 10 * math.log10(1/metric.get()[1])
        metric.reset()
    avg_psnr /= batches
    print('validation avg psnr: %f'%avg_psnr)


def train(epoch, ctx):
    if isinstance(ctx, mx.Context):
        ctx = [ctx]
    net.initialize(mx.init.Orthogonal(), ctx=ctx)
    # re-initialize conv4's weight to be Orthogonal
    #net.conv4.initialize(mx.init.Orthogonal(scale=1), ctx=ctx, force_reinit=True)
    trainer = gluon.Trainer(net.collect_params(), 'adam',
                            {'learning_rate': opt.lr})
    loss = gluon.loss.L2Loss()
    test(ctx)

    for i in range(epoch):
        train_data.reset()
        nimg = 0
        for batch in train_data:
            data = gluon.utils.split_and_load(batch.data[0], ctx_list=ctx,
                                              batch_axis=0)
            label = gluon.utils.split_and_load(batch.label[0], ctx_list=ctx,
                                               batch_axis=0)
            outputs = []
            with ag.record():
                for x, y in zip(data, label):
                    z = net(x)
                    L = loss(z, y)
                    L.backward()
                    outputs.append(z)
            trainer.step(batch.data[0].shape[0])
            metric.update(label, outputs)
            name, acc = metric.get()
            print('[%d] %s=%f'%(nimg, name, acc))
            nimg += batch_size

        name, acc = metric.get()
        metric.reset()
        print('training mse at epoch %d: %s=%f'%(i, name, acc))
        test(ctx)

        net.save_params('superres.{}.params'.format(i))
    net.save_params('superres.params')

train(opt.epochs, ctx)
