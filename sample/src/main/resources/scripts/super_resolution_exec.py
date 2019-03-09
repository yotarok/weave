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

parser.add_argument('--param', type=str,
                    help='parameter file')
parser.add_argument('--input', type=str,
                    help='input image')
parser.add_argument('--output', type=str,
                    help='output image')

opt = parser.parse_args()

print(opt)

upscale_factor = 2

ctx = mx.cpu()

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
            self.conv1 = nn.Conv2D(64, (5, 5), strides=(1, 1), padding=(2, 2))
            self.conv2 = nn.Conv2D(64, (3, 3), strides=(1, 1), padding=(1, 1))
            self.conv3 = nn.Conv2D(32, (3, 3), strides=(1, 1), padding=(1, 1))
            self.conv4 = nn.Conv2D(upscale_factor ** 2, (3, 3), strides=(1, 1), padding=(1, 1))
        self.upscale_factor = upscale_factor

    def forward(self, x):
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

from PIL import Image

net.load_params(opt.param, ctx=ctx)

img = Image.open(opt.input).convert('YCbCr')

y, cb, cr = img.split()
data = mx.nd.expand_dims(mx.nd.expand_dims(mx.nd.array(y), axis=0), axis=0)
out_img_y = mx.nd.reshape(net(data), shape=(-3, -2)).asnumpy()

out_img_y = out_img_y.clip(0, 255)
out_img_y = Image.fromarray(np.uint8(out_img_y[0]), mode='L')

out_img_cb = cb.resize(out_img_y.size, Image.BICUBIC)
out_img_cr = cr.resize(out_img_y.size, Image.BICUBIC)
out_img = Image.merge('YCbCr', [out_img_y, out_img_cb, out_img_cr]).convert('RGB')

out_img.save(opt.output)
