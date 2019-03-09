import argparse

import mxnet as mx
from mxnet import gluon
from mxnet.gluon import nn
from mxnet import autograd
import numpy as np
import logging
import os.path
import sys
import datetime
import time

parser = argparse.ArgumentParser()
parser.add_argument('--nsample', type=int, default=10,
                    help='input batch size')
parser.add_argument('--nz', type=int, default=100,
                    help='size of the latent z vector')
parser.add_argument('--ngf', type=int, default=64)
parser.add_argument('--netG', default='',
                    help="path to netG (to continue training)")
parser.add_argument('--outprefix', default='./gen_',
                    help='folder to output images and model checkpoints')

opt = parser.parse_args()
print(opt)

logging.basicConfig(level=logging.DEBUG)
nz = int(opt.nz)
ngf = int(opt.ngf)
nc = 3

ctx = mx.cpu()

netG = nn.Sequential()
with netG.name_scope():
    # input is Z, going into a convolution
    netG.add(nn.Conv2DTranspose(ngf * 8, 4, 1, 0, use_bias=False))
    netG.add(nn.BatchNorm())
    netG.add(nn.LeakyReLU(0.2))
    # state size. (ngf*8) x 4 x 4
    netG.add(nn.Conv2DTranspose(ngf * 4, 4, 2, 1, use_bias=False))
    netG.add(nn.BatchNorm())
    netG.add(nn.LeakyReLU(0.2))
    # state size. (ngf*8) x 8 x 8
    netG.add(nn.Conv2DTranspose(ngf * 2, 4, 2, 1, use_bias=False))
    netG.add(nn.BatchNorm())
    netG.add(nn.LeakyReLU(0.2))
    # state size. (ngf*8) x 16 x 16
    netG.add(nn.Conv2DTranspose(ngf, 4, 2, 1, use_bias=False))
    netG.add(nn.BatchNorm())
    netG.add(nn.LeakyReLU(0.2))
    # state size. (ngf*8) x 32 x 32
    netG.add(nn.Conv2DTranspose(ngf, 4, 2, 1, use_bias=False))
    netG.add(nn.BatchNorm())
    netG.add(nn.LeakyReLU(0.2))
    # state size. (ngf*8) x 64 x 64
    netG.add(nn.Conv2DTranspose(nc, 4, 2, 1, use_bias=True))
    netG.add(nn.Activation('tanh'))
    # state size. (nc) x 128 x 128

netG.load_params(opt.netG, ctx=ctx)

noise = mx.nd.random.normal(0, 1, shape=(opt.nsample, nz, 1, 1), ctx=ctx)

output = netG(noise)
output = ((output + 1.0) * 128.0).astype('uint8')

print(output.shape)
output = output.transpose((0, 2, 3, 1))

from PIL import Image

for j in range(output.shape[0]):
    image = Image.fromarray(output[j].asnumpy(), mode='RGB')
    image.save(opt.outprefix + ".{}.jpg".format(j))

'''
for j in range(output.shape[0]):
    image = Image.new('RGB', (128, 128))

    for y in range(128):
        for x in range(128):
            pix = tuple(x.asscalar() for x in output[j, x, y, :])
            image.putpixel((x, y), pix)
    image.save(opt.outprefix + ".{}.jpg".format(j))
'''
