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
parser.add_argument('--nz', type=int, default=100,
                    help='size of the latent z vector')
parser.add_argument('--ngf', type=int, default=64)
parser.add_argument('--ndf', type=int, default=64)
parser.add_argument('--ensembledir', type=str,
                    help="ensembledir")
parser.add_argument('--use_lse', default=True, action='store_true',
                    help="Use LSE")
parser.add_argument('--batch-size', type=int, default=16,
                    help='input batch size')
parser.add_argument('--withoutDbn', default=False, action='store_true',
                    help="Remove batchnorm from D")

opt = parser.parse_args()
print(opt)

paths_D = []
paths_G = []
for l in os.listdir(opt.ensembledir):
    l = l.strip()
    if l.startswith("discriminator_"):
        paths_D.append(os.path.join(opt.ensembledir, l))
    elif l.startswith("generator_"):
        paths_G.append(os.path.join(opt.ensembledir, l))
mx.random.seed(int(time.time()))
ctx = mx.cpu()

nc = 3
nz = int(opt.nz)
ngf = int(opt.ngf)
ndf = int(opt.ndf)

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

netD = nn.Sequential()
with netD.name_scope():
    # input is (nc) x 128 x 128
    netD.add(nn.Conv2D(ndf, 4, 2, 1, use_bias=False))
    netD.add(nn.LeakyReLU(0.2))
    # input is (nc) x 64 x 64
    netD.add(nn.Conv2D(ndf, 4, 2, 1, use_bias=False))
    if not opt.withoutDbn:
        netD.add(nn.BatchNorm())
    netD.add(nn.LeakyReLU(0.2))
    # state size. (ndf) x 32 x 32
    netD.add(nn.Conv2D(ndf * 2, 4, 2, 1, use_bias=False))

    if not opt.withoutDbn:
        netD.add(nn.BatchNorm())
    netD.add(nn.LeakyReLU(0.2))
    # state size. (ndf) x 16 x 16
    netD.add(nn.Conv2D(ndf * 4, 4, 2, 1, use_bias=False))
    if not opt.withoutDbn:
        netD.add(nn.BatchNorm())
    netD.add(nn.LeakyReLU(0.2))
    # state size. (ndf) x 8 x 8
    netD.add(nn.Conv2D(ndf * 8, 4, 2, 1, use_bias=False))
    if not opt.withoutDbn:
        netD.add(nn.BatchNorm())
    netD.add(nn.LeakyReLU(0.2))
    # state size. (ndf) x 4 x 4
    if opt.use_lse:
        netD.add(nn.Conv2D(1, 4, 1, 0, use_bias=False))
    else:
        netD.add(nn.Conv2D(2, 4, 1, 0, use_bias=False))



print("Generate")

Gouts = []

for path_G in paths_G:
    netG.load_params(path_G, ctx=ctx)
    noise = mx.nd.random.normal(0, 1, shape=(opt.batch_size, nz, 1, 1), ctx=ctx)
    Gouts.append(netG(noise))

print("Evaluate")

sum_per_sample = np.zeros((len(paths_G), opt.batch_size))
for path_D in paths_D:
    print("Critique from " + path_D)
    netD.load_params(path_D, ctx=ctx)
    for i, (path_G, output) in enumerate(zip(paths_G, Gouts)):
        print(" - Evaluation of output from " + path_G)
        score = netD(output).reshape((opt.batch_size,))
        score = score.asnumpy()
        print(score)
        #sum_per_sample[i, :] -= (score - 1.0) ** 2
        sum_per_sample[i, :] += score

print (sum_per_sample)
idx = sum_per_sample.argmax()
idx = (idx // opt.batch_size, idx % opt.batch_size)
print(idx)
print(Gouts[idx[0]].shape)

bestGout = Gouts[idx[0]]
bestout = bestGout[int(idx[1])]

bestout = ((bestout.asnumpy() + 1.0) * 128.0).astype('uint8')
bestout = bestout.transpose((1, 2, 0))
from PIL import Image
image = Image.fromarray(bestout, mode='RGB')
image.save("best.png")



