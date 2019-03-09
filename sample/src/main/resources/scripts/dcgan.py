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
parser.add_argument('--datasetdir', type=str,
                    help='dataset directory')
parser.add_argument('--batch-size', type=int, default=64,
                    help='input batch size')
parser.add_argument('--nz', type=int, default=100,
                    help='size of the latent z vector')
parser.add_argument('--ngf', type=int, default=64)
parser.add_argument('--ndf', type=int, default=64)
parser.add_argument('--nepoch', type=int, default=25,
                    help='number of epochs to train for')
parser.add_argument('--lr', type=float, default=0.0002,
                    help='learning rate, default=0.0002')
parser.add_argument('--beta1', type=float, default=0.5, help='beta1 for adam. default=0.5')
parser.add_argument('--netG', default='',
                    help="path to netG (to continue training)")
parser.add_argument('--netD', default='',
                    help="path to netD (to continue training)")
parser.add_argument('--outf', default='./results',
                    help='folder to output images and model checkpoints')
parser.add_argument('--check-point', default=True,
                    help="save results at each epoch or not")
parser.add_argument('--use_lse', default=False, action='store_true',
                    help="Use LSE")
parser.add_argument('--use_glorot', default=False, action='store_true',
                    help="Use Glorot")
parser.add_argument('--withoutDbn', default=False, action='store_true',
                    help="Remove batchnorm from D")

opt = parser.parse_args()
print(opt)

logging.basicConfig(level=logging.DEBUG)
nz = int(opt.nz)
ngf = int(opt.ngf)
ndf = int(opt.ndf)
nc = 3
outf = opt.outf

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

def crop_rotate_reverse(im):
    # crop
    hei, wid, nch = im.shape
    assert hei == 128 or wid == 128
    assert hei >= 128 and wid >= 128 and nch == 3

    if hei > 128:
        yoff = np.random.randint(0, hei - 128)
        im = im[yoff:yoff + 128, :, :]
    elif wid > 128:
        xoff = np.random.randint(0, wid - 128)
        im = im[:, xoff:xoff + 128, :]

    assert im.shape == (128, 128, 3)

    '''
    if np.random.rand() < 0.5:
        # X-reverse
        im = im[:, ::-1, :]
    if np.random.rand() < 0.5:
        # Y-reverse
        im = im[::-1, :, :]
    if np.random.rand() < 0.5:
        # transpose
        im = im.swapaxes(0, 1)
    '''
    r = np.random.rand()
    if r < 0.25:
        pass
    elif r < 0.5:
        # 90deg-rotate = transpose * Yrev
        im = im.swapaxes(0, 1)
        im = im[::-1, :, :]
    elif r < 0.75:
        # 180deg-rotate = Xrev * Yrev
        im = im[::-1, :, :]
        im = im[:, ::-1, :]
    else:
        # 270deg-rotate = transpose * Xrev
        im = im.swapaxes(0, 1)
        im = im[:, ::-1, :]

    im = (im.astype(np.float32) - 128.0) / 128.0

    # HxWxC => CxWxH
    im = im.transpose((2, 0, 1))
    return im

def fill_buf(buf, i, img, shape):
    n = buf.shape[0]//shape[1]
    m = buf.shape[1]//shape[0]

    sx = (i%m)*shape[0]
    sy = (i//m)*shape[1]
    buf[sy:sy+shape[1], sx:sx+shape[0], :] = img

def visual(title, X, name):
    import matplotlib as mpl
    mpl.use('Agg')
    from matplotlib import pyplot as plt

    assert len(X.shape) == 4
    X = X.transpose((0, 2, 3, 1))
    X = np.clip((X - np.min(X))*(255.0/(np.max(X) - np.min(X))), 0, 255).astype(np.uint8)
    n = np.ceil(np.sqrt(X.shape[0]))
    buff = np.zeros((int(n*X.shape[1]), int(n*X.shape[2]), int(X.shape[3])), dtype=np.uint8)
    for i, img in enumerate(X):
        fill_buf(buff, i, img, X.shape[1:3])
    buff = buff[:,:,::-1]
    plt.imshow(buff)
    plt.title(title)
    plt.savefig(name)

train_imgs = []
val_imgs = []

for idx, l in enumerate(open(os.path.join(opt.datasetdir, 'filelist.txt'))):
    filepath = os.path.join(opt.datasetdir, l.strip())

    str_image = None
    with open(filepath, 'rb') as fp:
        str_image = fp.read()
    image = mx.img.imdecode(str_image)
    if idx % 10 == 0:
        val_imgs.append(image)
    else:
        train_imgs.append(image)

train_data = gluon.data.DataLoader(
    gluon.data.ArrayDataset(train_imgs).transform(crop_rotate_reverse, lazy=True),
    batch_size=opt.batch_size, shuffle=True, last_batch='discard')

val_data = gluon.data.DataLoader(
    gluon.data.ArrayDataset(val_imgs).transform(crop_rotate_reverse),
    batch_size=opt.batch_size, shuffle=False)

ctx = get_default_context()

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

# loss
if opt.use_lse:
    loss = gluon.loss.L2Loss()
else:
    loss = gluon.loss.SoftmaxCrossEntropyLoss()

# initialize the generator and the discriminator
if len(opt.netG) > 0:
    netG.load_params(opt.netG, ctx=ctx)
else:
    if opt.use_glorot:
        netG.initialize(mx.init.Xavier(), ctx=ctx)
    else:
        netG.initialize(mx.init.Normal(0.02), ctx=ctx)

if len(opt.netD) > 0:
    netD.load_params(opt.netD, ctx=ctx)
else:
    if opt.use_glorot:
        netD.initialize(mx.init.Xavier(), ctx=ctx)
    else:
        netD.initialize(mx.init.Normal(0.02), ctx=ctx)

# trainer for the generator and the discriminator
trainerG = gluon.Trainer(netG.collect_params(),
                         'adam', {'learning_rate': opt.lr,
                                  'beta1': opt.beta1})
trainerD = gluon.Trainer(netD.collect_params(),
                         'adam', {'learning_rate': opt.lr,
                                  'beta1': opt.beta1})

if opt.use_lse:
    real_label = 1.0 * mx.nd.ones((opt.batch_size,), ctx=ctx)
    fake_label = -1.0 * mx.nd.ones((opt.batch_size,), ctx=ctx)
else:
    real_label = mx.nd.ones((opt.batch_size,), ctx=ctx)
    fake_label = mx.nd.zeros((opt.batch_size,), ctx=ctx)

metric = mx.metric.Accuracy()
print('Training... ')

iter = 0
for epoch in range(opt.nepoch):
    tic = time.time()
    btic = time.time()
    for data in train_data:
        ############################
        # (1) Update D network: maximize log(D(x)) + log(1 - D(G(z)))
        ###########################
        # train with real_t
        data = data.as_in_context(ctx)
        noise = mx.nd.random.normal(0, 1, shape=(opt.batch_size, nz, 1, 1), ctx=ctx)

        with autograd.record():
            output = netD(data)
            if opt.use_lse:
                pass
            else:
                output = output.reshape((opt.batch_size, 2))
            errD_real = loss(output, real_label)
            metric.update([real_label,], [output,])

            fake = netG(noise)
            output = netD(fake.detach())
            if opt.use_lse:
                pass
            else:
                output = output.reshape((opt.batch_size, 2))
            errD_fake = loss(output, fake_label)
            errD = errD_real + errD_fake
            errD.backward()
            metric.update([fake_label,], [output,])

        trainerD.step(opt.batch_size)

        ############################
        # (2) Update G network: maximize log(D(G(z)))
        ###########################
        with autograd.record():
            output = netD(fake)
            if opt.use_lse:
                pass
            else:
                output = output.reshape((-1, 2))
            errG = loss(output, real_label)
            errG.backward()

        trainerG.step(opt.batch_size)

        name, acc = metric.get()
        # logging.info('speed: {} samples/s'.format(opt.batch_size / (time.time() - btic)))
        logging.info('discriminator loss = %f, generator loss = %f, binary training acc = %f at iter %d epoch %d' %(mx.nd.mean(errD).asscalar(), mx.nd.mean(errG).asscalar(), acc, iter, epoch))
        if iter % 100 == 0:
            visual('gout', fake.asnumpy(), name=os.path.join(outf,'fake_img_iter_%d.png' %iter))
            visual('data', data.asnumpy(), name=os.path.join(outf,'real_img_iter_%d.png' %iter))

        iter = iter + 1
        btic = time.time()

    name, acc = metric.get()
    metric.reset()
    logging.info('\nbinary training acc at epoch %d: %s=%f' % (epoch, name, acc))
    logging.info('time: %f' % (time.time() - tic))

    if epoch % 100 == 0:
        netG.save_params(os.path.join(outf,'generator_epoch_%d.params' % epoch))
        netD.save_params(os.path.join(outf,'discriminator_epoch_%d.params' % epoch))

netG.save_params(os.path.join(outf,'generator_epoch_%d.params' % epoch))
netD.save_params(os.path.join(outf,'discriminator_epoch_%d.params' % epoch))

