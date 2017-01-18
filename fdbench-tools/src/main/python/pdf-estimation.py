#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys

from scipy.stats.kde import gaussian_kde
from numpy import linspace
import numpy as np
from matplotlib import pyplot as plt
from scipy.interpolate import UnivariateSpline


data = []

print 'input file: ', sys.argv[1]
with open(sys.argv[1]) as f:
    for line in f:
        data.append(len(line))

print 'samples:', len(data), 'max: ', max(data), 'min: ', min(data), 'median: ', np.median(data), 'mean: ', np.mean(data), 'stddev:', np.std(data)
# this create the kernel, given an array it will estimate the probability over that values
kde = gaussian_kde(data)
# these are the values over wich your kernel will be evaluated
dist_space = linspace(min(data), max(data), 100)
# plot the results
plt.plot(dist_space, kde(dist_space))
plt.show()
# n = len(data)/100
# p, x = np.histogram(data, bins=n) # bin it into n = N/10 bins
# x = x[:-1] + (x[1] - x[0])/2   # convert bin edges to centers
# f = UnivariateSpline(x, p, s=n)
# plt.plot(x, f(x))
# plt.show()