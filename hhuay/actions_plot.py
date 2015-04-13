from __future__ import unicode_literals

import os
import warnings

from .util import (
    options,
    read_data,
    ROOT_DIR,
)

IMAGE_DIR = os.path.join(ROOT_DIR, 'output', 'images')


def _plot_bar_histogram(plt, fig, ax, data):
    xvalues, yvalues = zip(*data['data'].items())
    ax.bar(xvalues, yvalues)


def plot(name, type, title=None, xlabel=None, ylabel=None):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        import matplotlib

    matplotlib.rcParams['backend'] = 'svg'
    import matplotlib.pyplot as plt

    data = read_data(name)

    fig = plt.figure()
    ax = fig.add_subplot(111)

    plotf = globals()['_plot_' + type]
    plotf(plt, fig, ax, data)

    if title is not None:
        plt.title(title)
    if xlabel is not None:
        plt.xlabel(xlabel)
    if ylabel is not None:
        plt.ylabel(ylabel)

    svg_fn = os.path.join(IMAGE_DIR, name + '.svg')
    fig.savefig(svg_fn, format='svg')


@options()
def action_plotall(args, config, db, wdb):
    if not os.path.exists(IMAGE_DIR):
        os.mkdir(IMAGE_DIR)

    plot('user_session_counts_reverse', 'bar_histogram', **{
        'title': 'Histogram of session count',
        'ylabel': 'Number of users with session count',
        'xlabel': 'Session count',
    })

