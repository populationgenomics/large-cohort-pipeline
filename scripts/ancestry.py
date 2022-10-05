#!/usr/bin/env python3

import logging

from larcoh import relatedness, ancestry_pca, ancestry_plots
from larcoh.utils import start_hail_context

logger = logging.getLogger(__file__)


def main():
    start_hail_context()
    relatedness.flag_related()
    ancestry_pca.run()
    ancestry_plots.run()


if __name__ == '__main__':
    main()
