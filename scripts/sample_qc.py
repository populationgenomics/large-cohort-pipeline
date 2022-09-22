#!/usr/bin/env python3

import logging

from larcoh import sample_qc, dense_subset
from larcoh.utils import start_hail_context

logger = logging.getLogger(__file__)


def main():
    start_hail_context()

    sample_qc.run()

    dense_subset.make_dense_subset()


if __name__ == '__main__':
    main()
