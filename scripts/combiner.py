#!/usr/bin/env python

"""
Combine a set of GVCFs into a VDS.
"""

from larcoh import combiner
from larcoh.utils import start_hail_context


def main():
    start_hail_context()

    combiner.combine()


if __name__ == '__main__':
    main()
