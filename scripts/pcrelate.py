#!/usr/bin/env python

"""
Script to run pcrelate. In a separate script because it needs a non-preemptible cluster.
"""

from larcoh import relatedness
from larcoh.utils import start_hail_context


def main():
    start_hail_context()

    relatedness.pcrelate()
