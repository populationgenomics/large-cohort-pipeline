#!/usr/bin/env python

"""
Combine a set of GVCFs into a VDS.
"""

from larcoh import combiner, parameters
from larcoh.utils import start_hail_context


start_hail_context()

combiner.run(
    out_vds_path=parameters.vds_path,
    tmp_prefix=parameters.tmp_prefix / 'combiner',
)
