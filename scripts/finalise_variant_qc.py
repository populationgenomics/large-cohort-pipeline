#!/usr/bin/env python3

from larcoh.utils import start_hail_context
from larcoh.variant_qc.load_vqsr import load_vqsr


def main():
    start_hail_context()

    # Convert VQSR VCF to HT
    load_vqsr()

    # TODO: generate QC annotations

    # TODO: generate frequencies

    # TODO: making final MT


if __name__ == '__main__':
    main()
