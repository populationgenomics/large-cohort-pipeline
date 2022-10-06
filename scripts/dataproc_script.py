#!/usr/bin/env python

"""
Generic script to run a function on dataproc.
"""

import click
from larcoh.utils import start_hail_context
from cpg_utils import to_path


start_hail_context()


@click.command()
@click.argument('function_import_name')
@click.argument('function_path_args', nargs=-1)
def main(function_import_name: str, function_path_args: list[str]):
    func = __import__(function_import_name)
    func([to_path(path) for path in function_path_args])


main()
