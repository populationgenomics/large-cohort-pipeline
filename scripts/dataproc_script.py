#!/usr/bin/env python

"""
Generic script to run a function on dataproc.
"""

import click
from larcoh.utils import start_hail_context
from cpg_utils import to_path
from importlib import import_module


@click.command()
@click.argument('import_module_name')
@click.argument('function_name')
@click.argument('function_path_args', nargs=-1)
def main(import_module_name: str, function_name: str, function_path_args: list[str]):
    module = import_module(import_module_name)
    func = getattr(module, function_name)
    start_hail_context()
    func(*[to_path(path) for path in function_path_args])


main()
