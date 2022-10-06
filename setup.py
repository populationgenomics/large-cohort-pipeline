#!/usr/bin/env python

import setuptools


setuptools.setup(
    name='larcoh',
    version='0.1.5',
    description='Pipeline for joint calling, sample and variant QC for WGS germline '
    'variant calling data in large cohorts',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/populationgenomics/large-cohort-pipeline',
    license='MIT',
    packages=['larcoh'],
    package_data={
        'larcoh': ['larcoh.toml'],
    },
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'cpg-utils',
        'cpg-gnomad',
        'analysis-runner',
        'sample-metadata',
        'hail',
        'pandas',
        'click',
        'google-cloud-storage',
        'google-cloud-secret-manager',
        'coloredlogs',
    ],
    keywords='bioinformatics',
    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Natural Language :: English',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Operating System :: Unix',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
    ],
)
