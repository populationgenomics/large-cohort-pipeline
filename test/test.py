"""
Test Hail Query functions.
"""
import glob
import os
from os.path import exists

import toml
from cpg_utils.hail_batch import dataset_path
from cpg_utils import to_path, Path
from cpg_utils.config import set_config_paths
from cpg_utils.config import update_dict
from cpg_utils.workflows.filetypes import GvcfPath
from cpg_utils.workflows.targets import Cohort
from cpg_utils.workflows.utils import timestamp
from pytest_mock import MockFixture


def _set_config(dir_path: Path, extra_conf: dict | None = None):
    results_dir_path = (
        to_path(__file__).parent / 'results' / os.getenv('TEST_TIMESTAMP', timestamp())
    )
    d = {
        'workflow': {
            'local_dir': str(results_dir_path.absolute()),
            'dataset_gcp_project': 'thousand-genomes',
            'dataset': 'thousand-genomes',
            'access_level': 'test',
            'sequencing_type': 'genome',
            'output_version': '1.0',
            'check_intermediates': True,
            'path_scheme': 'local',
            'reference_prefix': str(to_path(__file__).parent / 'data' / 'reference'),
        },
        'hail': {
            'billing_project': 'thousand-genomes',
            'dry_run': True,
            'query_backend': 'spark_local',
        },
    }
    if extra_conf:
        update_dict(d, extra_conf)
    with (out_path := dir_path / 'config.toml').open('w') as f:
        toml.dump(d, f)
    set_config_paths([str(out_path)])


def test_larcoh(mocker: MockFixture, tmpdir: str):
    """
    Run entire workflow in a local mode.
    """
    _set_config(
        to_path(tmpdir),
        extra_conf={
            'combiner': {
                'intervals': ['chr20:start-end', 'chrX:start-end', 'chrY:start-end'],
            },
        },
    )

    cohort = Cohort()
    ds = cohort.create_dataset('thousand-genomes')
    gvcf_root = to_path(__file__).parent / 'data' / 'gvcf'
    found_gvcf_paths = list(gvcf_root.glob('*.g.vcf.gz'))
    assert len(found_gvcf_paths) > 0, gvcf_root
    for gvcf_path in found_gvcf_paths:
        sample_id = gvcf_path.name.split('.')[0]
        s = ds.add_sample(id=sample_id, external_id=sample_id.replace('CPG', 'EXT'))
        s.gvcf = GvcfPath(gvcf_path)

    mocker.patch('cpg_utils.workflows.inputs.create_cohort', lambda: cohort)

    from larcoh import (
        combiner,
        sample_qc,
        dense_subset,
        relatedness,
        ancestry_pca,
        ancestry_plots,
    )
    from larcoh.utils import start_hail_context

    start_hail_context()
    combiner.run()
    sample_qc.run()
    dense_subset.run()
    relatedness.pcrelate()
    relatedness.flag_related()
    ancestry_pca.run()
    ancestry_plots.run()

    assert exists(to_path(dataset_path(f'vds/v1-0.vds')))
