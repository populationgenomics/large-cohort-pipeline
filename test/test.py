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

DEFAULT_CONF = f"""
[workflow]
dataset_gcp_project = 'thousand-genomes'
dataset = 'thousand-genomes'
access_level = 'test'
sequencing_type = 'genome'
output_version = '1.0'
check_intermediates = true

[hail]
billing_project = 'thousand-genomes'
dry_run = true
"""


def _set_config(dir_path: Path, extra_conf: dict | None = None):
    d = toml.loads(DEFAULT_CONF)
    if extra_conf:
        update_dict(d, extra_conf)
    print(d)
    with (out_path := dir_path / 'config.toml').open('w') as f:
        toml.dump(d, f)
    set_config_paths(['local.toml', str(out_path)])


def test_larcoh(mocker: MockFixture, tmpdir: str):
    """
    Run entire workflow in a local mode.
    """
    results_dir_path = to_path('results') / os.getenv('TEST_TIMESTAMP', timestamp())

    _set_config(
        to_path(tmpdir),
        extra_conf={
            'workflow': {
                'local_dir': str(results_dir_path),
            },
            'combiner': {
                'intervals': ['chr20:start-end', 'chrX:start-end', 'chrY:start-end'],
            },
        },
    )

    cohort = Cohort()
    ds = cohort.create_dataset('test-input-dataset')
    gvcf_paths = [to_path(p) for p in glob.glob('data/gvcf/*.g.vcf.gz')]
    for gvcf_path in gvcf_paths:
        sample_name = gvcf_path.name.split('.')[0]
        s = ds.add_sample(id=sample_name)
        s.gvcf = GvcfPath(gvcf_path)

    mocker.patch('cpg_utils.workflows.inputs.create_cohort', lambda: cohort)

    from larcoh import combiner, sample_qc, dense_subset, relatedness
    from larcoh.utils import start_hail_context

    start_hail_context()
    combiner.combine()
    sample_qc.run()
    dense_subset.make_dense_subset()
    relatedness.pcrelate()

    assert exists(to_path(dataset_path(f'vds/v1-0.vds')))
