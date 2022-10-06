import os
import click
import coloredlogs
from cpg_utils import Path, to_path

from cpg_utils.config import get_config, set_config_paths
from cpg_utils.hail_batch import dataset_path
from cpg_utils.workflows.batch import get_batch

# from cpg_utils.workflows.targets import Cohort
# from cpg_utils.workflows.workflow import (
#     run_workflow,
#     stage,
#     CohortStage,
#     StageInput,
#     StageOutput,
# )


fmt = '%(asctime)s %(levelname)s (%(name)s %(lineno)s): %(message)s'
coloredlogs.install(level='INFO', fmt=fmt)


def path_prefix(category: str | None = None) -> Path:
    output_version = get_config()['workflow']['output_version']
    output_version = f'v{output_version}'.replace('.', '-')
    _suffix = f'larcoh/{output_version}'
    return to_path(dataset_path(_suffix, category=category))


# @stage
# class Combiner(CohortStage):
#     def __init__(self, name: str):
#         super().__init__(name)
#
#     def expected_outputs(self, cohort: Cohort) -> Path:
#         output_version = get_config()['workflow']['output_version']
#         vds_version = get_config()['workflow'].get('vds_version') or output_version
#         vds_version = f'v{vds_version}'.replace('.', '-')
#         return to_path(dataset_path(f'vds/{vds_version}.vds'))
#
#     def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
#         scatter_count = get_config()['workflow'].get('scatter_count', 50)
#         if scatter_count > 100:
#             autoscaling_workers = '200'
#         elif scatter_count > 50:
#             autoscaling_workers = '100'
#         else:
#             autoscaling_workers = '50'
#
#         # Can't import it before all configs are set:
#         from larcoh.dataproc_utils import dataproc_job
#
#         j = dataproc_job(
#             script_name='scripts/combiner.py',
#             params=dict(
#                 out_vds_path=self.expected_outputs(cohort),
#                 tmp_prefix=self.tmp_prefix,
#             ),
#             autoscaling_policy=f'vcf-combiner-{autoscaling_workers}',
#         )
#         return self.make_outputs(cohort, self.expected_outputs(cohort), [j])


@click.command()
@click.argument('config_paths', nargs=-1)
def main(config_paths: list[str]):
    """
    Run a workflow, using CONFIG_PATHS in the order specified, overriding
    $CPG_CONFIG_PATH if specified.
    """
    larcoh_config_path = to_path(__file__).parent / 'configs' / 'larcoh.toml'
    assert larcoh_config_path.exists(), larcoh_config_path
    if _env_var := os.environ.get('CPG_CONFIG_PATH'):
        config_paths = _env_var.split(',') + list(config_paths)
    set_config_paths(list(config_paths))

    scatter_count = get_config()['workflow'].get('scatter_count', 50)
    if scatter_count > 100:
        autoscaling_workers = '200'
    elif scatter_count > 50:
        autoscaling_workers = '100'
    else:
        autoscaling_workers = '50'

    from larcoh.dataproc_utils import dataproc_job
    from larcoh.combiner import run

    output_version = get_config()['workflow']['output_version']
    vds_version = get_config()['workflow'].get('vds_version') or output_version
    vds_version = f'v{vds_version}'.replace('.', '-')
    vds_path = to_path(dataset_path(f'vds/{vds_version}.vds'))

    combiner_j = dataproc_job(
        job_name='Combiner',
        function=run,
        function_path_args=[vds_path, path_prefix('tmp') / 'combiner'],
        autoscaling_policy=f'vcf-combiner-{autoscaling_workers}',
    )

    # sample_qc_j = dataproc_job(
    #     script_name='scripts/sample_qc.py',
    #     depends_on=[combiner_j],
    # )
    #
    # pcrelate_j = dataproc_job(
    #     script_name='scripts/pcrelate.py',
    #     preemptible=False,
    #     depends_on=[combiner_j, sample_qc_j],
    # )
    #
    # ancestry_j = dataproc_job(
    #     script_name='scripts/ancestry.py',
    #     depends_on=[combiner_j, sample_qc_j, pcrelate_j],
    # )
    #
    # siteonly_vcf_j = dataproc_job(
    #     script_name='scripts/make_siteonly_vcf.py',
    #     # hl.export_vcf() uses non-preemptible workers' disk to merge VCF files.
    #     # 10 samples take 2.3G, 400 samples take 60G, which roughly matches
    #     # `huge_disk` (also used in the AS-VQSR VCF-gather job)
    #     worker_boot_disk_size=200,
    #     secondary_worker_boot_disk_size=200,
    #     depends_on=[combiner_j, sample_qc_j],
    # )
    #
    # from larcoh.variant_qc.hb_vqsr_jobs import add_vqsr_jobs
    #
    # vqsr_jobs = add_vqsr_jobs()
    # for j in vqsr_jobs:
    #     j.depends_on(siteonly_vcf_j)
    #
    # dataproc_job(
    #     script_name='scripts/finalise_variant_qc.py',
    #     depends_on=[combiner_j, sample_qc_j, ancestry_j] + vqsr_jobs,
    # )

    get_batch().run(wait=False)


if __name__ == '__main__':
    main()
