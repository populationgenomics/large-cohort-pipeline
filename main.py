import os
import click
import coloredlogs

from cpg_utils.config import get_config, set_config_paths

fmt = '%(asctime)s %(levelname)s (%(name)s %(lineno)s): %(message)s'
coloredlogs.install(level='INFO', fmt=fmt)


@click.command()
@click.argument('config_paths', nargs=-1)
def main(config_paths: list[str]):
    """
    Run a workflow, using CONFIG_PATHS in the order specified, overriding
    $CPG_CONFIG_PATH if specified.
    """
    if _env_var := os.environ.get('CPG_CONFIG_PATH'):
        config_paths = _env_var.split(',') + list(config_paths)
    set_config_paths(list(config_paths))

    # Can't import it before all configs are set.
    from larcoh.dataproc_utils import dataproc_job

    scatter_count = get_config()['workflow'].get('scatter_count', 50)
    if scatter_count > 100:
        autoscaling_workers = '200'
    elif scatter_count > 50:
        autoscaling_workers = '100'
    else:
        autoscaling_workers = '50'

    combiner_j = dataproc_job(
        script_name='scripts/combiner.py',
        autoscaling_policy=f'vcf-combiner-{autoscaling_workers}',
    )

    sample_qc_j = dataproc_job(
        script_name='scripts/sample_qc.py',
        depends_on=[combiner_j],
    )

    pcrelate_j = dataproc_job(
        script_name='scripts/pcrelate.py',
        preemptible=False,
        depends_on=[combiner_j, sample_qc_j],
    )

    ancestry_j = dataproc_job(
        script_name='scripts/ancestry.py',
        depends_on=[combiner_j, sample_qc_j, pcrelate_j],
    )

    siteonly_vcf_j = dataproc_job(
        script_name='scripts/make_siteonly_vcf.py',
        # hl.export_vcf() uses non-preemptible workers' disk to merge VCF files.
        # 10 samples take 2.3G, 400 samples take 60G, which roughly matches
        # `huge_disk` (also used in the AS-VQSR VCF-gather job)
        worker_boot_disk_size=200,
        secondary_worker_boot_disk_size=200,
        depends_on=[combiner_j, sample_qc_j],
    )

    from larcoh.variant_qc.hb_vqsr_jobs import add_vqsr_jobs

    vqsr_jobs = add_vqsr_jobs()
    for j in vqsr_jobs:
        j.depends_on(siteonly_vcf_j)

    dataproc_job(
        script_name='scripts/finalise_variant_qc.py',
        depends_on=[combiner_j, sample_qc_j, ancestry_j] + vqsr_jobs,
    )


if __name__ == '__main__':
    main()
