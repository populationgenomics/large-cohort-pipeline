import os
import click
import coloredlogs
from cpg_utils import Path, to_path

from cpg_utils.config import get_config, set_config_paths
from cpg_utils.workflows.batch import get_batch
from cpg_utils.workflows.inputs import get_cohort

from cpg_utils.workflows.targets import Cohort
from cpg_utils.workflows.workflow import (
    run_workflow,
    stage,
    CohortStage,
    StageInput,
    StageOutput,
    get_workflow,
)

fmt = '%(asctime)s %(levelname)s (%(name)s %(lineno)s): %(message)s'
coloredlogs.install(level='INFO', fmt=fmt)


@stage
class Combiner(CohortStage):
    def expected_outputs(self, cohort: Cohort) -> Path:
        if vds_version := get_config()['workflow'].get('vds_version'):
            if not vds_version.startswith('v'):
                vds_version = f'v{vds_version}'
            vds_version = vds_version.replace('.', '-')

        vds_version = (
            vds_version or get_workflow().output_version or get_workflow().run_timestamp
        )
        return cohort.analysis_dataset.prefix() / 'vds' / f'{vds_version}.vds'

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        # Can't import it before all configs are set:
        from larcoh.dataproc_utils import dataproc_job
        from larcoh.combiner import run

        scatter_count = get_config()['workflow'].get('scatter_count', 50)
        if (
            policy_name_template := get_config()['hail']
            .get('dataproc', {})
            .get('combiner_autoscaling_policy')
        ):
            if scatter_count > 100:
                autoscaling_workers = '200'
            elif scatter_count > 50:
                autoscaling_workers = '100'
            else:
                autoscaling_workers = '50'
            policy_name = policy_name_template.format(max_workers=autoscaling_workers)
        else:
            policy_name = None

        j = dataproc_job(
            job_name=self.__class__.__name__,
            function=run,
            function_path_args=dict(
                out_vds_path=self.expected_outputs(cohort),
                tmp_prefix=self.tmp_prefix,
            ),
            autoscaling_policy=policy_name,
        )
        return self.make_outputs(cohort, self.expected_outputs(cohort), [j])


@stage(required_stages=[Combiner])
class SampleQC(CohortStage):
    def expected_outputs(self, cohort: Cohort) -> Path:
        return get_workflow().prefix / 'sample_qc.ht'

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        from larcoh.dataproc_utils import dataproc_job
        from larcoh.sample_qc import run

        j = dataproc_job(
            job_name=self.__class__.__name__,
            function=run,
            function_path_args=dict(
                vds_path=inputs.as_path(cohort, Combiner),
                out_sample_qc_ht_path=self.expected_outputs(cohort),
                tmp_prefix=self.tmp_prefix,
            ),
            depends_on=inputs.get_jobs(cohort),
        )
        return self.make_outputs(cohort, self.expected_outputs(cohort), [j])


@stage(required_stages=[Combiner])
class DenseSubset(CohortStage):
    def expected_outputs(self, cohort: Cohort) -> Path:
        return get_workflow().prefix / 'dense-subset.mt'

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        from larcoh.dataproc_utils import dataproc_job
        from larcoh.dense_subset import run

        j = dataproc_job(
            job_name=self.__class__.__name__,
            function=run,
            function_path_args=dict(
                vds_path=inputs.as_path(cohort, Combiner),
                out_dense_mt_path=self.expected_outputs(cohort),
            ),
            depends_on=inputs.get_jobs(cohort),
        )
        return self.make_outputs(cohort, self.expected_outputs(cohort), [j])


@stage(required_stages=[SampleQC, DenseSubset])
class Relatedness(CohortStage):
    def expected_outputs(self, cohort: Cohort) -> dict[str, Path]:
        return dict(
            relatedness=get_workflow().prefix / 'relatedness.ht',
            relateds_to_drop=get_workflow().prefix / 'relateds-to-drop.ht',
        )

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        from larcoh.dataproc_utils import dataproc_job
        from larcoh.relatedness import run

        j = dataproc_job(
            job_name=self.__class__.__name__,
            function=run,
            function_path_args=dict(
                dense_mt_path=inputs.as_path(cohort, DenseSubset),
                sample_qc_ht_path=inputs.as_path(cohort, SampleQC),
                out_relatedness_ht_path=self.expected_outputs(cohort)['relatedness'],
                out_relateds_to_drop_ht_path=self.expected_outputs(cohort)[
                    'relateds_to_drop'
                ],
                tmp_prefix=self.tmp_prefix,
            ),
            depends_on=inputs.get_jobs(cohort),
        )
        return self.make_outputs(cohort, self.expected_outputs(cohort), [j])


@stage(required_stages=[SampleQC, DenseSubset, Relatedness])
class Ancestry(CohortStage):
    def expected_outputs(self, cohort: Cohort) -> dict[str, Path]:
        return dict(
            scores=get_workflow().prefix / 'ancestry' / 'scores.ht',
            eigenvalues=get_workflow().prefix / 'ancestry' / 'eigenvalues.ht',
            loadings=get_workflow().prefix / 'ancestry' / 'loadings.ht',
            inferred_pop=get_workflow().prefix / 'ancestry' / 'inferred_pop.ht',
        )

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        from larcoh.dataproc_utils import dataproc_job
        from larcoh.ancestry_pca import run

        j = dataproc_job(
            job_name=self.__class__.__name__,
            function=run,
            function_path_args=dict(
                dense_mt_path=inputs.as_path(cohort, DenseSubset),
                sample_qc_ht_path=inputs.as_path(cohort, SampleQC),
                relateds_to_drop_ht_path=inputs.as_path(
                    cohort, Relatedness, id='relateds_to_drop'
                ),
                tmp_prefix=self.tmp_prefix,
                out_scores_ht_path=self.expected_outputs(cohort)['scores'],
                out_eigenvalues_ht_path=self.expected_outputs(cohort)['eigenvalues'],
                out_loadings_ht_path=self.expected_outputs(cohort)['loadings'],
                out_inferred_pop_ht_path=self.expected_outputs(cohort)['inferred_pop'],
            ),
            depends_on=inputs.get_jobs(cohort),
        )
        return self.make_outputs(cohort, self.expected_outputs(cohort), [j])


@stage(required_stages=[SampleQC, Ancestry])
class AncestryPlots(CohortStage):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.out_prefix = get_workflow().web_prefix / 'ancestry'
        self.out_fname_pattern = '{scope}_pc{pci}.{ext}'

    def expected_outputs(self, cohort: Cohort) -> Path:
        return self.out_prefix / self.out_fname_pattern.format(
            scope='dataset', pci=1, ext='html'
        )

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        from larcoh.dataproc_utils import dataproc_job
        from larcoh.ancestry_plots import run

        j = dataproc_job(
            job_name=self.__class__.__name__,
            function=run,
            function_path_args=dict(
                out_path_pattern=self.out_prefix / self.out_fname_pattern,
                sample_qc_ht_path=inputs.as_path(cohort, SampleQC),
                scores_ht_path=inputs.as_path(cohort, Ancestry, id='scores'),
                eigenvalues_ht_path=inputs.as_path(cohort, Ancestry, id='eigenvalues'),
                loadings_ht_path=inputs.as_path(cohort, Ancestry, id='loadings'),
                inferred_pop_ht_path=inputs.as_path(
                    cohort, Ancestry, id='inferred_pop'
                ),
            ),
            depends_on=inputs.get_jobs(cohort),
        )
        return self.make_outputs(cohort, self.expected_outputs(cohort), [j])


@stage(required_stages=[Combiner, SampleQC, Relatedness])
class MakeSiteOnlyVcf(CohortStage):
    def expected_outputs(self, cohort: Cohort) -> Path:
        return self.tmp_prefix / 'siteonly.vcf.gz'

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        from larcoh.dataproc_utils import dataproc_job
        from larcoh.variant_qc.site_only_vcf import run

        j = dataproc_job(
            job_name=self.__class__.__name__,
            function=run,
            function_path_args=dict(
                vds_path=inputs.as_path(cohort, Combiner),
                sample_qc_ht_path=inputs.as_path(cohort, SampleQC),
                relateds_to_drop_ht_path=inputs.as_path(
                    cohort, Relatedness, id='relateds_to_drop'
                ),
                out_vcf_path=self.expected_outputs(cohort),
                tmp_prefix=self.tmp_prefix,
            ),
            depends_on=inputs.get_jobs(cohort),
            # hl.export_vcf() uses non-preemptible workers' disk to merge VCF files.
            # 10 samples take 2.3G, 400 samples take 60G, which roughly matches
            # `huge_disk` (also used in the AS-VQSR VCF-gather job)
            worker_boot_disk_size=200,
            secondary_worker_boot_disk_size=200,
        )
        return self.make_outputs(cohort, self.expected_outputs(cohort), [j])


@stage(required_stages=MakeSiteOnlyVcf)
class Vqsr(CohortStage):
    def expected_outputs(self, cohort: Cohort):
        return self.tmp_prefix / 'siteonly.vqsr.vcf.gz'

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        from larcoh.variant_qc.hb_vqsr_jobs import add_vqsr_jobs

        jobs = add_vqsr_jobs(
            b=get_batch(),
            input_siteonly_vcf_path=inputs.as_path(cohort, MakeSiteOnlyVcf),
            tmp_prefix=self.tmp_prefix,
            gvcf_count=len(get_cohort().get_samples()),
            out_path=self.expected_outputs(cohort),
        )
        return self.make_outputs(cohort, data=self.expected_outputs(cohort), jobs=jobs)


@stage(required_stages=Vqsr)
class VariantAnnotation(CohortStage):
    def expected_outputs(self, cohort: Cohort) -> dict[str, Path]:
        return {
            'vqsr_ht': get_workflow().prefix / 'vqsr.ht',
        }

    def queue_jobs(self, cohort: Cohort, inputs: StageInput) -> StageOutput | None:
        from larcoh.dataproc_utils import dataproc_job
        from larcoh.variant_qc.annotate import run

        j = dataproc_job(
            job_name=self.__class__.__name__,
            function=run,
            function_path_args=dict(
                site_only_vcf_path=inputs.as_path(cohort, Vqsr),
                vqsr_ht_path=self.expected_outputs(cohort)['vqsr_ht'],
            ),
            depends_on=inputs.get_jobs(cohort),
        )
        return self.make_outputs(cohort, data=self.expected_outputs(cohort), jobs=[j])


@click.command()
@click.argument('config_paths', nargs=-1)
def main(config_paths: list[str]):
    """
    Run a workflow, using CONFIG_PATHS in the order specified, overriding
    $CPG_CONFIG_PATH if specified.
    """
    larcoh_config_path = to_path(__file__).parent / 'configs' / 'larcoh.toml'
    assert larcoh_config_path.exists(), larcoh_config_path
    config_paths = [str(larcoh_config_path)] + list(config_paths)
    if _env_var := os.environ.get('CPG_CONFIG_PATH'):
        config_paths += _env_var.split(',') + list(config_paths)
    set_config_paths(list(config_paths))

    run_workflow([VariantAnnotation])


if __name__ == '__main__':
    main()
