#!/usr/bin/env python3

import hail as hl

from larcoh import parameters
from larcoh.utils import start_hail_context
from larcoh.variant_qc.vqsr import vds_to_sites_only_ht


def main():
    """
    Run the large cohort workflow, assuming we are inside Hail Query environment
    (i.e. running on a Dataproc cluster).
    """

    start_hail_context()

    vds = hl.vds.read_vds(str(parameters.vds_path))

    ht = hl.read_table(str(parameters.sample_qc_ht_path))
    related_to_drop = hl.read_table(str(parameters.relateds_to_drop))
    ht.annotate(related=hl.is_defined(related_to_drop[ht.key]))

    vqsr_prefix = parameters.tmp_prefix / 'vqsr'

    # MT to site-only HT annotated with AS INFO fields
    siteonly_ht = vds_to_sites_only_ht(
        vds=vds,
        sample_ht=ht,
        out_ht_path=vqsr_prefix / 'input.ht',
    )
    vcf_path = vqsr_prefix / 'input.vcf.bgz'
    hl.export_vcf(siteonly_ht, vcf_path)

    # vqsred_vcf_path = vqsr_prefix / 'output.vcf.gz'
    # if overwrite or not utils.file_exists(vqsred_vcf_path):
    #     vqsr_vcf_job = add_vqsr_jobs(
    #         b,
    #         combined_vcf_path=combined_vcf_path,
    #         gvcf_count=sample_count,
    #         work_bucket=vqsr_bucket,
    #         web_bucket=join(web_bucket, 'vqsr'),
    #         vqsr_params_d=vqsr_params_d,
    #         output_vcf_path=vqsred_vcf_path,
    #         overwrite=overwrite,
    #         depends_on=depends_on + [siteonly_ht_to_vcf_job],
    #     )
    # else:
    #     vqsr_vcf_job = b.new_job('AS-VQSR [reuse]')
    #
    # job_name = 'AS-VQSR: load_vqsr'
    # vqsr_split_ht_path = join(vqsr_bucket, 'vqsr-split.ht')
    # if overwrite or not utils.file_exists(vqsr_split_ht_path):
    #     load_vqsr_job = add_job(
    #         b,
    #         f'{utils.SCRIPTS_DIR}/load_vqsr.py '
    #         f'--overwrite '
    #         f'--out-path {vqsr_split_ht_path} '
    #         f'--vqsr-vcf-path {vqsred_vcf_path} '
    #         f'--bucket {work_bucket} ',
    #         job_name=job_name,
    #         is_test=is_test,
    #         highmem=highmem_workers,
    #         num_workers=scatter_count,
    #         depends_on=depends_on + [vqsr_vcf_job],
    #     )
    #     load_vqsr_job.depends_on(vqsr_vcf_job)
    # else:
    #     load_vqsr_job = b.new_job(f'{job_name} [reuse]')
    #
    # job_name = 'Var QC: generate QC annotations'
    # allele_data_ht_path = join(work_bucket, 'allele-data.ht')
    # # allele_data:
    # #   nonsplit_alleles
    # #   has_star
    # #   variant_type
    # #   n_alt_alleles
    # #   allele_type
    # #   was_mixed
    #
    # qc_ac_ht_path = join(work_bucket, 'qc-ac.ht')
    # # ac_qc_samples_raw
    # # ac_qc_samples_unrelated_raw
    # # ac_release_samples_raw
    # # ac_qc_samples_adj
    # # ac_qc_samples_unrelated_adj
    # # ac_release_samples_adj
    #
    # fam_stats_ht_path = join(work_bucket, 'fam-stats.ht') if ped_file else None
    #
    # if any(
    #     not utils.can_reuse(fp, overwrite)
    #     for fp in [allele_data_ht_path, qc_ac_ht_path]
    #     + ([fam_stats_ht_path] if fam_stats_ht_path else [])
    # ):
    #     var_qc_anno_job = add_job(
    #         b,
    #         f'{utils.SCRIPTS_DIR}/generate_variant_qc_annotations.py '
    #         + f'{"--overwrite " if overwrite else ""}'
    #         + f'--mt {raw_combined_mt_path} '
    #         + f'--hard-filtered-samples-ht {hard_filter_ht_path} '
    #         + f'--meta-ht {meta_ht_path} '
    #         + f'--out-allele-data-ht {allele_data_ht_path} '
    #         + f'--out-qc-ac-ht {qc_ac_ht_path} '
    #         + (f'--out-fam-stats-ht {fam_stats_ht_path} ' if ped_file else '')
    #         + (f'--fam-file {ped_file} ' if ped_file else '')
    #         + f'--bucket {work_bucket} '
    #         + f'--n-partitions {scatter_count * 25}',
    #         job_name=job_name,
    #         is_test=is_test,
    #         highmem=highmem_workers,
    #         num_workers=scatter_count,
    #         depends_on=depends_on,
    #     )
    #     var_qc_anno_job.depends_on(*depends_on)
    # else:
    #     var_qc_anno_job = b.new_job(f'{job_name} [reuse]')
    #
    # job_name = 'Var QC: generate frequencies'
    # freq_ht_path = join(work_bucket, 'frequencies.ht')
    # # InbreedingCoeff
    # # freq
    # # faf
    # # popmax:
    # #   AC
    # #   AF
    # #   AN
    # #   homozygote_count
    # #   pop
    # #   faf95
    # # qual_hists
    # # raw_qual_hists
    #
    # if overwrite or not utils.file_exists(freq_ht_path):
    #     freq_job = add_job(
    #         b,
    #         f'{utils.SCRIPTS_DIR}/generate_freq_data.py --overwrite '
    #         f'--mt {raw_combined_mt_path} '
    #         f'--hard-filtered-samples-ht {hard_filter_ht_path} '
    #         f'--meta-ht {meta_ht_path} '
    #         f'--out-ht {freq_ht_path} '
    #         f'--bucket {work_bucket} ',
    #         job_name=job_name,
    #         is_test=is_test,
    #         highmem=highmem_workers,
    #         num_workers=scatter_count,
    #         depends_on=depends_on,
    #         long=True,
    #     )
    #     freq_job.depends_on(*depends_on)
    # else:
    #     freq_job = b.new_job(f'{job_name} [reuse]')
    #
    # job_name = 'Making final MT'
    # if not utils.can_reuse(out_filtered_combined_mt_path, overwrite):
    #     final_mt_j = add_job(
    #         b,
    #         f'{utils.SCRIPTS_DIR}/make_finalised_mt.py '
    #         f'--overwrite '
    #         f'--mt {raw_combined_mt_path} '
    #         f'--vqsr-ht {vqsr_split_ht_path} '
    #         f'--freq-ht {freq_ht_path} '
    #         f'--allele-data-ht {allele_data_ht_path} '
    #         f'--qc-ac-ht {qc_ac_ht_path} '
    #         f'--out-mt {out_filtered_combined_mt_path} '
    #         f'--meta-ht {meta_ht_path} ',
    #         job_name=job_name,
    #         is_test=is_test,
    #         num_workers=scatter_count,
    #         depends_on=depends_on,
    #     )
    #     final_mt_j.depends_on(load_vqsr_job, var_qc_anno_job, freq_job)
    # else:
    #     final_mt_j = b.new_job(f'{job_name} [reuse]')
    #
    # return final_mt_j
    #
    # info_ht = get_info(split=False).ht()
    # hl.export_vcf(adjust_vcf_incompatible_types(info_ht), info_vcf_path())

    # var_qc_job = add_variant_qc_jobs(
    #     b=b,
    #     work_bucket=join(analysis_bucket, 'variant_qc'),
    #     web_bucket=join(web_bucket, 'variant_qc'),
    #     raw_combined_mt_path=raw_combined_mt_path,
    #     hard_filter_ht_path=hard_filter_ht_path,
    #     meta_ht_path=meta_ht_path,
    #     out_filtered_combined_mt_path=filtered_combined_mt_path,
    #     sample_count=len(samples_df),
    #     ped_file=ped_fpath,
    #     overwrite=overwrite,
    #     vqsr_params_d=utils.get_filter_cutoffs(filter_cutoffs_path)['vqsr'],
    #     scatter_count=scatter_count,
    #     is_test=output_namespace in ['test', 'tmp'],
    #     depends_on=[combiner_job, sample_qc_job],
    #     highmem_workers=highmem_workers,
    # )
    #
    # job_name = 'Remove ref blocks'
    # if not utils.can_reuse(filtered_combined_noref_mt_path, overwrite):
    #     noref_mt_j = add_job(
    #         b,
    #         f'{utils.SCRIPTS_DIR}/make_noref_mt.py '
    #         f'--overwrite '
    #         f'--mt {filtered_combined_mt_path} '
    #         f'--out-mt {filtered_combined_noref_mt_path}',
    #         job_name=job_name,
    #         num_workers=scatter_count,
    #         depends_on=[var_qc_job],
    #     )
    # else:
    #     noref_mt_j = b.new_job(f'{job_name} [reuse]')
    #
    # if subset_projects:
    #     diff_projects = set(subset_projects) - set(input_projects)
    #     if diff_projects:
    #         raise click.BadParameter(
    #             f'--subset-project values should be a subset of --input-project '
    #             f'values. The following projects are not in input projects: '
    #             f'{diff_projects} '
    #         )
    #     subset_projects = list(set(subset_projects))
    #     subset_mt_path = f'{release_bucket}/mt/{output_version}-{"-".join(sorted(subset_projects))}.mt'
    #     job_name = f'Making subset MT for {", ".join(subset_projects)}'
    #     if overwrite or not utils.file_exists(subset_mt_path):
    #         add_job(
    #             b,
    #             f'{utils.SCRIPTS_DIR}/make_subset_mt.py '
    #             f'--mt {filtered_combined_noref_mt_path} ' +
    #             (''.join(f'--subset-project {p} ' for p in subset_projects)) +
    #             f'--out-mt {subset_mt_path}',
    #             job_name=job_name,
    #             is_test=output_namespace in ['test', 'tmp'],
    #             num_workers=scatter_count,
    #             depends_on=[noref_mt_j],
    #         )
    #     else:
    #         b.new_job(f'{job_name} [reuse]')
    #
    #     subset_vcf_path = f'{release_bucket}/vcf/{output_version}-{"-".join(subset_projects)}.vcf.bgz'
    #     job_name = f'Convert subset MT to VCF for {", ".join(subset_projects)}'
    #     if overwrite or not utils.file_exists(subset_vcf_path):
    #         add_job(
    #             b,
    #             f'{utils.SCRIPTS_DIR}/final_mt_to_vcf.py '
    #             f'--mt {subset_mt_path} ' +
    #             f'--out-vcf {subset_vcf_path}',
    #             job_name=job_name,
    #             is_test=output_namespace in ['test', 'tmp'],
    #             num_workers=scatter_count,
    #             preemptible=False,
    #             depends_on=[noref_mt_j],
    #         )
    #     else:
    #         b.new_job(f'{job_name} [reuse]')


if __name__ == '__main__':
    main()
