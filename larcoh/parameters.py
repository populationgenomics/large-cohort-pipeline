# from cpg_utils import to_path
# from cpg_utils.config import get_config
# from cpg_utils.hail_batch import dataset_path
#
# output_version = get_config()['workflow']['output_version']
# output_version = f'v{output_version}'.replace('.', '-')
# vds_version = get_config()['workflow'].get('vds_version')
# vds_version = f'v{vds_version}'.replace('.', '-') if vds_version else output_version
# _suffix = f'larcoh/{output_version}'
# out_prefix = to_path(dataset_path(_suffix))
# tmp_prefix = to_path(dataset_path(_suffix, category='tmp'))
# web_prefix = to_path(dataset_path(_suffix, category='web'))
#
# vds_path = to_path(dataset_path(f'vds/{vds_version}.vds'))
# sample_qc_ht_path = out_prefix / 'sample_qc.ht'
# dense_mt_path = out_prefix / 'dense-subset.mt'
# relatedness_ht_path = out_prefix / 'relatedness.ht'
# relateds_to_drop_ht_path = out_prefix / 'relateds-to-drop.ht'
#
# ancestry_prefix = out_prefix / 'ancestry'
# eigenvalues_ht_path = ancestry_prefix / 'eigenvalues.ht'
# scores_ht_path = ancestry_prefix / 'scores.ht'
# loadings_ht_path = ancestry_prefix / 'loadings.ht'
# inferred_pop_ht_path = ancestry_prefix / 'inferred_pop.ht'
