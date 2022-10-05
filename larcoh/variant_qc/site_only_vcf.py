#!/usr/bin/env python

"""
Convert to site-only table and annotate with AS fields.
"""

import hail as hl
from cpg_utils import Path
from cpg_utils.workflows.utils import can_reuse
from gnomad.utils.vcf import adjust_vcf_incompatible_types
from gnomad.utils.sparse_mt import default_compute_info

from larcoh import parameters


def site_only_ht_to_vcf():
    vqsr_prefix = parameters.tmp_prefix / 'vqsr'
    site_only_ht_path = vqsr_prefix / 'site_only.ht'
    out_vcf_path = vqsr_prefix / 'site_only.vcf.gz'

    if can_reuse(out_vcf_path):
        return out_vcf_path

    hl.export_vcf(hl.read_table(site_only_ht_path), out_vcf_path)


def vds_to_site_only_ht() -> hl.Table:
    """
    Convert VDS into sites-only VCF-ready table.
    """
    vqsr_prefix = parameters.tmp_prefix / 'vqsr'
    out_ht_path = vqsr_prefix / 'site_only.ht'

    if can_reuse(out_ht_path):
        return hl.read_table(str(out_ht_path))

    vds = hl.vds.read_vds(str(parameters.vds_path))
    sample_ht = hl.read_table(str(parameters.sample_qc_ht_path))
    related_to_drop = hl.read_table(str(parameters.relateds_to_drop_ht_path))
    sample_ht.annotate(related=hl.is_defined(related_to_drop[sample_ht.key]))

    mt = vds.variant_data
    mt = mt.filter_cols(sample_ht[mt.col_key].filtered, keep=False)
    mt = mt.filter_cols(sample_ht[mt.col_key].related, keep=False)
    mt = _filter_rows_and_add_tags(mt)
    var_ht = _create_info_ht(mt, n_partitions=mt.n_partitions())
    var_ht = adjust_vcf_incompatible_types(
        var_ht,
        # with default INFO_VCF_AS_PIPE_DELIMITED_FIELDS, AS_VarDP will be converted
        # into a pipe-delimited value e.g.: VarDP=|132.1|140.2
        # which breaks VQSR parser (it doesn't recognise the delimiter and treats
        # it as a array with a single string value "|132.1|140.2", leading to
        # an IndexOutOfBound exception when trying to access value for second allele)
        pipe_delimited_annotations=[],
    )
    var_ht.write(str(out_ht_path), overwrite=True)
    return var_ht


def _filter_rows_and_add_tags(mt: hl.MatrixTable) -> hl.MatrixTable:
    mt = hl.experimental.densify(mt)

    # Filter to only non-reference sites.
    # An example of a variant with hl.len(mt.alleles) > 1 BUT NOT
    # hl.agg.any(mt.LGT.is_non_ref()) is a variant that spans a deletion,
    # which was however filtered out, so the LGT was set to NA, however the site
    # was preserved to account for the presence of that spanning deletion.
    # locus   alleles    LGT
    # chr1:1 ["GCT","G"] 0/1
    # chr1:3 ["T","*"]   NA
    mt = mt.filter_rows((hl.len(mt.alleles) > 1) & (hl.agg.any(mt.LGT.is_non_ref())))

    # annotate site level DP as site_dp onto the mt rows to avoid name collision
    mt = mt.annotate_rows(site_dp=hl.agg.sum(mt.DP))

    # Add AN tag as ANS
    return mt.annotate_rows(ANS=hl.agg.count_where(hl.is_defined(mt.LGT)) * 2)


def _create_info_ht(mt: hl.MatrixTable, n_partitions: int) -> hl.Table:
    """Create info table from vcf matrix table"""
    info_ht = default_compute_info(mt, site_annotations=True, n_partitions=n_partitions)
    info_ht = info_ht.annotate(
        info=info_ht.info.annotate(DP=mt.rows()[info_ht.key].site_dp)
    )
    return info_ht
