import logging

import hail as hl
from cpg_utils.hail_batch import reference_path
from cpg_utils.workflows.utils import can_reuse

from larcoh import parameters


def make_dense_subset() -> hl.MatrixTable:
    """
    Filter a sparse VariantDataset to a set of predetermined QC sites
    and return a dense MatrixTable with split multiallelics.
    @return: filtered and densified MatrixTable.
    """
    if can_reuse(parameters.dense_mt_path):
        return hl.read_matrix_table(str(parameters.dense_mt_path))

    vds = hl.vds.read_vds(str(parameters.vds_path))

    logging.info('Splitting multiallelics')
    vds = hl.vds.split_multi(vds, filter_changed_loci=True)

    logging.info('Filtering variants to predetermined QC variants...')
    qc_variants_ht = hl.read_table(
        str(reference_path('gnomad/predetermined_qc_variants'))
    )
    vds = hl.vds.filter_variants(vds, qc_variants_ht)
    logging.info('Densifying data...')
    mt = hl.vds.to_dense_mt(vds)
    mt = mt.select_entries('GT', 'GQ', 'DP', 'AD')
    logging.info(
        f'Number of predetermined QC variants found in the VDS: {mt.count_rows()}'
    )
    mt = mt.naive_coalesce(5000)
    return mt.checkpoint(str(parameters.dense_mt_path), overwrite=True)
