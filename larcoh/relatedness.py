import logging
import hail as hl
from cpg_utils import Path
from cpg_utils.config import get_config
from cpg_utils.workflows.utils import can_reuse
from gnomad.sample_qc.relatedness import compute_related_samples_to_drop

from larcoh import parameters


def pcrelate() -> hl.Table:
    """
    Writes table with the following structure:
    Row fields:
        'i': str
        'j': str
        'kin': float64
        'ibd0': float64
        'ibd1': float64
        'ibd2': float64
    Key: ['i', 'j']
    """
    mt = hl.read_matrix_table(str(parameters.dense_mt_path))

    if can_reuse(parameters.relatedness_ht_path):
        return hl.read_table(str(parameters.relatedness_ht_path))

    mt = mt.select_entries('GT')

    logging.info('Running relatedness check')
    scores_ht_path = parameters.tmp_prefix / 'pcrelate' / 'relatedness_pca_scores.ht'
    if can_reuse(scores_ht_path):
        scores_ht = hl.read_table(scores_ht_path)
    else:
        sample_num = mt.cols().count()
        _, scores_ht, _ = hl.hwe_normalized_pca(
            mt.GT, k=max(1, min(sample_num // 3, 10)), compute_loadings=False
        )
        scores_ht.checkpoint(scores_ht_path, overwrite=True)

    relatedness_ht = hl.pc_relate(
        mt.GT,
        min_individual_maf=0.01,
        scores_expr=scores_ht[mt.col_key].scores,
        block_size=4096,
        min_kinship=0.05,
    )

    # Converting keys for type struct{str} to str to align
    # with the rank_ht `s` key:
    relatedness_ht = relatedness_ht.key_by(i=relatedness_ht.i.s, j=relatedness_ht.j.s)
    return relatedness_ht.checkpoint(
        str(parameters.relatedness_ht_path), overwrite=True
    )


def flag_related() -> hl.Table:
    """
    Rank samples and flag samples to drop so there is only one sample per family
    left, with the highest rank in the family.
    """
    sample_ht = hl.read_table(str(parameters.sample_qc_ht_path))
    relatedness_ht = hl.read_table(str(parameters.relatedness_ht_path))
    out_ht_path = parameters.relateds_to_drop

    logging.info(f'Flagging related samples to drop')
    if can_reuse(out_ht_path):
        return hl.read_table(str(out_ht_path))

    rankings_ht_path = parameters.tmp_prefix / 'relatedness' / f'samples_rankings.ht'
    if can_reuse(rankings_ht_path):
        rank_ht = hl.read_table(rankings_ht_path)
    else:
        rank_ht = _compute_sample_rankings(
            sample_ht=sample_ht,
        ).checkpoint(rankings_ht_path, overwrite=True)

    try:
        filtered_samples = hl.literal(
            rank_ht.aggregate(
                hl.agg.filter(rank_ht.filtered, hl.agg.collect(rank_ht.s))
            )
        )
    except hl.ExpressionException:
        # Hail doesn't handle it with `aggregate` when none of
        # the samples is 'filtered'
        filtered_samples = hl.empty_array(hl.tstr)

    samples_to_drop_ht = compute_related_samples_to_drop(
        relatedness_ht,
        rank_ht,
        kin_threshold=get_config()['larcoh']['max_kin'],
        filtered_samples=filtered_samples,
    )
    samples_to_drop_ht = samples_to_drop_ht.checkpoint(str(out_ht_path), overwrite=True)
    sample_ht.annotate(
        related=hl.is_defined(samples_to_drop_ht[sample_ht.key]),
    )


def _compute_sample_rankings(
    sample_ht: hl.Table,
) -> hl.Table:
    """
    Orders samples by hard filters and coverage and adds rank, which is the lower,
    the better.

    @param sample_ht: table with a `chr20_mean_dp` row field
    @return: table ordered by rank, with the following row fields:
        `rank`, `filtered`
    """
    ht = sample_ht.drop(*list(sample_ht.globals.dtype.keys()))
    ht = ht.select(
        'chr20_mean_dp',
        filtered=hl.len(ht.filters) > 0,
    )
    ht = ht.order_by(ht.filtered, hl.desc(ht.chr20_mean_dp)).add_index(name='rank')
    return ht.key_by('s').select('filtered', 'rank')
