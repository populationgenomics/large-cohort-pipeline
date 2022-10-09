import hail as hl
import logging

from cpg_utils import Path
from cpg_utils.workflows.utils import can_reuse
from gnomad.resources.grch38.gnomad import POPS_TO_REMOVE_FOR_POPMAX, DOWNSAMPLINGS
from gnomad.sample_qc.sex import adjusted_sex_ploidy_expr
from gnomad.utils.annotations import (
    get_adj_expr,
    age_hists_expr,
    bi_allelic_site_inbreeding_expr,
    faf_expr,
    pop_max_expr,
    set_female_y_metrics_to_na_expr,
    annotate_freq,
    qual_hist_expr,
)
from gnomad.utils.release import make_faf_index_dict, make_freq_index_dict


def run(
    vds_path: Path,
    sample_qc_ht_path: Path,
    relateds_to_drop_ht_path: Path,
    out_ht_path: Path,
):
    if can_reuse(out_ht_path):
        return

    vds = hl.vds.read_vds(str(vds_path))
    sample_qc_ht = hl.read_table(str(sample_qc_ht_path))
    relateds_to_drop_ht = hl.read_table(str(relateds_to_drop_ht_path))

    freq_ht = frequency_annotations(
        vds,
        sample_qc_ht,
        relateds_to_drop_ht,
    )
    logging.info(f'Writing out frequency data to {out_ht_path}...')
    freq_ht.write(str(out_ht_path), overwrite=True)


def frequency_annotations(
    vds: hl.vds.VariantDataset,
    sample_qc_ht: hl.Table,
    relateds_to_drop_ht: hl.Table,
) -> hl.Table:
    """
    Generate frequency annotations (AF, AC, AN, InbreedingCoeff)
    """
    logging.info('Reading full sparse MT and metadata table...')
    mt = vds.variant_data

    mt = mt.filter_cols(hl.len(sample_qc_ht[mt.col_key].filters) > 0, keep=False)
    mt = mt.filter_cols(hl.is_defined(relateds_to_drop_ht[mt.col_key]), keep=False)
    mt = hl.experimental.sparse_split_multi(mt, filter_changed_loci=True)

    logging.info('Densify-ing...')
    # The reason is that sparse matrix table has got NA records representing
    # the reference blocks, which affects the calculation of frequencies.
    # That's why we need to convert it to a "dense" representation, effectively
    # dropping reference blocks.
    mt = hl.experimental.densify(mt)
    mt = mt.filter_rows(hl.len(mt.alleles) > 1)

    logging.info('Computing adj and sex adjusted genotypes...')
    mt = mt.annotate_entries(
        GT=adjusted_sex_ploidy_expr(mt.locus, mt.GT, mt.meta.sex_karyotype),
        adj=get_adj_expr(mt.GT, mt.GQ, mt.DP, mt.AD),
    )

    logging.info('Generating frequency data...')

    mt = _compute_age_hists(mt)

    mt = annotate_freq(
        mt,
        sex_expr=mt.meta.sex_karyotype,
        pop_expr=mt.meta.pop,
        downsamplings=DOWNSAMPLINGS,
    )

    # Remove all loci with raw AC=0
    mt = mt.filter_rows(mt.freq[1].AC > 0)

    # Create a look-up Dictionary for entries contained
    # in the frequency annotation array
    mt = mt.annotate_globals(
        freq_index_dict=make_freq_index_dict(
            freq_meta=hl.eval(mt.freq_meta),
            downsamplings=hl.eval(mt.downsamplings),
            label_delimiter='-',
        )
    )

    # Unset Y-variant AF/AC/AN for female-specific metrics
    mt = mt.annotate_rows(freq=set_female_y_metrics_to_na_expr(mt))

    mt = _calc_inbreeding_coeff(mt)

    mt = _compute_filtering_af_and_popmax(mt)

    freq_ht = _annotate_quality_metrics_hist(mt)
    return freq_ht


def _compute_age_hists(mt: hl.MatrixTable) -> hl.MatrixTable:
    logging.info('Computing age histograms for each variant...')
    try:
        mt = mt.annotate_cols(age=hl.float64(mt.meta.age))
    except AttributeError:
        pass
    else:
        mt = mt.annotate_rows(
            **age_hists_expr(
                mt.adj,
                mt.GT,
                mt.age,
            )
        )
        # Compute callset-wide age histogram global
        mt = mt.annotate_globals(
            age_distribution=mt.aggregate_cols(
                hl.agg.hist(
                    mt.age,
                    30,
                    80,
                    10,
                )
            )
        )
    return mt


def _calc_inbreeding_coeff(mt: hl.MatrixTable) -> hl.MatrixTable:
    """
    NOTE: This is not the ideal location to calculate this, but added here
    to avoid another densify
    """
    logging.info('Calculating InbreedingCoeff...')
    # the algorithm assumes all samples are unrelated:
    return mt.annotate_rows(InbreedingCoeff=bi_allelic_site_inbreeding_expr(mt.GT))


def _compute_filtering_af_and_popmax(mt: hl.MatrixTable) -> hl.MatrixTable:
    logging.info('Computing filtering allele frequencies and popmax...')
    faf, faf_meta = faf_expr(mt.freq, mt.freq_meta, mt.locus, POPS_TO_REMOVE_FOR_POPMAX)
    mt = mt.select_rows(
        'InbreedingCoeff',
        'freq',
        faf=faf,
        popmax=pop_max_expr(mt.freq, mt.freq_meta, POPS_TO_REMOVE_FOR_POPMAX),
    )
    mt = mt.annotate_globals(
        faf_meta=faf_meta, faf_index_dict=make_faf_index_dict(faf_meta)
    )
    mt = mt.annotate_rows(
        popmax=mt.popmax.annotate(
            faf95=mt.faf[
                mt.faf_meta.index(lambda x: x.values() == ['adj', mt.popmax.pop])
            ].faf95
        )
    )
    return mt


def _annotate_quality_metrics_hist(mt: hl.MatrixTable) -> hl.Table:
    logging.info('Annotating quality metrics histograms...')
    # NOTE: these are performed here as the quality metrics histograms
    # also require densifying
    mt = mt.annotate_rows(qual_hists=qual_hist_expr(mt.GT, mt.GQ, mt.DP, mt.AD, mt.adj))
    ht = mt.rows()
    ht = ht.annotate(
        qual_hists=hl.Struct(
            **{
                i.replace('_adj', ''): ht.qual_hists[i]
                for i in ht.qual_hists
                if '_adj' in i
            }
        ),
        raw_qual_hists=hl.Struct(
            **{i: ht.qual_hists[i] for i in ht.qual_hists if '_adj' not in i}
        ),
    )
    return ht
