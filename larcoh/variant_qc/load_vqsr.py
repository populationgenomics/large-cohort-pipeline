import hail as hl
import logging

from cpg_utils import Path
from cpg_utils.hail_batch import genome_build
from cpg_utils.workflows.utils import can_reuse


def run(
    site_only_vcf_path: Path,
    out_ht_path: Path,
):
    load_vqsr(site_only_vcf_path, out_ht_path)


def load_vqsr(
    site_only_vcf_path: Path,
    out_ht_path: Path,
) -> hl.Table:
    """
    Convert VQSR VCF to HT
    """
    if can_reuse(out_ht_path):
        return hl.read_table(str(out_ht_path))

    logging.info(
        f'AS-VQSR: importing annotations from a site-only VCF {site_only_vcf_path}'
    )
    ht = hl.import_vcf(
        str(site_only_vcf_path),
        reference_genome=genome_build(),
    ).rows()

    # VCF has SB fields as float in header:
    # > ##INFO=<ID=SB,Number=1,Type=Float,Description="Strand Bias">
    # Even though they are lists of ints, e.g. SB=6,11,2,0
    # Hail would fail to parse it, throwing:
    # > java.lang.NumberFormatException: For input string: "6,11,2,0"
    # To mitigate this, we can drop the SB field before the HT is (lazily) parsed.
    # In order words, dropping it before calling ht.write() makes sure that Hail would
    # never attempt to actually parse it.
    ht = ht.annotate(info=ht.info.drop('SB'))

    # Dropping also all INFO/AS* annotations as well as InbreedingCoeff, as they are
    # causing problems splitting multiallelics after parsing by Hail, when Hail attempts
    # to subset them by allele index, and running into index out of bounds:
    # `HailException: array index out of bounds: index=1, length=1`
    ht = ht.annotate(info=ht.info.drop(*[f for f in ht.info if f.startswith('AS_')]))

    unsplit_count = ht.count()
    ht = hl.split_multi_hts(ht)
    ht.write(str(out_ht_path), overwrite=True)
    ht = hl.read_table(str(out_ht_path))
    logging.info(f'Wrote split HT to {out_ht_path}')
    split_count = ht.count()
    logging.info(
        f'Found {unsplit_count} unsplit and {split_count} split variants with VQSR annotations'
    )
    return ht
