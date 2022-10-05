import hail as hl
import logging
from gnomad.utils.sparse_mt import split_info_annotation

from larcoh import parameters


def load_vqsr():
    vqsr_prefix = parameters.tmp_prefix / 'vqsr'
    vqsr_site_only_vcf_path = vqsr_prefix / 'vqsr.vcf.gz'
    output_ht_path = vqsr_prefix / 'vqsr.ht'

    logging.info(f'Importing VQSR annotations...')
    mt = hl.import_vcf(
        vqsr_site_only_vcf_path,
        force_bgz=True,
        reference_genome='GRCh38',
    )

    ht = mt.rows()

    # some numeric fields are loaded as strings, so converting them to ints and floats
    ht = ht.annotate(
        info=ht.info.annotate(
            AS_VQSLOD=ht.info.AS_VQSLOD.map(hl.float),
            AS_SB_TABLE=ht.info.AS_SB_TABLE.split(r'\|').map(
                lambda x: hl.if_else(
                    x == '', hl.missing(hl.tarray(hl.tint32)), x.split(',').map(hl.int)
                )
            ),
        ),
    )
    unsplit_count = ht.count()

    ht = hl.split_multi_hts(ht)
    ht = ht.annotate(
        info=ht.info.annotate(**split_info_annotation(ht.info, ht.a_index)),
    )
    ht = ht.annotate(
        filters=ht.filters.union(hl.set([ht.info.AS_FilterStatus])),
    )
    ht.write(output_ht_path, overwrite=True)
    ht = hl.read_table(output_ht_path)
    logging.info(f'Wrote split HT to {output_ht_path}')
    split_count = ht.count()
    logging.info(
        f'Found {unsplit_count} unsplit and {split_count} split variants with VQSR annotations'
    )
