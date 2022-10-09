# Large Cohort Workflow

A [Hail Query](https://hail.is/) workflow for large germline genomic variant calling cohorts.

1. Combine GVCFs (generated by GATK4) into a [VDS](https://hail.is/docs/0.2/vds/hail.vds.VariantDataset.html#hail.vds.VariantDataset).
2. Perform sample-level QC, including sex and ancestry inference.
3. Perform variant-level QC, including [allele-specific VQSR](https://gatk.broadinstitute.org/hc/en-us/articles/360035890551-Allele-specific-annotation-and-filtering-of-germline-short-variants).

## Usage

```sh
analysis-runner --dataset prophecy --access-level test --description "test larcoh" --output-dir "seqr-loader-test" \
--config configs/prophecy-test.toml \
main.py
```

The workflow will find input GVCFs in the `main` bucket, write the resulting VDS into the `test` bucket: `gs://cpg-prophecy-test/vds`, and other annotation tables into `gs://cpg-prophecy-test/larcoh`, specifically:

* `gs://cpg-prophecy-test/vds/v01.vds` raw combined GVCFs in sparse dataset format,
* `gs://cpg-prophecy-test/larcoh/v01/sample_qc.ht` sample-level metadata and QC:
  * Input metadata,
  * Inferred sex,
  * `hl.sample_qc`
* `gs://cpg-prophecy-test/larcoh/v01/relatedness.ht` pairwise sample relatedness pcrelate results,
* `gs://cpg-prophecy-test/larcoh/v01/relateds_to_drop.ht` related samples to drop, with top ranking sample selected from each family,
* `gs://cpg-prophecy-test/larcoh/v01/vqsr.ht` AS-VQSR results,
* `gs://cpg-prophecy-test/larcoh/v01/frequencies.ht` row-level 
* `gs://cpg-prophecy-test/larcoh/v01/ancestry` PCA results:
  * `gs://cpg-prophecy-test/larcoh/v01/ancestry/eigenvalues.ht`
  * `gs://cpg-prophecy-test/larcoh/v01/ancestry/loadings.ht`
  * `gs://cpg-prophecy-test/larcoh/v01/ancestry/scores.ht`
  * `gs://cpg-prophecy-test/larcoh/v01/ancestry/inferred_pop.ht` - sample-level table with inferred population labels (`ht.pop`) from the PCA analysis, as long there are samples with known `continental_pop` provided
* `gs://cpg-prophecy-test-web/larcoh/v01/ancestry/*` - bucket with PCA and loadings plots, accessible as https://test-web.populationgenomics.org.au/prophecy/larcoh/ancestry/*
* `gs://cpg-prophecy-test/larcoh/v01/dense-subset.mt` dense matrix table used for PCA, relatedness, and sex inferring.

## Overview of the pipeline steps

1. Find inputs from metamist.

1. Run the GVCF combiner using `scripts/combine_gvcfs.py`. The script merges GVCFs into a sparse VDS format using [Hail's vcf_combiner](https://hail.is/docs/0.2/experimental/vcf_combiner.html).

1. Run the `scripts/sample_qc.py` script, that performs the [sample-level QC](#sample-qc), and generates a Table with the filtered sample IDs, as well as a metadata Table with metrics that were used for filtering (coverage, sex, ancestry, contamination, variant numbers/distributions, etc.).

1. Run the [allele-specific VQSR approach](#allele-specific-vqsr) to perform the variant filtration.

## Sample QC

The sample QC and random forest variant QC pipelines are largely a re-implementation and orchestration of [the Hail methods used for the quality control of GnomAD release](https://github.com/broadinstitute/gnomad_qc). Good summaries of gnomAD QC pipeline can be found in gnomAD update blog posts:

* [https://macarthurlab.org/2017/02/27/the-genome-aggregation-database-gnomad](https://macarthurlab.org/2017/02/27/the-genome-aggregation-database-gnomad)
* [https://macarthurlab.org/2018/10/17/gnomad-v2-1](https://macarthurlab.org/2018/10/17/gnomad-v2-1)
* [https://macarthurlab.org/2019/10/16/gnomad-v3-0](https://macarthurlab.org/2019/10/16/gnomad-v3-0)
* [https://gnomad.broadinstitute.org/blog/2020-10-gnomad-v3-1-new-content-methods-annotations-and-data-availability/#sample-and-variant-quality-control](https://gnomad.broadinstitute.org/blog/2020-10-gnomad-v3-1-new-content-methods-annotations-and-data-availability/#sample-and-variant-quality-control)
* [https://blog.hail.is/whole-exome-and-whole-genome-sequencing-recommendations/](https://blog.hail.is/whole-exome-and-whole-genome-sequencing-recommendations/)

Here we give a brief overview of the sample QC steps:

   1. Compute sample QC metrics using Hail’s [`sample_qc`](https://hail.is/docs/0.2/methods/genetics.html#hail.methods.sample_qc) module on all autosomal bi-allelic SNVs.

   1. Filter outlier samples using the following cutoffs. Note that the most up to date cutoffs are speified in the configuration file [filter_cutoffs.yaml](joint_calling/filter_cutoffs.yaml), which can be overridden with `--filter-cutoffs-file`.

   1. Filter using BAM-level metrics was performed when such metrics were available. We removed samples that were outliers for:

      * Contamination: freemix > 5% (`call-UnmappedBamToAlignedBam/UnmappedBamToAlignedBam/*/call-CheckContamination/*.selfSM`/`FREEMIX`)
      * Chimeras: > 5% (`call-AggregatedBamQC/AggregatedBamQC/*/call-CollectAggregationMetrics/*.alignment_summary_metrics`/`PCT_CHIMERAS`)
      * Duplication: > 30% (`call-UnmappedBamToAlignedBam/UnmappedBamToAlignedBam/*/call-MarkDuplicates/*.duplicate_metrics`/`PERCENT_DUPLICATION`)
      * Median insert size: < 250 (`call-AggregatedBamQC/AggregatedBamQC/*/call-CollectAggregationMetrics/*.insert_size_metrics`/`MEDIAN_INSERT_SIZE`)
      * Median coverage < 18X (calculated from the GVCFs).

   1. Sex inferred for each sample with Hail's [`impute_sex`](https://hail.is/docs/0.2/methods/genetics.html?highlight=impute_sex#hail.methods.impute_sex). Filter samples with sex chromosome aneuploidies or ambiguous sex assignment.

   1. Note that all filtering above makes it exclude samples from the variant QC modelling, as well as from the AC/AF/AN frequency calculation. However, it keeps the samples in the final matrix table, with labels in `mt.meta.hardfilter`.

   1. Relatedness inferred between samples using Hail's[`pc_relate`](https://hail.is/docs/0.2/methods/genetics.html?highlight=pc_relate#hail.methods.pc_relate). Identified pairs of 1st and 2nd degree relatives. Filter to a set of unrelated individuals using Hail's [`maximal_independent_set`](https://hail.is/docs/0.2/methods/misc.html?highlight=maximal_independent_set#hail.methods.maximal_independent_set) that tries to keep as many samples as possible. When multiple samples could be selected, we kept the sample with the highest coverage.
   
   1. PCA was a ran on high-quality variants, and RF was trained using 16 principal components as features on samples with known ancestry. Ancestry was assigned to all samples for which the probability of that ancestry was >75%.
   
   1. Hail [`sample_qc`](https://hail.is/docs/0.2/methods/genetics.html#hail.methods.sample_qc) was used stratified by 8 ancestry assignment PCs. Within each PC, outliers were filtered if they are 4 median absolute deviations (MADs) away from the median for the following metrics: `n_snp`, `r_ti_tv`, `r_insertion_deletion`, `n_insertion`, `n_deletion`, `r_het_hom_var`, `n_het`, `n_hom_var`, `n_transition`, `n_transversion`, or 8 MADs away from the median number of singletons (`n_singleton` metric).


## Allele-specific variant quality score recalibration (AS-VQSR)

   1. Export variants into a sites-only VCF and split it into SNPs and indels, as well as region-wise for parallel processing.
   
   1. Run Gnarly Genotyper to perform "quick and dirty" joint genotyping.
   
   1. Create SNP and indel recalibration models using the allele-specific version of GATK Variant Quality Score Recalibration [VQSR](https://gatkforums.broadinstitute.org/gatk/discussion/9622/allele-specific-annotation-and-filtering), using the standard GATK training resources (HapMap, Omni, 1000 Genomes, Mills indels), with the following features:
   
      * SNVs:   `AS_FS`, `AS_SOR`, `AS_ReadPosRankSum`, `AS_MQRankSum`, `AS_QD`, `AS_MQ`
      * Indels: `AS_FS`, `AS_SOR`, `AS_ReadPosRankSum`, `AS_MQRankSum`, `AS_QD`
      * No sample had a high quality genotype at this variant site (GQ>=20, DP>=10, and AB>=0.2 for heterozygotes) (all fields are populated by GATK)
      * `InbreedingCoeff` < -0.3 (there was an excess of heterozygotes at the site compared to Hardy-Weinberg expectations) (`InbreedingCoeff` is populated by GATK)
   
   1. Apply the models to the VCFs and combine them back into one VCF.
   
   1. Import the VCF back to a matrix table.
   
   VQSR pipeline is a compilation from the following 2 WDL workflows:
   
   1. `hail-ukbb-200k-callset/GenotypeAndFilter.AS.wdl`
   1. The [Broad VQSR workflow](https://github.com/broadinstitute/warp/blob/develop/pipelines/broad/dna_seq/germline/joint_genotyping/JointGenotyping.wdl) documented [here](https://gatk.broadinstitute.org/hc/en-us/articles/360035531112--How-to-Filter-variants-either-with-VQSR-or-by-hard-filtering), translated from WDL with a help of [Janis](https://github.com/PMCC-BioinformaticsCore/janis).
