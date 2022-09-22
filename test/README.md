To prepare data:

```bash
# Prepare GVCFs
for s in CPG99994 CPG99986 CPG99978 CPG99960 CPG99952 CPG99945 CPG99937 CPG99929 CPG99911 CPG99903
do 
  bcftools view -R intervals.bed gs://cpg-thousand-genomes-main/gvcf/$s.g.vcf.gz -Oz -o gvcf/$s.g.vcf.gz 
  tabix gvcf/$s.g.vcf.gz
done

# Prepare reference
python prep_reference_tables.py
mkdir data
gsutil cp -r gs://cpg-reference/subset-toy-chr20-X-Y data/reference

mkdir hg38/v0
gsutil cp gs://cpg-reference/hg38/v0/wgs_calling_regions.hg38.interval_list tmp-wgs_calling_regions.hg38.interval_list
picard BedToIntervalList -I intervals.bed \
--SEQUENCE_DICTIONARY /Users/vlad/bio/hg38/Homo_sapiens_assembly38.dict \
-O intervals.interval_list
picard IntervalListTools --ACTION INTERSECT \
-I tmp-wgs_calling_regions.hg38.interval_list \
-I intervals.interval_list \
-O ./reference/hg38/v0/wgs_calling_regions.hg38.interval_list
```
