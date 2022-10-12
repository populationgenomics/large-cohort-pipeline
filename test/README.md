To prepare data:

```bash
mkdir data

# Prepare GVCFs
export GCS_OAUTH_TOKEN=$(gcloud auth application-default print-access-token)
mkdir -p data/gvcf
for s in CPG99994 CPG99986 CPG99978 CPG99960 CPG99952 CPG99945 CPG99937 CPG99929 CPG99911 CPG99903
do 
  bcftools view -R data/intervals.bed gs://cpg-thousand-genomes-main/gvcf/$s.g.vcf.gz -Oz -o data/gvcf/$s.g.vcf.gz 
  tabix data/gvcf/$s.g.vcf.gz
done

# Prepare reference
python prep-ref-tables.py
gsutil cp -r gs://cpg-reference/subset-toy-chr20-X-Y/ data/reference/

mkdir -p data/reference/hg38/v0
gsutil cp gs://cpg-reference/hg38/v0/wgs_calling_regions.hg38.interval_list \
  data/tmp-wgs_calling_regions.hg38.interval_list
picard BedToIntervalList -I data/intervals.bed \
  --SEQUENCE_DICTIONARY /Users/vlad/bio/hg38/Homo_sapiens_assembly38.dict \
  -O data/intervals.interval_list
picard IntervalListTools --ACTION INTERSECT \
  -I data/tmp-wgs_calling_regions.hg38.interval_list \
  -I data/intervals.interval_list \
  -O data/reference/hg38/v0/wgs_calling_regions.hg38.interval_list

mkdir -p data/reference/gencode
bedtools intersect -wa \
  -a <(gsutil cat gs://cpg-reference/gencode/gencode.v39.annotation.gtf.bgz | gunzip -c) \
  -b data/intervals.bed > data/reference/gencode/gencode.v39.annotation.gtf
bgzip data/reference/gencode/gencode.v39.annotation.gtf -c > data/reference/gencode/gencode.v39.annotation.gtf.bgz
rm data/reference/gencode/gencode.v39.annotation.gtf
```
