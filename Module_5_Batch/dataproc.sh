# GUI args
--input_yellow=gs://sharp-harbor-411301-bucket/pq/yellow/2021/*
--input_green=gs://sharp-harbor-411301-bucket/pq/green/2021/*
--output=gs://sharp-harbor-411301-bucket/pq_combined/2021

# GCLOUD to GCS
gcloud dataproc jobs submit pyspark \
     --cluster=spark-cluster \
     --region=australia-southeast1 \
     gs://sharp-harbor-411301-bucket/code/cluster_spark_submit.py \
     -- \
          --input_yellow=gs://sharp-harbor-411301-bucket/pq/yellow/2021/* \
          --input_green=gs://sharp-harbor-411301-bucket/pq/green/2021/* \
          --output=gs://sharp-harbor-411301-bucket/pq_combined/2021

# GCLOUD to BigQuery
gcloud dataproc jobs submit pyspark \
     --cluster=spark-cluster \
     --region=australia-southeast1 \
     gs://sharp-harbor-411301-bucket/code/big_query_spark.py \
     -- \
          --input_yellow=gs://sharp-harbor-411301-bucket/pq/yellow/2021/* \
          --input_green=gs://sharp-harbor-411301-bucket/pq/green/2021/* \
          --output=trips_data_all.report_2021