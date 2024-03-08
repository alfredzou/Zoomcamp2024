URL="spark://batch-vm.australia-southeast1-b.c.sharp-harbor-411301.internal:7077"

spark-submit \
     --master="${URL}" \
     cluster_spark.py \
          --input_yellow=./parquet/yellow/2021/* \
          --input_green=./parquet/green/2021/* \
          --output=./pq_combined/2021