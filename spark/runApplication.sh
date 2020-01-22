 ./spark/bin/spark-submit \
  --class de.hpi.spark_tutorial.Sindy \
  --deploy-mode client \
  --master local[8] \
  ./target/SparkTutorial-1.0.jar \
  --path data --cores 20
