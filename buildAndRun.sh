# navigate to the project root directory
# cd /path/to/project
# bash buildAndRun.sh /path/to/resources

INPUT_RESOURCES_PATH=$1

# build docker image
docker build -t spark-sql-batch-processing-python .

# run docker container
docker run \
#-v "/Users/nassimbouali/MyRepos/Data Spark SQL Engine Python/resources":/resources \
-v $INPUT_RESOURCES_PATH:/resources \
-it spark-sql-batch-processing-python \
spark-submit \
--jars /resources/lib/postgresql-42.7.1.jar \
/app/application.py \
--inline \
--plan "/resources/configuration-deployment-csv-to-jdbc.json"
