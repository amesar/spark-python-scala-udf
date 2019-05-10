
. ./setup.env
path=/tmp/jobs/python-scala-udf-job
curl -X POST -H "Authorization: Bearer $TOKEN" -F path=$path $API_URL/dbfs/mkdirs
