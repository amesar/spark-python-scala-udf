
# Run Databricks job

. ./setup.env
curl -X POST -H "Authorization: Bearer $TOKEN" -d @run_submit.json $API_URL/jobs/runs/submit
