from google.cloud import bigquery

print('Big Query client connect')
client = bigquery.Client(project='dataeng-219316')

# list datasets
print('Big Query  list dat sets in the project')
project = client.project
datasets = list(client.list_datasets())
if datasets:
  print('Datasets in project {}:'.format(project))
  for dataset in datasets:
     print('\t{}'.format(dataset.dataset_id))
else:
  print('{} project does not contain any datasets.'.format(project))

print('Query gsod data')

query_job = client.query("""
            SELECT
                 temp,
                 da,
                 mo,
                 year
                 FROM `bigquery-public-data.noaa_gsod.gsod1929`
                 ORDER BY temp DESC
                 LIMIT 10""")

results = query_job.result()  # Waits for job to complete
for row in results:
        print(" On day {}-{}-{}, Temp is {}".format(row.year, row.mo, row.da, row.temp))



