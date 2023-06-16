import argparse
import requests
import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

def get_geojson_data(url):
    response = requests.get(url)
    return response.json()

# Define a DoFn to yield each feature from the GeoJSON data
class GeoJSONToFeature(beam.DoFn):
    def process(self, element):
        features = element.get('features', [])
        for feature in features:
            yield feature

def run_pipeline(project, dataset, table, temp_location, region, url, output_csv_path, job_name):
    # Define and initialize pipeline options
    pipeline_options = PipelineOptions(
        runner='DataflowRunner',
        project=project,
        temp_location=temp_location,
        region=region,
        job_name=job_name
    )

    # Create a Pipeline
    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Read data from the GeoJSON URL
        geojson_data = (
            pipeline
            | 'Generate URL' >> beam.Create([url])
            | 'Generate GeoJSON' >> beam.Map(get_geojson_data)
        )

        # Extract features from the GeoJSON data
        features = geojson_data | 'Extract Features' >> beam.ParDo(GeoJSONToFeature())

        # Define the schema dynamically from the features
        schema = features | 'Get Schema' >> beam.schema.GenerateSchema()

        # Write the features to BigQuery table
        features | 'Write to BigQuery' >> WriteToBigQuery(
            table=f'{dataset}.{table}',
            schema=schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND  # Append to the table for streaming
        )

        # Write the features to Google Cloud Storage in CSV format
        features | 'Write to CSV' >> WriteToText(
            file_path_prefix=output_csv_path,
            file_name_suffix='.csv',
            header=True,
            shard_name_template=''
        )

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Apache Beam Pipeline')
    parser.add_argument('--project', default='your_project', help='GCP Project ID')
    parser.add_argument('--dataset', default='your_dataset', help='BigQuery Dataset')
    parser.add_argument('--table', default='your_table', help='BigQuery Table')
    parser.add_argument('--temp_location', default='gs://your_bucket/temp', help='GCS Temporary Location')
    parser.add_argument('--region', default='your_region', help='GCP Region')
    parser.add_argument('--url', default='https://api.data.gov.sg/v1/transport/taxi-availability', help='GeoJSON URL')
    parser.add_argument('--output_csv_path', default='gs://your_bucket/output', help='Output CSV path')
    parser.add_argument('--job_name', default='your_job_name', help='Job name for Apache Beam')

    args = parser.parse_args()

    run_pipeline(args.project, args.dataset, args.table, args.temp_location, args.region, args.url, args.output_csv_path, args.job_name)

# python my_script.py --job_name my_beam_job
# use Dataflow google cloud scheduler to schedule this job on 1 minute interval
