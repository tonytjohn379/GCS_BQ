import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


bigquery_schema = 'id:INTEGER,name:STRING'


options = PipelineOptions(
    runner='DataflowRunner',
    project='tony1-508206',
    region='us-east1',
    temp_location='gs://profile_bucket1/temp',
    staging_location='gs://profile_bucket1/staging'
)


def parse_line(line):
    fields = line.split(',')

    return {
        'id': int(fields[0]),
        'name': fields[1]
    }


with beam.Pipeline(options=options) as p:

    (
        p
        | 'Read File from GCS'
        >> beam.io.ReadFromText(
            'gs://profile_bucket1/source_file/sample.csv',
            skip_header_lines=1
        )

        | 'Parse CSV'
        >> beam.Map(parse_line)

        | 'Write to BigQuery' 
        >> beam.io.WriteToBigQuery(
            table='tony1-508206:project1.profile',
            schema=bigquery_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )
    )