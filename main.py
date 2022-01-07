
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery

#

if __name__ == '__main__':
    pipeline_options = PipelineOptions(
        temp_location='gs://db7_temp_loc/',
        staging_location='gs://db7_temp_loc/',
        save_main_session=True,
        region='us-central1',
        runner='DataflowRunner',
        job_name='dons-job',
        project='york-cdf-start',
        dataset='bigquerypython'
    )

    # Schema definition for first output table
    table_schema1 = {
        'fields': [
            {'name': 'name', 'type': 'STRING', 'mode': 'nullable'},
            {'name': 'last_name', 'type': 'STRING', 'mode': 'nullable'}

        ]
    }

    # Schema definition for second output table
    table_schema2 = {
        'fields': [
            {'name': 'order_id', 'type': 'INTEGER', 'mode': 'nullable'},
            {'name': 'last_name', 'type': 'STRING', 'mode': 'nullable'}

        ]
    }

    # Table specification for the BQ tables
    table_spec1 = bigquery.TableReference(
        projectId='york-cdf-start',
        datasetId='d_bledsoe_dataflow1',
        tableId='test_table4'
    )

    table_spec2 = bigquery.TableReference(
        projectId='york-cdf-start',
        datasetId='d_bledsoe_dataflow1',
        tableId='test_table5'
    )

    with beam.Pipeline(options=pipeline_options) as pipeline:           # Join for first two tables
        data3 = (
                pipeline | 'Query tables' >> beam.io.ReadFromBigQuery(
                    query='SELECT table1.name, table2.last_name FROM york-cdf-start.bigquerypython.bqtable1 as table1 '
                        'join york-cdf-start.bigquerypython.bqtable4 as table2 on table1.order_id = table2.order_id', project="york-cdf-start", use_standard_sql=True)
        )

        data4 = (
                pipeline | 'Query tables2' >> beam.io.ReadFromBigQuery(
                    query='SELECT table1.order_id, table2.last_name FROM york-cdf-start.bigquerypython.bqtable1 as table1 '
                        'join york-cdf-start.bigquerypython.bqtable4 as table2 on table1.order_id = table2.order_id', project="york-cdf-start", use_standard_sql=True)
            )

        # Outputting the two tables to BigQuery.
        data3 | "Write1" >> beam.io.WriteToBigQuery(
            table_spec1,
            schema=table_schema1,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,

        )

        data4 | "Write2" >> beam.io.WriteToBigQuery(
            table_spec2,
            schema=table_schema2,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,

        )
