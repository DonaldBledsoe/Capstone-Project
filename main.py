
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery


table1 = "york-cdf-start.bigquerypython.bqtable1"
table2 = "york-cdf-start.bigquerypython.bqtable4"


if __name__ == '__main__':
    pipeline_options = PipelineOptions(
        temp_location='gs://db7_temp_loc/',
        # staging_location='gs://db7_temp_loc/',
        # save_main_session=True,
        # region='us-central1',
        # runner='DataflowRunner',
        # job_name='dons-job',
        # project='york-cdf-start',
        # dataset='bigquerypython'
    )

    # Schema definition for all of the payment order history tables
    table_schema = {
        'fields': [
            {'name': 'name', 'type': 'STRING', 'mode': 'nullable'},
            {'name': 'last_name', 'type': 'STRING', 'mode': 'nullable'}

        ]
    }

    # Table specification for the BQ tables
    table_usd_spec = bigquery.TableReference(
        projectId='york-cdf-start',
        datasetId='d_bledsoe_dataflow1',
        tableId='test_table2'
    )

    # table_gbp_spec = bigquery.TableReference(
    #     projectId='york-cdf-start',
    #     datasetId='d_bledsoe_dataflow1',
    #     tableId='gbp_order_payment_history'
    # )

    with beam.Pipeline(options=pipeline_options) as pipeline:
        data3 = (
                pipeline | 'Query tables' >> beam.io.ReadFromBigQuery(
                    query='SELECT table1.name, table2.last_name FROM york-cdf-start.bigquerypython.bqtable1 as table1 '
                           'join york-cdf-start.bigquerypython.bqtable4 as table2 on table1.order_id = table2.order_id', project="york-cdf-start",use_standard_sql=True)
        )


        # data = pipeline | "Read Table 1" >> beam.io.ReadFromBigQuery(table=table1) | beam.Map(print)
        # data2 = pipeline | "Read Table 2" >> beam.io.ReadFromBigQuery(table=table2) | beam.Map(print)

        # Outputting the tables to BigQuery.
        data3 | "Write1" >> beam.io.WriteToBigQuery(
            table_usd_spec,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,

        )

        # GBP_Orders | "Write2" >> beam.io.WriteToBigQuery(
        #     table_gbp_spec,
        #     schema=table_schema,
        #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        #     write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        #     method="STREAMING_INSERTS"
        #
        # )
        #
        # EUR_Orders | "Write3" >> beam.io.WriteToBigQuery(
        #     table_eur_spec,
        #     schema=table_schema,
        #     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        #     write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        #     method="STREAMING_INSERTS"
        # )
