

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery



topic_id = "projects/york-cdf-start/topics/dataflow-order-stock-update"     # topic for WriteToPubSub

if __name__ == '__main__':

    # Schema definition for all of the payment order history tables
    table_schema = {
        'fields': [
            {'name': 'order_id', 'type': 'INTEGER', 'mode': 'nullable'},
            {'name': 'order_address', 'type': 'RECORD', 'mode': 'Repeated',
                'fields': [
                    {'name': 'order_building_num', 'type': 'STRING', 'mode': 'nullable'},
                    {'name': 'order_street_name', 'type': 'STRING', 'mode': 'nullable'},
                    {'name': 'order_city', 'type': 'STRING', 'mode': 'nullable'},
                    {'name': 'order_state_code', 'type': 'STRING', 'mode': 'nullable'},
                    {'name': 'order_zip_code', 'type': 'STRING', 'mode': 'nullable'},
            ],
            },

            {'name': 'customer_first_name', 'type': 'STRING', 'mode': 'nullable'},
            {'name': 'customer_last_name', 'type': 'STRING', 'mode': 'nullable'},
            {'name': 'customer_ip', 'type': 'STRING', 'mode': 'nullable'},
            {'name': 'cost_total', 'type': 'Float', 'mode': 'nullable'},
        ]
    }

    # Table specification for the BQ tables
    table_usd_spec = bigquery.TableReference(
        projectId='york-cdf-start',
        datasetId='d_bledsoe_dataflow1',
        tableId='usd_order_payment_history'
    )

    table_gbp_spec = bigquery.TableReference(
        projectId='york-cdf-start',
        datasetId='d_bledsoe_dataflow1',
        tableId='gbp_order_payment_history'
    )

    table_eur_spec = bigquery.TableReference(
        projectId='york-cdf-start',
        datasetId='d_bledsoe_dataflow1',
        tableId='eur_order_payment_history'
    )

    with beam.Pipeline(options=PipelineOptions()) as pipeline:
        orders = pipeline | beam.io.ReadFromBigQuery()


        # separating the orders by currency
        usd_split = total_cost | beam.Filter(usd)          #USD orders

        gbp_split = total_cost | beam.Filter(gbp)          # GBP orders

        eur_split = total_cost | beam.Filter(eur)          # Eur orders




        # Outputting the tables to BigQuery.
        USD_Orders | "Write1" >> beam.io.WriteToBigQuery(
            table_usd_spec,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            method="STREAMING_INSERTS"
        )

        GBP_Orders | "Write2" >> beam.io.WriteToBigQuery(
            table_gbp_spec,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            method="STREAMING_INSERTS"

        )

        EUR_Orders | "Write3" >> beam.io.WriteToBigQuery(
            table_eur_spec,
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            method="STREAMING_INSERTS"
        )