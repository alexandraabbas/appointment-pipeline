"""
python -m streaming_pipeline 
    --subscription=projects/{project}/subscriptions/{subscription}
    --bigquery_table={project}:{datatset}.{table}
    --bigquery_table_for_failed_rows={project}:{datatset}.{table}
    --streaming
"""

import argparse
import logging
import json

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io import WriteToText
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions


def parse(element):
    return json.loads(element)


def tuple_to_dict(element):
    return {"FailedRow": json.dumps(element)}


class ValidateMessages(beam.DoFn):
    OUTPUT_TAG = "invalid_messages"

    def process(self, element):
        try:
            yield parse(element)
        except:
            yield pvalue.TaggedOutput(self.OUTPUT_TAG, element)


# class WriteToBigQuery(beam.PTransform):
#     pass


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--subscription",
        dest="subscription",
        required=True,
        help='Input PubSub subscription of the form "projects/<PROJECT>/subscriptions/<SUBSCRIPTION>".',
    )
    parser.add_argument(
        "--bigquery_table",
        dest="bigquery_table",
        required=True,
        help="The fully-qualified BigQuery table to which to write.",
    )
    parser.add_argument(
        "--bigquery_table_for_failed_rows",
        dest="bigquery_table_for_failed_rows",
        required=True,
        help="The fully-qualified BigQuery table to which to write failed inserts.",
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True

    p = beam.Pipeline(options=pipeline_options)
    # yapf: disable
    messages = (
        p
        | "ReadFromPubSub" >> ReadFromPubSub(subscription=known_args.subscription).with_output_types(bytes)
        | "ParseAndValidateMessages" >> beam.ParDo(ValidateMessages())
            .with_outputs(ValidateMessages.OUTPUT_TAG, main="valid_messages"))

    valid_messages = messages["valid_messages"]
    invalid_messages = messages[ValidateMessages.OUTPUT_TAG] | "TupleToDict" >> beam.Map(tuple_to_dict)

    invalid_messages | "WriteFailedRowsToBigQuery" >> WriteToBigQuery(
            known_args.bigquery_table_for_failed_rows,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER)

    valid_messages | beam.FlatMap(lambda message: logging.info(message))

    failed_rows = (
        valid_messages 
        | "WriteToBigQuery" >> WriteToBigQuery(
            known_args.bigquery_table,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER))

    failed_rows_pcoll = failed_rows["FailedRows"] | "TupleToDict" >> beam.Map(tuple_to_dict)

    failed_rows_pcoll | "WriteFailedRowsToBigQuery" >> WriteToBigQuery(
            known_args.bigquery_table_for_failed_rows,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER)
    # yapf: enable
    result = p.run()
    result.wait_until_finish()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
