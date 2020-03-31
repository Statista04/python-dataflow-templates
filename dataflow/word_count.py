"""A streaming word-counting workflow.
"""

# pytype: skip-file

from __future__ import absolute_import

import argparse
import logging

from past.builtins import unicode

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.examples.wordcount import WordExtractingDoFn
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions


class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--input_file',
                            help='Input for the pipeline',
                            default='gs://my-bucket/input')
        parser.add_argument(
            '--output_file',
            help='Output for the pipeline',
            default='gs://my-bucket/output')
        parser.add_argument(
            '--input_subscription')


class Window(beam.PTransform):

    def __init__(self, window_value_in_seconds=None):
        self._window = window_value_in_seconds

    def expand(self, pcoll):
        if self._window:
            output_pcoll = pcoll | "Window" >> beam.WindowInto(window.FixedWindows(self._window))
        else:
            output_pcoll = pcoll | "Window" >> beam.WindowInto(window.GlobalWindows())

        return output_pcoll


class WriteToGCS(beam.PTransform):

    def __init__(self, gcs_path):
        beam.PTransform.__init__(self)
        self.gcs_path = gcs_path

    def expand(self, pcoll):
        return (pcoll | "WriteToGCS" >> beam.io.WriteToText(self.gcs_path)

                )


class WriteToBQ(beam.PTransform):

    def __init__(self, schema, dataset, table_name):
        beam.PTransform.__init__(self)
        self._schema = schema
        self._dataset = dataset
        self._table = table_name

    def expand(self, pcoll):
        output = self._dataset + '.' + self._table
        return (pcoll | "WriteToBQ" >> beam.io.WriteToBigQuery(
            output,
            schema=self.schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)

                )


class MapWordLine(beam.PTransform):

    def __init__(self, random_number):
        beam.PTransform.__init__(self)
        self.random_number = random_number

    def expand(self, pcoll):
        print(pcoll)
        return (pcoll | "Maptolist" >> beam.Map(lambda line: (line.split(',')))

                | "AddOne" >> beam.Map(lambda x: x + self.random_number * len(x))
                | "write" >> beam.io.WriteToText("test_output2.csv")
                )


def run(argv=None, save_main_session=True):
    """Build and run the pipeline."""

    pipeline_options = PipelineOptions()
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    pipeline_options.view_as(StandardOptions).streaming = True
    user_options = pipeline_options.view_as(MyOptions)

    with beam.Pipeline(options=pipeline_options) as p:

        # Read from PubSub into a PCollection.
        if user_options.input_subscription:
            messages = (
                    p
                    | beam.io.ReadFromPubSub(subscription=user_options.input_subscription).
                    with_output_types(bytes))
        else:
            messages = (
                    p
                    | beam.io.ReadFromText(
                user_options.input_file))

        """
        lines = messages | 'decode' >> beam.Map(lambda x: x.decode('utf-8'))
    
        """
        t1 = (messages | Window() )
        t2 =  messages  | MapWordLine([5])
        t3 = t2 | beam.Map(lambda k_v: '%s,%s' % (k_v[0], k_v[1]))


        t3 | beam.io.WriteToText(user_options.output_file)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
