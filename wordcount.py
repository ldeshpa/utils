from __future__ import absolute_import

import argparse
import logging
import re
import google.cloud.dataflow as df


empty_line_aggregator = df.Aggregator('emptyLines')
average_word_size_aggregator = df.Aggregator('averageWordLength',
                                             df.combiners.MeanCombineFn(),
                                             float)

class WordExtractingDoFn(df.DoFn):
  def process(self, context):
    text_line = context.element.strip()
    if not text_line:
      context.aggregate_to(empty_line_aggregator, 1)
    words = re.findall(r'[A-Za-z\']+', text_line)
    for w in words:
      context.aggregate_to(average_word_size_aggregator, len(w))
    return words

def run(argv=None):

  parser = argparse.ArgumentParser()
  parser.add_argument('--input',
                      dest='input',
                      default='gs://dataflow-samples/shakespeare/kinglear.txt',
                      help='Input file to process.')
  parser.add_argument('--output',
                      dest='output',
                      required=True,
                      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  p = df.Pipeline(argv=pipeline_args)

  lines = p | df.io.Read('read', df.io.TextFileSource(known_args.input))
  counts = (lines
            | (df.ParDo('split', WordExtractingDoFn())
               .with_output_types(unicode))
            | df.Map('pair_with_one', lambda x: (x, 1))
            | df.GroupByKey('group')
            | df.Map('count', lambda (word, ones): (word, sum(ones))))
  output = counts | df.Map('format', lambda (word, c): '%s: %s' % (word, c))
  output | df.io.Write('write', df.io.TextFileSink(known_args.output))

  result = p.run()
  empty_line_values = result.aggregated_values(empty_line_aggregator)
  logging.info('number of empty lines: %d', sum(empty_line_values.values()))
  word_length_values = result.aggregated_values(average_word_size_aggregator)
  logging.info('average word lengths: %s', word_length_values.values())


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
