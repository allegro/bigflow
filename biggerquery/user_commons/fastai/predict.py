import apache_beam as beam
from apache_beam.pvalue import AsSingleton


def side_input_factory(p, item, label_name):
    return AsSingleton(p
                       | label_name + ', creating collection' >> beam.Create([item])
                       | label_name + ', combining globally' >> beam.CombineGlobally(beam.combiners.ToListCombineFn()))


class PartitionTable(beam.DoFn):
    def process(self, element, collection_size_side_input):
        import random
        number_of_partitions = collection_size_side_input // 100000
        yield str(random.randrange(0, number_of_partitions + 1, 1)), element


def default_predict(variables_partition,
                    model_side_input):
    from fastai.tabular import load_learner
    import io
    import pandas as pd
    variables_partition = variables_partition[1]

    df = pd.DataFrame.from_records(variables_partition)

    learn = load_learner('/some/path', file=io.BytesIO(model_side_input[0]))

    predictions = [bool(learn.predict(r)[1]) for _, r in df.iterrows()]
    df['prediction'] = predictions

    for _, object_variables in df.iterrows():
        yield object_variables.to_dict()


class PredictOfferPotential(beam.DoFn):
    def process(self, variables_partition, model_side_input):
        yield from default_predict(variables_partition=variables_partition, model_side_input=model_side_input)


def run(p, input_collection, output, model_bytes):
    model_side_input = side_input_factory(p, model_bytes, 'creating model side input')
    collection_size_side_input = AsSingleton(p
                       | 'reading input collection for size calculation' >> input_collection
                       | 'calculating input collection size' >> beam.combiners.Count.Globally())

    p | 'loading variables' >> input_collection \
    | 'creating partitions' >> beam.ParDo(
        PartitionTable(),
        collection_size_side_input=collection_size_side_input) \
    | 'grouping partitions' >> beam.GroupByKey() \
    | 'predicting' >> beam.ParDo(
        PredictOfferPotential(),
        model_side_input=model_side_input) \
    | 'saving predictions' >> output

    p.run().wait_until_finish()


if __name__ == '__main__':
    run(**globals()['run_kwargs'])