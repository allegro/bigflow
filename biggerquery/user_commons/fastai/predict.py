import apache_beam as beam
from apache_beam.pvalue import AsSingleton


def side_input_factory(p, item, label_name):
    return AsSingleton(p
                       | label_name + ', creating collection' >> beam.Create([item])
                       | label_name + ', combining globally' >> beam.CombineGlobally(beam.combiners.ToListCombineFn()))


class Predictor(beam.DoFn):
    def process(self, element, model_side_input):
        if not hasattr(self, 'learn'):
            from fastai.tabular import load_learner
            import io
            self.learn = load_learner('/some/path', file=io.BytesIO(model_side_input[0]))

        element['prediction'] = self.learn.predict(element)[1].item()
        yield element


def run(p, input_collection, output, model_bytes):
    model_side_input = side_input_factory(p, model_bytes, 'creating model side input')

    p | 'loading variables' >> input_collection \
    | 'predicting' >> beam.ParDo(
        Predictor(),
        model_side_input=model_side_input) \
    | 'saving predictions' >> output

    p.run().wait_until_finish()


if __name__ == '__main__':
    run(**globals()['run_kwargs'])