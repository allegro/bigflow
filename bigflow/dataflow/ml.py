### EXPERIMENTAL

import typing
import logging
import abc
import os

import pandas as pd

import apache_beam as beam


from . import io


logger = logging.getLogger(__name__)


def load_catboost_classifier(path: str, **kwargs):
    """Loads CatBoost classifier from single binary file (unpacked)"""

    import catboost
    logger.info("Load CatBoost model from %s, params %s", path, kwargs)

    m = catboost.CatBoostClassifier(**kwargs)
    with io.download_file_to_localfs(path) as fn:
        m.load_model(fn)

    return m


class BaseModel(abc.ABC):

    def __init__(self, model_path, model_opts=None, predict_opts=None):
        self.model_path = model_path
        self.model_opts = model_opts or {}
        self.predict_opts = predict_opts or {}
        self._model = None

    @abc.abstractmethod
    def load_model(self, path, **kwargs):
        raise NotImplementedError

    def ensure_model(self) -> typing.Any:
        if not self._model:
            self._model = self.load_model(self.model_path, **self.model_opts)
        return self._model

    def predict(self, X: pd.DataFrame):
        return pd.DataFrame(self.ensure_model().predict(X, **self.predict_opts))

    def __getstate__(self):
        # Don't tranfer model over network (pickling might not work for big models)
        d = self.__dict__.copy()
        d['_model'] = None
        return d


class CatBoostClassifierModel(BaseModel):

    def load_model(self, path, **kwargs):
        return load_catboost_classifier(path, **kwargs)

    def predict(self, X: pd.DataFrame):
        m = self.ensure_model()
        X = X.reset_index()
        Xs = X[m.feature_names_]
        return super().predict(Xs)


class MlOpsModelPredictor(BaseModel):

    def __init__(self, model_type, **kwargs):
        super().__init__(**kwargs)
        self.model_type = model_type
        assert not self.predict_opts

    def load_model(self, path, **kwargs):
        logger.info("Load mlops model %s from %s, opts %s", self.model_type, path, kwargs)
        m = self.model_type(**kwargs)
        with io.download_and_unpack_to_localfs(path) as d:
            logger.info("Load model from dir %s", d)
            m.load(d)
        return m

    def predict(self, X):
        request = {
            'instances': [
                {'values': list(x)}
                for x in X.values
            ],
        }
        response = self.ensure_model().predict(request)
        y = response['predictions']
        return pd.DataFrame(y)


def apply_model(df: pd.DataFrame, model: 'BaseModel', key_column: typing.Union[str, typing.List[str]]):
    logger.info("Process chunk, size %d", len(df))
    df = df.reset_index()
    i = df[key_column]
    y = model.predict(df)
    return pd.concat([i, y], axis=1)


@beam.ptransform_fn
def ApplyModel(
    pcoll: beam.PCollection[pd.DataFrame],
    model,
    key_column,
):
    return (pcoll
        | "Apply model"   >> beam.Map(apply_model, model=model, key_column=key_column)
    )