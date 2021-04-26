### EXPERIMENTAL

import tempfile
import shutil
import logging
import os.path
import contextlib
import typing
import os
import csv
from apache_beam.pvalue import PCollection

import pandas as pd

import apache_beam as beam
import apache_beam.io as beam_io

import apache_beam.dataframe as df
import apache_beam.dataframe.io as df_io
import apache_beam.dataframe.convert as df_convert


from apache_beam.io.filesystems import FileSystems


logger = logging.getLogger(__name__)


@contextlib.contextmanager
def download_file_to_localfs(path: str):
    """Downloads single file from external filesystem (gcs, s3 etc)"""

    logger.info("Download file %s to local fs", path)
    with FileSystems.open(path) as a, tempfile.TemporaryDirectory() as d:
        fn = os.path.join(d, path.split("/", 2)[-1])
        os.makedirs(os.path.dirname(fn))
        with open(fn, 'bw+') as f:
            shutil.copyfileobj(a, f)
        yield fn


@contextlib.contextmanager
def download_and_unpack_to_localfs(path: str):
    """Download and unpack archive from external filesystem (s3://, gs://, etc)."""

    with tempfile.TemporaryDirectory() as tmpdir:
        with download_file_to_localfs(path) as fn:
            shutil.unpack_archive(fn, tmpdir)
        yield tmpdir


def _to_dataframe(x):
    return pd.DataFrame(x)


@beam.ptransform_fn
def ReadCSVFilesPlain(
    p: beam.Pipeline,
    file_pattern: str,
    fieldnames: typing.List[str],
):
    return (p
        | "Read files csv files"      >> beam_io.ReadFromText(file_pattern=file_pattern, skip_header_lines=1)
        | "Chunk for parsing"         >> beam.BatchElements()
        | "Parse csv lines to dicts"  >> beam.FlatMap(lambda x: map(dict, csv.DictReader(x, fieldnames=fieldnames)))
        | "Chunk for processing"      >> beam.BatchElements()
        | "Convert chunks to DF"      >> beam.Map(_to_dataframe)
    )


@beam.ptransform_fn
def WritePandasToCSV(
    pcoll: PCollection[pd.DataFrame],
    file_path_prefix: str,
    **kwargs,
):
    return (pcoll
        | "Convert DFs to tuples"   >> beam.FlatMap(lambda df: map(list, df.values))
        | "Convert to csv lines"    >> beam.MapTuple(lambda *args: ",".join(map(str, args)))
        | "Write results to csv"    >> beam_io.WriteToText(file_path_prefix=file_path_prefix, **kwargs)
    )


@beam.ptransform_fn
def ReadCSVToPandas(
    p: beam.Pipeline,
    *args,
    **kwargs,
) -> PCollection[pd.DataFrame]:
    data = p | "Read CSV" >> df_io.read_csv(*args, **kwargs)
    return df_convert.to_pcollection(data, yield_elements='pandas')
