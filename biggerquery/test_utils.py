import os
import tempfile
from unittest import TestCase

import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
from avro.datafile import DataFileReader
from avro.io import DatumReader


class BeamTestCase(TestCase):
    def setUp(self):
        self.files_to_delete = []

    def cleanUp(self):
        for f in self.files_to_delete:
            os.remove(f)

    def empty_file(self, file_name_prefix):
        _, file_path = tempfile.mkstemp(prefix=file_name_prefix)
        self.files_to_delete.append(file_path)
        return file_path

    def read_from_avro(self, avro_file_path):
        return _read_from_avro(avro_file_path)

    def create_avro_file(self, schema, items, file_prefix):
        result_file_path =  _create_avro_file(schema, items, file_prefix)
        self.files_to_delete.append(result_file_path)
        return result_file_path


def _read_from_avro(avro_file_path):
    avro_file_path = _translate_to_sharded_avro_file_name(avro_file_path)
    reader = DataFileReader(open(avro_file_path, 'rb'), DatumReader())
    result = [s for s in reader]
    reader.close()
    return result


def _create_avro_file(schema, items, file_prefix):
    _, result_file_path = tempfile.mkstemp(prefix=file_prefix, suffix='.avro')
    parsed_schema = avro.schema.Parse(schema)
    with open(result_file_path, 'wb') as f:
        writer = DataFileWriter(f, DatumWriter(), parsed_schema)
        for s in items:
            writer.append(s)
        writer.close()
    return result_file_path


def _translate_to_sharded_avro_file_name(file_name):
    return file_name + '0_1.avro'