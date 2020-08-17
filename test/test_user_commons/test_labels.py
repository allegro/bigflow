from unittest import TestCase

from bigflow.bigquery.interactive import InteractiveComponent
from bigflow.bigquery.interactive import add_label_component


class FakeTable(object):
    def __init__(self, table_name):
        self.labels = {}
        self.table_name = table_name


class FakeClient(object):

    def get_table(self, table_name):
        return FakeTable(table_name)

    def update_table(self, table, fields):
        return table.table_name, table.labels


class FakeDataset(object):
    def __init__(self, project_id, dataset_name):
        self.project_id = project_id
        self.dataset_name = dataset_name

    @property
    def client(self):
        return FakeClient()

    @property
    def config(self):
        return {}


TEST_LABELS = {'my_label': 'test_label'}
TEST_PROJECT_ID = 'test_project'
TEST_DATASET_NAME = 'test_dataset'
TEST_TABLE_NAME = 'test_table'


class LabelsTestCase(TestCase):

    def test_should_return_interactive_component_when_ds_is_provided(self):
        # given
        dataset = FakeDataset(TEST_PROJECT_ID, TEST_DATASET_NAME)

        # when
        add_label = add_label_component(TEST_TABLE_NAME, TEST_LABELS, ds=dataset)

        # then
        self.assertIsInstance(add_label, InteractiveComponent)

        # and
        self.test_should_execute_update_table_with_proper_parameters(add_label._standard_component)

    def test_should_return_raw_component_when_ds_is_not_provided(self):
        # given
        add_label = add_label_component(TEST_TABLE_NAME, TEST_LABELS)

        # expect
        self.test_should_execute_update_table_with_proper_parameters(add_label)

    def test_should_execute_update_table_with_proper_parameters(self, add_label=None):
        # given
        add_label = add_label or add_label_component(TEST_TABLE_NAME, TEST_LABELS)
        dataset = FakeDataset(TEST_PROJECT_ID, TEST_DATASET_NAME)

        # expect
        self.assertEqual(add_label(dataset), (f'{TEST_PROJECT_ID}.{TEST_DATASET_NAME}.{TEST_TABLE_NAME}', TEST_LABELS))
