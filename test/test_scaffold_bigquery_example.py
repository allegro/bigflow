from unittest import TestCase, mock
from unittest.mock import patch

from google.cloud import bigquery
from bigflow.bigquery.dataset_manager import DatasetManager, TemplatedDatasetManager
from bigflow.scaffold.scaffold_templates import bq_workflow_template

exec(bq_workflow_template % {'project_id': 'gcp-project'})


class ScaffoldBigQueryExampleTestCase(TestCase):
    @mock.patch('bigflow.bigquery.dataset_manager.create_bigquery_client')
    @patch.object(DatasetManager, 'write')
    @patch.object(DatasetManager, 'create_table')
    @patch.object(DatasetManager, 'collect')
    @patch.object(TemplatedDatasetManager, 'create_full_table_id',
                  side_effect=lambda table: 'gcp-project.bigflow_test' + '.' + table)
    @patch.object(DatasetManager, 'table_exists_or_error')
    def test_should_use_proper_queries(self, table_exists_or_error_mock, create_full_table_id_mock, collect__mock,
                                       create_table_mock, write_mock, create_bigquery_client_mock):
        create_bigquery_client_mock.return_value = mock.create_autospec(bigquery.Client, project='my_gcp_project', credentials=None, location='EU')
        table_exists_or_error_mock.return_value = True
        internationalports_workflow.run('2019-01-01')
        collect__mock.assert_called_with('''
        INSERT INTO `gcp-project.bigflow_test.ports` (port_name, port_latitude, port_longitude, country, index_number)
        VALUES
        ('GDYNIA', 54.533333, 18.55, 'POL', '28740'),
        ('GDANSK', 54.35, 18.666667, 'POL', '28710'),
        ('SANKT-PETERBURG', 59.933333, 30.3, 'RUS', '28370'),
        ('TEXAS', 34.8, 31.3, 'USA', '28870');
        ''')

        create_table_mock.assert_any_call('''
    CREATE TABLE IF NOT EXISTS polish_ports (
      port_name STRING,
      port_latitude FLOAT64,
      port_longitude FLOAT64)
''')

        create_table_mock.assert_any_call('''
    CREATE TABLE IF NOT EXISTS ports (
      port_name STRING,
      port_latitude FLOAT64,
      port_longitude FLOAT64,
      country STRING,
      index_number STRING)
''')
        write_mock.assert_called_with('gcp-project.bigflow_test.ports', '''
        SELECT port_name, port_latitude, port_longitude
        FROM `gcp-project.bigflow_test.ports`
        WHERE country = 'POL'
        ''', 'WRITE_TRUNCATE')