import importlib
import inspect
import runpy

from biggerquery import create_dataflow_manager

dm = create_dataflow_manager('PROJECT_ID',
                             '2019-01-01',
                             'transactions',
                             'DATAFLOW_BUCKET',
                             'REQUIREMENTS_FILE_PATH',
                             internal_tables=['user_transaction_metrics', 'transactions'])
c = importlib.import_module('_pipeline')
runpy.run_path(
    inspect.getmodule(c).__file__,
    init_globals={
        'dm': dm
    },
    run_name='__main__')