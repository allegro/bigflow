import biggerquery as bgq


def sensor_component(table_alias, where_clause, ds=None):

    def sensor(ds):
        result = ds.collect('''
        SELECT count(*) > 0 as table_ready
        FROM `{%(table_alias)s}`
        WHERE %(where_clause)s
        ''' % {
            'table_alias': table_alias,
            'where_clause': where_clause
        })

        if not result.iloc[0]['table_ready']:
            raise ValueError('{} is not ready'.format(table_alias))

    sensor.__name__ = 'wait_for_{}'.format(table_alias)

    return sensor if ds is None else bgq.component(ds=ds)(sensor)