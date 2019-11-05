from biggerquery.interactive import interactive_component


def add_label_component(table_name, labels, ds=None):

    def add_label(ds):
        table = ds.client.get_table("{}.{}.{}".format(ds.project_id, ds.dataset_name, table_name))
        table.labels = labels
        return ds.client.update_table(table, ["labels"])

    if ds is None:
        return add_label
    else:
        return interactive_component(ds=ds)(add_label)
