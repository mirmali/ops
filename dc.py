import _read, _write
from opsrest.constants import *

def read(schema, idl):
    '''
    Return running configuration
    '''

    config = {}
    for table_name in schema.ovs_tables.keys():

    # Check only root table or top level table
        if schema.ovs_tables[table_name].parent is not None:
            continue

    # Get table data for root or top level table
        table_data = _read.get_table_data(table_name, schema, idl)

        if table_data is not None:
            config.update(table_data)

    # remove system uuid
    config[OVSDB_SCHEMA_SYSTEM_TABLE] = config[OVSDB_SCHEMA_SYSTEM_TABLE].values()[0]
    return config

def write(data, schema, idl, txn=None):

    if txn is None:
        from ovs.db.idl import Transaction
        txn = ovs.db.idl.Transaction(idl)

    # dc.read returns config db with 'System' table
    # indexed to 'System' keyword. Replace it with
    # current database's System row UUID so that all
    # tables in 'data' are represented the same way

    system_uuid = str(idl.tables[OVSDB_SCHEMA_SYSTEM_TABLE].rows.keys()[0])
    data[OVSDB_SCHEMA_SYSTEM_TABLE] = {system_uuid:data[OVSDB_SCHEMA_SYSTEM_TABLE]}

    # iterate over all top-level tables i.e. root
    for table_name, tableschema in schema.ovs_tables.iteritems():

        # iterate over non-children tables
        if schema.ovs_tables[table_name].parent is not None:
            continue

        # set up the non-child table
        _write.setup_table(table_name, data, schema, idl, txn)

    # iterate over all tables to fill in references
    for table_name, tableschema in schema.ovs_tables.iteritems():

        if schema.ovs_tables[table_name].parent is not None:
            continue

        _write.setup_references(table_name, data, schema, idl)

    result = txn.commit_block()
    return result
