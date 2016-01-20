#  Copyright (C) 2015 Hewlett Packard Enterprise Development LP
#
#   Licensed under the Apache License, Version 2.0 (the "License"); you may
#   not use this file except in compliance with the License. You may obtain
#   a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#   License for the specific language governing permissions and limitations
#   under the License.

import _read, _write
from opsrest.constants import *

def read(schema, idl):
    """Read the OpenSwitch OVSDB database

    Args:
        schema (opslib.RestSchema): This is the
            parsed extended-schema (vswitch.extschema) object.
        idl (ovs.db.idl.Idl): This is the IDL object that
            represents the OVSDB IDL.

    Returns:
        dict: Returns a Python dictionary object containing
            data read from all OVSDB tables and arranged according
            to the relationship between various table as
            described in vswitch.extschema
    """

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
    """Write a new configuration to OpenSwitch OVSDB database

    Args:
        data (dict): The new configuration represented as a Python
            dictionary object.
        schema (opslib.RestSchema): This is the
            parsed extended-schema (vswitch.extschema) object.
        idl (ovs.db.idl.Idl): This is the IDL object that
            represents the OVSDB IDL.
        txn (ovs.db.idl.Transaction): OVSDB transaction object. If
            txn is 'None' an ovs.db.idl.Transaction object is instantiated
            during write operation.

    Returns:
        result (string): The result of the transaction. See ovs.db.idl.Transaction
        class documentation for possible values

   """

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
