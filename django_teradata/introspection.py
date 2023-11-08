from django.db.backends.base.introspection import (
    BaseDatabaseIntrospection, FieldInfo,
    TableInfo,
)

'''
https://support.teradata.com/community?id=community_question&sys_id=57a683ef1b57fb00682ca8233a4bcb44
A1 ARRAY 
AN MULTI-DIMENSIONAL ARRAY
AT TIME 
BF BYTE
BO BLOB 
BV VARBYTE
CF CHARACTER 
CO CLOB
CV VARCHAR 
D DECIMAL
DA DATE 
DH INTERVAL DAY TO HOUR
DM INTERVAL DAY TO MINUTE 
DS INTERVAL DAY TO SECOND
DY INTERVAL DAY 
F FLOAT
HM INTERVAL HOUR TO MINUTE 
HS INTERVAL HOUR TO SECOND
HR INTERVAL HOUR 
I INTEGER
I1 BYTEINT 
I2 SMALLINT
I8 BIGINT 
JN JSON
MI INTERVAL MINUTE 
MO INTERVAL MONTH
MS INTERVAL MINUTE TO SECOND 
N NUMBER
PD PERIOD(DATE) 
PM PERIOD(TIMESTAMP WITH TIME ZONE)
PS PERIOD(TIMESTAMP) 
PT PERIOD(TIME)
PZ PERIOD(TIME WITH TIME ZONE) 
SC INTERVAL SECOND
SZ TIMESTAMP WITH TIME ZONE 
TS TIMESTAMP
TZ TIME WITH TIME ZONE 
UT UDT Type
XM XML 
YM INTERVAL YEAR TO MONTH
YR INTERVAL YEAR 
++ TD_ANYTYPE
'''


def get_data_type(name):
    if name == 'A1':
        return 'ARRAY'
    if name == 'AN':
        return "MULTI - DIMENSIONAL ARRAY"
    if name == 'AT':
        return 'TIME'
    if name == 'BF':
        return 'BYTE'
    if name == 'BO':
        return 'BLOB'
    if name == 'BV':
        return 'VARBYTE'
    if name == 'CF':
        return 'CHARACTER'
    if name == 'CO':
        return 'CLOB'
    if name == 'CV':
        return 'VARCHAR'
    if name == 'D':
        return 'DECIMAL'
    if name == 'DA':
        return 'DATE'
    if name == 'DH':
        return "INTERVAL DAY TO HOUR"
    if name == 'DM':
        return "INTERVAL DAY TO MINUTE"
    if name == 'DS':
        return "INTERVAL DAY TO SECOND"
    if name == 'DY':
        return "INTERVAL DAY"
    if name == 'F':
        return 'FLOAT'
    if name == 'HM':
        return "INTERVAL HOUR TO MINUTE"
    if name == 'HS':
        return "INTERVAL HOUR TO SECOND"
    if name == 'HR':
        return "INTERVAL HOUR"
    if name == 'I':
        return 'INTEGER'
    if name == 'I1':
        return 'BYTEINT'
    if name == 'I2':
        return 'SMALLINT'
    if name == 'I8':
        return 'BIGINT'
    if name == 'JN':
        return 'JSON'
    if name == 'MI':
        return "INTERVAL MINUTE"
    if name == 'MO':
        return "INTERVAL MONTH"
    if name == 'MS':
        return "INTERVAL MINUTE TO SECOND"
    if name == 'N':
        return 'NUMBER'
    if name == 'PD':
        return "PERIOD(DATE)"
    if name == 'PM':
        return "PERIOD(TIMESTAMP WITH TIME ZONE)"
    if name == 'PS':
        return "PERIOD(TIMESTAMP)"
    if name == 'PT':
        return "PERIOD(TIME)"
    if name == 'PZ':
        return "PERIOD(TIME WITH TIME ZONE)"
    if name == 'SC':
        return "INTERVAL SECOND"
    if name == 'SZ':
        return "TIMESTAMP WITH TIME ZONE"
    if name == 'TS':
        return 'TIMESTAMP'
    if name == 'TZ':
        return "TIME WITH TIME ZONE"
    if name == 'UT':
        return "UDT Type"
    if name == 'XM':
        return 'XML'
    if name == 'YM':
        return "INTERVAL YEAR TO MONTH"
    if name == 'YR':
        return "INTERVAL YEAR"

    return name


class DatabaseIntrospection(BaseDatabaseIntrospection):
    # Maps Teradata data types to Django Fields.
    # https://pypi.org/project/teradatasql/#DataTypes
    data_types_reverse = {
        'BIGINT': 'IntegerField',
        'BLOB': 'BinaryField',
        'BYTE': 'BinaryField',
        'BYTEINT': 'IntegerField',
        'CHARACTER': 'TextField',
        'CLOB': 'TextField',
        'DATE': 'DateField',
        'DECIMAL': 'DecimalField',
        'FLOAT': 'FloatField',
        'INTEGER': 'IntegerField',
        'INTERVAL YEAR': 'TextField',
        'INTERVAL YEAR TO MONTH': 'TextField',
        'INTERVAL MONTH': 'TextField',
        'INTERVAL DAY': 'TextField',
        'INTERVAL DAY TO HOUR': 'TextField',
        'INTERVAL DAY TO MINUTE': 'TextField',
        'INTERVAL DAY TO SECOND'
        'INTERVAL HOUR': 'TextField',
        'INTERVAL HOUR TO MINUTE': 'TextField',
        'INTERVAL HOUR TO SECOND': 'TextField',
        'INTERVAL MINUTE': 'TextField',
        'INTERVAL MINUTE TO SECOND': 'TextField',
        'INTERVAL SECOND': 'TextField',
        'NUMBER': 'DecimalField',
        'PERIOD(DATE)': 'TextField',
        'PERIOD(TIME)': 'TextField',
        'PERIOD(TIME WITH TIME ZONE)': 'TextField',
        'PERIOD(TIMESTAMP)': 'TextField',
        'PERIOD(TIMESTAMP WITH TIME ZONE)': 'TextField',
        'SMALLINT': 'IntegerField',
        'TIME': 'TimeField',
        'TIME WITH TIME ZONE': 'TimeField',
        'TIMESTAMP': 'DateTimeField',
        'TIMESTAMP WITH TIME ZONE': 'DateTimeField',
        'VARBYTE': 'BinaryField',
        'VARCHAR': 'TextField',
        'XML': 'TextField',
        'JSON': 'TextField',
    }

    def get_constraints(self, cursor, table_name):
        # ignore constraints since it is a read-only backend adapter
        return {}

    def get_primary_key_column(self, cursor, table_name):
        # ignore primary_key since it is a read-only backend adapter
        return None

    def get_relations(self, cursor, table_name):
        # ignore relations since it is a read-only backend adapter
        return {}

    def get_table_description(self, cursor, table_name):
        database_name = self.connection.settings_dict["NAME"]
        cursor.execute(f"""
            SELECT
            ColumnName, ColumnType, DecimalTotalDigits, DecimalFractionalDigits
            FROM
            dbc.columnsV
            WHERE
            DatabaseName = '{database_name}' AND
            TableName = '{table_name}'
        """)
        table_info = cursor.fetchall()
        return [
            FieldInfo(
                name,  # name
                get_data_type(data_type.strip()),  # type_code
                None,  # display_size
                None,  # internal_size
                precision,  # precision
                scale,      # scale
                None,  # null_ok
                None,  # default
                None,  # collation
            )
            for (
                name, data_type, precision, scale
            ) in table_info
        ]

    def get_table_list(self, cursor):
        database_name = self.connection.settings_dict['NAME']
        cursor.execute(f"SELECT TableName FROM DBC.TablesV WHERE TableKind = 'T' AND DataBaseName = '{database_name}'")
        tables = [
            TableInfo(
                row[0],  # table name
                't'  # 't' for table
            ) for row in cursor.fetchall()
        ]
        cursor.execute(f"SELECT TableName FROM DBC.TablesV WHERE TableKind = 'V' AND DataBaseName = '{database_name}'")
        views = [
            TableInfo(
                row[0],   # view name
                'v'  # 'v' for view
            ) for row in cursor.fetchall()
        ]
        return tables + views

