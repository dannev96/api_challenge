import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import json

#Function to write Avro file iterated over SQL input in getsql 
def CreateAvroFile(filename,schema,table_name,listar_datoscomplete):
    #listar_datoscomplete = listar_complete(table_name)
    #opens file object using wb(write binary)
    file = open('.\\files\\'+filename, 'wb')
    datum_writer = DatumWriter()
    row_writer = DataFileWriter(file, datum_writer, schema)
    if table_name == 'hired_employees':
        for datos in listar_datoscomplete:
            row_writer.append({
                'id' : int(datos[0]), 
                'name' : datos[1], 
                'datetime' : str(datos[2]), 
                'department_id' : int(datos[3]), 
                'job_id' : int(datos[4])
                })
    elif table_name == 'departments':
        for datos in listar_datoscomplete:
            row_writer.append({
                'id' : int(datos[0]), 
                'department' : datos[1]
                })
    elif table_name == 'jobs':
        for datos in listar_datoscomplete:
            row_writer.append({
                'id' : int(datos[0]), 
                'job' : datos[1]
                })
    row_writer.close()

def createschema(name_avro,name_table):
    if name_table == 'hired_employees':
        schema = avro.schema.Parse(json.dumps(
        {   
            'namespace': name_avro,
            'type': 'record',
            'name': name_table,
            'fields': [
            {'name': 'id','type': 'int'},
            {'name': 'name', 'type': 'string'},
            {'name': 'datetime', 'type': 'string'},
            {'name': 'department_id', 'type': 'int'},
            {'name': 'job_id', 'type': 'int'}
                    ]
        }))
        return True,schema
    elif name_table == 'departments':
        schema = avro.schema.Parse(json.dumps(
        {   
            'namespace': name_avro,
            'type': 'record',
            'name': name_table,
            'fields': [
            {'name': 'id','type': 'int'},
            {'name': 'department', 'type': 'string'}
                    ]
        }))
        return True,schema
    elif name_table == 'jobs':
        schema = avro.schema.Parse(json.dumps(
        {   
            'namespace': name_avro,
            'type': 'record',
            'name': name_table,
            'fields': [
            {'name': 'id','type': 'int'},
            {'name': 'job', 'type': 'string'}
                    ]
        }))
        return True,schema
    else:
        schema = None
        return False,schema