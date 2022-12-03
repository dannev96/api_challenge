from flask import Flask, jsonify, request, render_template, send_file,Response
from werkzeug.utils import secure_filename
from flask_mysqldb import MySQL
from flask_cors import CORS, cross_origin
from wtforms import FileField, SubmitField
from config import config
from validaciones import *
import datetime
import os
from io import StringIO
import json
import pandas as pd
import logging
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from flask_jsonpify import jsonpify

logging.getLogger('werkzeug').disabled = True
#logging.getLogger('werkzeug').disabled = True

'''old_getLogger = logging.getLogger
def getLogger(*args, **kwargs):
    print('Getting logger', args, kwargs)
    return old_getLogger(*args, **kwargs)
logging.getLogger = getLogger'''

temporaldia= str(datetime.date.today()).replace(' ','_')
logging.basicConfig(
    level=logging.DEBUG,
    format="{asctime} {levelname:<8} {message}",
    style='{',
    filename='.\logs\challenge'+temporaldia+'.log',
    filemode='a'
)


logging.info('=====API STARTED=====')

app = Flask(__name__)

# CORS(app)
CORS(app, resources={r"/api/*": {"origins": "http://localhost"}})

conexion = MySQL(app)

def listar_complete(codigo):
    try:
        cursor = conexion.connection.cursor()
        if codigo == 'hired_employees':
            sql = "SELECT id,name,datetime,department_id,job_id FROM {0} ".format(codigo)
        elif codigo == 'departments':
            sql = "SELECT id,department FROM {0} ".format(codigo)
        elif codigo == 'jobs':
            sql = "SELECT id,job FROM {0} ".format(codigo)
        else:
            return None
        cursor.execute(sql)
        datos = cursor.fetchall()
        curso = []
        if datos != None:
            for dat in datos:
                curso.append(dat)
            return curso
        else:
            return None
    except Exception as ex:
        raise ex

def listar_ids(codigo):
    try:
        cursor = conexion.connection.cursor()
        if codigo == 'hired_employees':
            sql = "SELECT id FROM {0} ".format(codigo)
        elif codigo == 'departments':
            sql = "SELECT id FROM {0} ".format(codigo)
        elif codigo == 'jobs':
            sql = "SELECT id FROM {0} ".format(codigo)
        else:
            return None
        cursor.execute(sql)
        datos = cursor.fetchall()
        curso = []
        if datos != None:
            for dat in datos:
                curso.append(dat[0])
            return curso
        else:
            return None
    except Exception as ex:
        raise ex

# @cross_origin
@app.route('/api/<codigo>', methods=['GET'])
def listar_datas(codigo):
    try:
        cursor = conexion.connection.cursor()
        if codigo == 'hired_employees':
            sql = "SELECT id,name,datetime,department_id,job_id FROM hired_employees ORDER BY id ASC"
        elif codigo == 'departments':
            sql = "SELECT id,department FROM departments ORDER BY id ASC"
        else:
            sql = "SELECT id,job FROM jobs ORDER BY id ASC"
        cursor.execute(sql)
        datos = cursor.fetchall()
        datas = []
        if codigo == 'hired_employees':
            for fila in datos:
                data = {'id': fila[0], 'name': fila[1], 'datetime': fila[2], 'department_id': fila[3], 'job_id': fila[4]}
                datas.append(data)
        elif codigo == 'departments':
            for fila in datos:
                data = {'id': fila[0], 'department': fila[1]}
                datas.append(data)
        else:
            for fila in datos:
                data = {'id': fila[0], 'job': fila[1]}
                datas.append(data)
        return jsonify({'datas': datas, 'mensaje': "Success.", 'exito': True})
    except Exception as ex:
        return jsonify({'mensaje': "Error", 'exito': False,'detalle':str(ex)})


def leer_bd(codigo,temp):
    try:
        cursor = conexion.connection.cursor()
        if temp == 'hired_employees':
            sql = "SELECT id,name,datetime,department_id,job_id FROM {0} WHERE id = '{1}'".format(temp,codigo)
        elif temp == 'departments':
            sql = "SELECT id,department FROM {0} WHERE id = '{1}'".format(temp,codigo)
        elif temp == 'jobs':
            sql = "SELECT id,job FROM {0} WHERE id = '{1}'".format(temp,codigo)
        else:
            return None
        cursor.execute(sql)
        datos = cursor.fetchone()
        if datos != None:
            if temp == 'hired_employees':
                curso = {'id': datos[0], 'name': datos[1], 'datetime': datos[2], 'department_id': datos[3], 'job_id': datos[4]}
            elif temp == 'departments':
                curso = {'id': datos[0], 'department': datos[1]}
            elif temp == 'jobs':
                curso = {'id': datos[0], 'job': datos[1]}
            else:
                curso = None
            return curso
        else:
            return None
    except Exception as ex:
        raise ex


@app.route('/api/<codigo2>/<codigo>', methods=['GET'])
def leer_datas(codigo2,codigo):
    try:
        data = leer_bd(codigo,codigo2)
        if data != None:
            return jsonify({'datos': data, 'mensaje': "Success", 'exito': True})
        else:
            return jsonify({'mensaje': "No encontrado.", 'exito': False})
    except Exception as ex:
        return jsonify({'mensaje': "Error", 'exito': False})



@app.route('/api/<codigo>', methods=['POST'])
def registrar_datas(codigo):
    if codigo == 'hired_employees':
        if (validar_id(request.json['id']) and validar_string(request.json['name']) and validar_string(request.json['datetime']) and validar_id(request.json['department_id']) and validar_id(request.json['job_id'])):
            lista_job = listar_datas('jobs')
            lista_deparment =listar_datas('departments')
            if request.json['job_id'] in lista_job and request.json['department_id'] in lista_deparment and validar_datetime(request.json['datetime']):
                flagvalida = True
                sql = """INSERT INTO hired_employees (id,name,datetime,department_id,job_id) 
                VALUES ({0}, '{1}', '{2}', {3}, {4})""".format(request.json['id'],
                                                        request.json['name'].replace('"',"'"), request.json['datetime'].replace('T',' ').replace('Z',''),request.json['department_id'], request.json['job_id'])
            else:
                flagvalida = False
        else:
            flagvalida = False
    elif codigo == 'departments':
        if (validar_id(request.json['id']) and validar_string(request.json['department'])):
            flagvalida = True
            sql = """INSERT INTO departments (id,department) 
            VALUES ({0}, '{1}')""".format(request.json['id'],
                                                    request.json['department'].replace('"',"'"))
        else:
            flagvalida = False
    else:
        if (validar_id(request.json['id']) and validar_string(request.json['job'])):
            flagvalida = True
            sql = """INSERT INTO jobs (id,job) 
            VALUES ({0}, '{1}')""".format(request.json['id'],
                                                    request.json['job'].replace('"',"'"))
        else:
            flagvalida = False

    if (flagvalida):
        try:
            curso = leer_bd(request.json['id'],codigo)
            if curso != None:
                return jsonify({'mensaje': "Id ya existe, no se puede duplicar.", 'exito': False})
            else:
                cursor = conexion.connection.cursor()
                cursor.execute(sql)
                conexion.connection.commit()  # Confirma la acción de inserción.
                return jsonify({'mensaje': "Registrado.", 'exito': True})
        except Exception as ex:
            return jsonify({'mensaje': "Error", 'exito': False,'Detalle':str(ex)})
    else:
        return jsonify({'mensaje': "Parámetros inválidos...", 'exito': False})


@app.route('/api/<codigo>', methods=['PUT'])
def actualizar_datas(codigo):
    if codigo == 'hired_employees':
        if (validar_id(request.json['id']) and validar_string(request.json['name']) and validar_string(request.json['datetime']) and validar_id(request.json['department_id']) and validar_id(request.json['job_id'])):
            lista_job = listar_datas('jobs')
            lista_deparment =listar_datas('departments')
            if request.json['job_id'] in lista_job and request.json['department_id'] in lista_deparment and validar_datetime(request.json['datetime']):
                flagvalida = True
                sql = """UPDATE hired_employees SET name = '{0}', datetime = '{1}' , department_id= {2}, job_id= {3}
                WHERE id = {4}""".format(request.json['name'].replace('"',"'"), request.json['datetime'].replace('T',' ').replace('Z',''),request.json['department_id'], request.json['job_id'], request.json['id'])
            else:
                flagvalida = False
        else:
            flagvalida = False
    elif codigo == 'departments':
        if (validar_id(request.json['id']) and validar_string(request.json['department'])):
            flagvalida = True
            sql = """UPDATE departments SET department = '{0}'
            WHERE id = {1}""".format(request.json['department'].replace('"',"'"), request.json['id'])
        else:
            flagvalida = False
    elif codigo == 'jobs':
        if (validar_id(request.json['id']) and validar_string(request.json['job'])):
            flagvalida = True
            sql = """UPDATE jobs SET job = '{0}'
            WHERE id = {1}""".format(request.json['job'].replace('"',"'"), request.json['id'])
        else:
            flagvalida = False
    else:
        flagvalida = False
    if (flagvalida):
        try:
            curso = leer_bd(request.json['id'],codigo)
            if curso != None:
                cursor = conexion.connection.cursor()
                cursor.execute(sql)
                conexion.connection.commit()  # Confirma la acción de actualización.
                return jsonify({'mensaje': "Actualizado.", 'exito': True})
            else:
                return jsonify({'mensaje': "No encontrado.", 'exito': False})
        except Exception as ex:
            return jsonify({'mensaje': "Error", 'exito': False,'Detalle':str(ex)})
    else:
        return jsonify({'mensaje': "Parámetros inválidos...", 'exito': False})

@app.route('/api/<codigo2>/<codigo>', methods=['DELETE'])
def eliminar_datas(codigo,codigo2):
    try:
        curso = leer_bd(codigo,codigo2)
        if curso != None:
            cursor = conexion.connection.cursor()
            sql = "DELETE FROM {0} WHERE id = '{1}'".format(codigo2,codigo)
            cursor.execute(sql)
            conexion.connection.commit()  # Confirma la acción de eliminación.
            return jsonify({'mensaje': "Eliminado.", 'exito': True})
        else:
            return jsonify({'mensaje': "No encontrado.", 'exito': False})
    except Exception as ex:
        return jsonify({'mensaje': "Error", 'exito': False})

ALLOWED_EXTENSIONS = set(['csv'])

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.',1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/api/csv', methods=['POST'])
def update_csv_datas():
    try:
        numero_insert=0
        numero_update=0
        file = request.files['file']
        temp = file.read()
        filename = secure_filename(file.filename)
        if file and allowed_file(filename) and (filename == 'hired_employees.csv' or filename == 'departments.csv' or filename == 'jobs.csv' ):
            codigo = filename[:-4]
            result=str(temp,'utf-8')
            data = StringIO(result) 
            if codigo == 'hired_employees':
                columns=['id','name','datetime','department_id','job_id']
                sql = """INSERT INTO hired_employees (id,name,datetime,department_id,job_id) VALUES """
            elif codigo == 'departments':
                columns=['id','department']
                sql = """INSERT INTO departments (id,department) VALUES """
            elif codigo == 'jobs':
                columns=['id','job']
                sql = """INSERT INTO jobs (id,job) VALUES """
            df=pd.read_csv(data,header=None,names=columns)
            df=df.fillna('')
            numfila,numcolumna = df.shape
            completar = ''
            if (codigo == 'jobs' or codigo == 'departments') and numcolumna == 2 :
                lista_ids = listar_ids(codigo)
                for (row,rs) in df.iterrows():
                    if validar_id(rs[0]):
                        data_id =str(int(rs[0]))
                        data_job = rs[1]
                        if rs[0] not in lista_ids:
                            completar += """({0},"{1}""), """.format(data_id.replace('"',"'"),data_job)
                            numero_insert += 1
                            if (numero_insert+numero_update) >= 1000:
                                break
                        else:
                            sql2 = """UPDATE {0} SET {1} = "{2}"
                            WHERE id = {3}""".format(codigo,codigo[:-1],data_job.replace('"',"'"), data_id)
                            cursor = conexion.connection.cursor()
                            cursor.execute(sql2)
                            conexion.connection.commit()
                            numero_update += 1
                            if (numero_insert+numero_update) >= 1000:
                                break
                    else:
                        logging.warning('Data not inserted or updated:\n'+str(rs))

            elif codigo == 'hired_employees' and numcolumna == 5:
                lista_ids = listar_ids(codigo)
                lista_department = listar_ids('departments')
                lista_job = listar_ids('jobs')
                for (row,rs) in df.iterrows():
                    if rs[0] != '' and validar_id(rs[0]):
                        if rs[3] in lista_department and rs[4] in lista_job and validar_datetime(rs[2]):
                            data_id =str(int(rs[0]))
                            data_name = rs[1]
                            data_datetime = rs[2].replace('T',' ').replace('Z','')
                            data_department_id = str(int(rs[3]))
                            data_job_id = str(int(rs[4]))
                            if rs[0] not in lista_ids:
                                completar += """({0},"{1}",'{2}',{3},{4}), """.format(data_id,data_name.replace('"',"'"),data_datetime,data_department_id,data_job_id)
                                numero_insert += 1
                                if (numero_insert+numero_update) >= 1000:
                                    break
                            else:
                                sql2 = """UPDATE hired_employees SET name = "{0}", datetime = '{1}' , department_id= {2}, job_id= {3}
                                WHERE id = {4}""".format(data_name.replace('"',"'"), data_datetime,data_department_id,data_job_id, data_id)
                                cursor = conexion.connection.cursor()
                                cursor.execute(sql2)
                                conexion.connection.commit()
                                numero_update += 1
                                if (numero_insert+numero_update) >= 1000:
                                    break
                        else:
                            logging.warning('Data not inserted or updated:\n'+str(rs))
                    else:
                        logging.warning('Data not inserted or updated:\n'+str(rs))
            else:
                return jsonify({'mensaje': "Error columna"}) 
            final_sql = sql + completar
            final_sql = final_sql[:-2]
            if numero_insert > 0:
                cursor = conexion.connection.cursor()
                cursor.execute(final_sql)
                conexion.connection.commit()
            if (numero_insert+numero_update) >= 1000:
                return jsonify({'mensaje': "first 1000",'insert':numero_insert,'update':numero_update,'total':numfila}) 
            else:
                return jsonify({'mensaje': "OK",'insert':numero_insert,'update':numero_update,'total':numfila}) 
        else:
            return jsonify({'mensaje': "Incorrect name file or extensions"}) 
    except Exception as ex:
        return jsonify({'mensaje': "Error", 'exito': False,'Detalle':str(ex)}) 

##AVRO

#Function to write Avro file iterated over SQL input in getsql 
def CreateAvroFile(filename,schema,table_name):
    listar_datoscomplete = listar_complete(table_name)
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

@app.route('/api/create_avro', methods=['POST'])
def create_avro():
    try:
        name_table = request.json['name_table']
        name_avro = name_table +'_'+ request.json['name_avro']+'.avro'
        flgschema,schema = createschema(name_avro,name_table)
        if flgschema:
            CreateAvroFile(name_avro,schema,name_table)
            return jsonify({'mensaje': "OK",'name_avro_created':name_avro}) 
        else:
            return jsonify({'mensaje': "name table error"}) 
    except Exception as ex:
            return jsonify({'mensaje': "Error",'Detalle':str(ex)}) 


@app.route('/api/read_avro', methods=['POST'])
def read_avro():
    name_avro = request.json['name_avro']
    result = []
    for root, dir, files in os.walk('./files/'):
        if name_avro in files:
            result.append(os.path.join(root, name_avro))
    if len(result) > 0 :
        reader = DataFileReader(open(name_avro, "rb"), DatumReader())
        lista = []
        for user in reader:
            lista.append(user)
        reader.close()
        return jsonify({'mensaje': "OK",'lista':lista}) 
    else:
        return jsonify({'mensaje': "Avro file not find"}) 

@app.route('/api/list_avro', methods=['GET'])
def list_avro():
    result = []
    for root, dir, files in os.walk('./files/'):
        for file in files :
            if file[-4:] == 'avro' and file != 'avro':
                result.append(os.path.join(root, file)[8:])
    if len(result) > 0 :
        return jsonify({'mensaje': "OK",'lista':result}) 
    else:
        return jsonify({'mensaje': "Avro file not find"}) 


@app.route('/api/upload_avro', methods=['POST'])
def upload_avro():
    name_avro = request.json['name_avro']
    result = []
    numero_insert = 0
    for root, dir, files in os.walk('./files/'):
        if name_avro in files:
            result.append(os.path.join(root, name_avro))
    if len(result) > 0 :
        reader = DataFileReader(open('./files/'+name_avro, "rb"), DatumReader())
        lista = []
        completar = ''
        if name_avro[:15] == 'hired_employees':
            sql2 = """DELETE FROM  hired_employees"""
            sql = """INSERT INTO hired_employees (id,name,datetime,department_id,job_id) VALUES """
            for user in reader:
                completar += """({0},"{1}",'{2}',{3},{4}), """.format(user['id'],user['name'],user['datetime'],user['department_id'],user['job_id'])
                numero_insert += 1
            reader.close()
        elif name_avro[:11] == 'departments':
            sql2 = """DELETE FROM  departments"""
            sql = """INSERT INTO departments (id,department) VALUES """
            for user in reader:
                completar += """({0},"{1}"), """.format(user['id'],user['department'])
                numero_insert += 1
            reader.close()
        elif name_avro[:4] == 'jobs':
            sql2 = """DELETE FROM jobs"""
            sql = """INSERT INTO jobs (id,job) VALUES """
            for user in reader:
                completar += """({0},"{1}"), """.format(user['id'],user['job'])
                numero_insert += 1
            reader.close()
        final_sql = sql + completar
        final_sql = final_sql[:-2]
        if numero_insert > 0:
            cursor = conexion.connection.cursor()
            cursor.execute(sql2)
            conexion.connection.commit()

            cursor = conexion.connection.cursor()
            cursor.execute(final_sql)
            conexion.connection.commit()
            return jsonify({'mensaje': "Success"}) 
        else:
            return jsonify({'mensaje': "Avro empty process abort"}) 
    else:
        return jsonify({'mensaje': "Avro file not find"}) 


def consulta_sql(year_minor,year_mayor,cadena):
    sql = """with cte as
        (
        select
            A.job_id as job_id,
            A.department_id as department_id,
            max(case when A.Q = 1 then total end) as Q1,
            max(case when A.Q = 2 then total end) as Q2,
            max(case when A.Q = 3 then total end) as Q3,
            max(case when A.Q = 4 then total end) as Q4
        from (select job_id,department_id,quarter(datetime) as Q,count(1) as total from challenge.hired_employees  
        where datetime >= '{0}' and datetime <'{1}'
        group by job_id,department_id,Q) A
        group by
            A.job_id,
            A.department_id
        )

        select 
            C.department as department,B.job as job,A.Q1 as Q1,A.Q2 as Q2,A.Q3 as Q3,A.Q4 as Q4
        from cte A
        left join jobs B ON A.job_id = B.id
        left join departments C ON A.department_id = C.id
        order by """.format(year_minor,year_mayor)  + cadena
    return sql

def consulta_sql2(year_minor,year_mayor,dato):
    sql = """with cte as
        (
		select B.id as id ,B.department as department,count(1) as hired from challenge.hired_employees A
		left join challenge.departments B ON A.department_id = B.id
		where A.datetime >= '{0}' and A.datetime < '{1}'
		group by id,department order by hired desc
        )
        
        SELECT id, department, hired
            FROM cte """.format(year_minor,year_mayor)
    if dato == 'all':
        cadena =""" """
    elif dato == 'lower':
        cadena ="""WHERE hired < ( SELECT avg(hired) FROM cte )"""
    else:
        cadena ="""WHERE hired > ( SELECT avg(hired) FROM cte )"""
    sql += cadena
    return sql


@app.route('/api/stkhldr/hired', methods=['GET'])
def list_hired():
    dato1 = request.args.get('group')
    dato1 = validar_dato1(dato1)

    dato2 = request.args.get('year')
    dato2 = validar_dato2(dato2)
    year_minor =  str(int(dato2))+'-01-01'
    year_mayor = str(int(dato2)+1)+'-01-01'

    dato3 = request.args.get('order')
    dato3 = validar_dato3(dato3)
    cadena = ' '
    for dat in dato3 :
        cadena += dat+' ,'
    cadena = cadena[:-2]
    sql = consulta_sql(year_minor,year_mayor,cadena)

    cursor = conexion.connection.cursor()
    cursor.execute(sql)
    datos = cursor.fetchall()
    df = pd.DataFrame(datos, columns =['department', 'job', 'Q1','Q2','Q3','Q4'])
    df=df.fillna(0)
    df = df.astype({'Q1':'int','Q2':'int','Q3':'int','Q4':'int'})
    return render_template('simple.html',  tables=[df.to_html(classes='data', header="true")], year=year_minor.split('-')[0])

@app.route('/api/download/stkhldr/hired', methods=['GET'])
def download_list_hired():
    dato1 = request.args.get('group')
    dato1 = validar_dato1(dato1)

    dato2 = request.args.get('year')
    dato2 = validar_dato2(dato2)
    year_minor =  str(int(dato2))+'-01-01'
    year_mayor = str(int(dato2)+1)+'-01-01'

    dato3 = request.args.get('order')
    dato3 = validar_dato3(dato3)
    cadena = ' '
    for dat in dato3 :
        cadena += dat+' ,'
    cadena = cadena[:-2]
    sql = consulta_sql(year_minor,year_mayor,cadena)
    cursor = conexion.connection.cursor()
    cursor.execute(sql)
    datos = cursor.fetchall()
    df = pd.DataFrame(datos, columns =['department', 'job', 'Q1','Q2','Q3','Q4'])
    df=df.fillna(0)
    df = df.astype({'Q1':'int','Q2':'int','Q3':'int','Q4':'int'})
    return Response(df.to_csv(),mimetype="text/csv",headers={"Content-Disposition":"attachment;filename=report_hired_"+year_minor.split('-')[0]+'.csv'})

@app.route('/api/stkhldr/department_hired', methods=['GET'])
def list_department_hired():
    dato2 = request.args.get('year')
    dato2 = validar_dato2(dato2)
    dato4 = request.args.get('mean')
    dato4 = validar_dato4(dato4)
    year_minor =  str(int(dato2))+'-01-01'
    year_mayor = str(int(dato2)+1)+'-01-01'
    sql = consulta_sql2(year_minor,year_mayor,dato4)
    cursor = conexion.connection.cursor()
    cursor.execute(sql)
    datos = cursor.fetchall()
    df = pd.DataFrame(datos, columns =['id', 'department', 'hired'])
    df = df.astype({'id':'int','hired':'int'})
    return render_template('simple2.html',  tables=[df.to_html(classes='data', header="true")], year=year_minor.split('-')[0])

@app.route('/api/download/stkhldr/department_hired', methods=['GET'])
def download_list_department_hired():
    dato2 = request.args.get('year')
    dato2 = validar_dato2(dato2)
    year_minor =  str(int(dato2))+'-01-01'
    year_mayor = str(int(dato2)+1)+'-01-01'
    dato4 = request.args.get('mean')
    dato4 = validar_dato2(dato4)
    sql = consulta_sql2(year_minor,year_mayor,dato4)
    cursor = conexion.connection.cursor()
    cursor.execute(sql)
    datos = cursor.fetchall()
    df = pd.DataFrame(datos, columns =['id', 'department', 'hired'])
    df = df.astype({'id':'int','hired':'int'})
    return Response(df.to_csv(),mimetype="text/csv",headers={"Content-Disposition":"attachment;filename=department_hired_mean"+year_minor.split('-')[0]+'.csv'})

def pagina_no_encontrada(error):
    return "<h1>Página no encontrada</h1>", 404


if __name__ == '__main__':
    app.config.from_object(config['development'])
    app.register_error_handler(404, pagina_no_encontrada)
    app.run()