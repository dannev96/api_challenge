from flask import Flask, jsonify, request, render_template, send_file,Response
from werkzeug.utils import secure_filename
from flask_mysqldb import MySQL
from flask_cors import CORS, cross_origin
from wtforms import FileField, SubmitField
from config import config
from validaciones import *
from avroact import *
from sqlcomplex import *
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
from models.ModelUser import ModelUser
from models.ModelJobs import ModelJobs
from models.ModelDepartments import ModelDepartments
from models.ModelHiredEmployees import ModelHiredEmployees
from models.entities.User import User
from models.entities.Jobs import Jobs
from models.entities.Departments import Departments
from models.entities.HiredEmployees import HiredEmployees
from flask_login import LoginManager, login_user, logout_user, login_required
from flask import Flask, render_template, request, redirect, url_for, flash

logging.getLogger('werkzeug').disabled = True

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
login_manager_app = LoginManager(app)

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
            datos = ModelHiredEmployees.get_ids(conexion)
        elif codigo == 'departments':
            datos = ModelDepartments.get_ids(conexion)
        elif codigo == 'jobs':
            datos = ModelJobs.get_ids(conexion)
        else:
            return None
        if datos != None:
            return datos
        else:
            return None
    except Exception as ex:
        raise ex

# @cross_origin
@app.route('/api/<codigo>', methods=['GET'])
@login_required
def listar_datas(codigo):
    try:
        cursor = conexion.connection.cursor()
        if codigo == 'hired_employees':
            datos = ModelHiredEmployees.get_list(conexion)
        elif codigo == 'departments':
            datos = ModelDepartments.get_list(conexion)
        elif codigo == 'jobs':
            datos = ModelJobs.get_list(conexion)
        else:
            datos = None

        if datos != None:
            return jsonify({'datas': datos, 'mensaje': "Success.", 'exito': True})
        else:
            return jsonify({'mensaje': "Empty.", 'exito': True})
    except Exception as ex:
        return jsonify({'mensaje': "Error", 'exito': False,'detalle':str(ex)})


def leer_bd(codigo,temp):
    try:
        if temp == 'hired_employees':
            datos = ModelHiredEmployees.get_by_id(conexion,codigo)
        elif temp == 'departments':
            datos = ModelDepartments.get_by_id(conexion,codigo)
        elif temp == 'jobs':
            datos = ModelJobs.get_by_id(conexion,codigo)
        else:
            return None
        if datos != None:
            curso = datos.__dict__
            return curso
        else:
            return None
    except Exception as ex:
        raise ex


@app.route('/api/<codigo2>/<codigo>', methods=['GET'])
@login_required
def leer_datas(codigo2,codigo):
    try:
        data = leer_bd(codigo,codigo2)
        if data != None:
            return jsonify({'datos': data, 'mensaje': "Success", 'exito': True})
        else:
            return jsonify({'mensaje': "No encontrado.", 'exito': False})
    except Exception as ex:
        return jsonify({'mensaje': "Error", 'exito': False,'Detalle':str(ex)})


#725.66
@app.route('/api/<codigo>', methods=['POST'])
@login_required
def registrar_datas(codigo):
    curso = leer_bd(request.json['id'],codigo)
    if curso != None:
        return jsonify({'mensaje': "Id ya existe, no se puede duplicar.", 'exito': False})
    else:
        if codigo == 'hired_employees':
            if (validar_id(request.json['id']) and validar_string(request.json['name']) and validar_string(request.json['datetime']) and validar_id(request.json['department_id']) and validar_id(request.json['job_id'])):
                lista_job = listar_ids('jobs')
                lista_deparment =listar_ids('departments')
                if request.json['job_id'] in lista_job and request.json['department_id'] in lista_deparment and validar_datetime(request.json['datetime']):
                    datos = ModelHiredEmployees.insert(conexion,request.json['id'],request.json['name'].replace('"',"'"), 
                    request.json['datetime'].replace('T',' ').replace('Z',''),request.json['department_id'], request.json['job_id'])
                    return jsonify(datos)
                else:
                    return jsonify({'mensaje': "Error", 'exito': False,"ids":"not created"})
            else:
                return jsonify({'mensaje': "Parámetros inválidos...", 'exito': False})
        elif codigo == 'departments':
            if (validar_id(request.json['id']) and validar_string(request.json['department'])):
                datos = ModelDepartments.insert(conexion,request.json['id'],request.json['department'].replace('"',"'"))
                return jsonify(datos)
            else:
                return jsonify({'mensaje': "Parámetros inválidos...", 'exito': False})
        elif codigo == 'jobs':
            if (validar_id(request.json['id']) and validar_string(request.json['job'])):
                datos = ModelJobs.insert(conexion,request.json['id'],request.json['job'].replace('"',"'"))
                return jsonify(datos)
            else:
                return jsonify({'mensaje': "Parámetros inválidos...", 'exito': False})
        else:
            return jsonify({'mensaje': "Error", 'exito': False})


@app.route('/api/<codigo>', methods=['PUT'])
@login_required
def actualizar_datas(codigo):
    if codigo == 'hired_employees':
        if (validar_id(request.json['id']) and validar_string(request.json['name']) and validar_string(request.json['datetime']) and validar_id(request.json['department_id']) and validar_id(request.json['job_id'])):
            lista_job = listar_ids('jobs')
            lista_deparment =listar_ids('departments')
            if request.json['job_id'] in lista_job and request.json['department_id'] in lista_deparment and validar_datetime(request.json['datetime']):
                datos = ModelHiredEmployees.update(conexion,request.json['id'],request.json['name'].replace('"',"'"), 
                request.json['datetime'].replace('T',' ').replace('Z',''),request.json['department_id'], request.json['job_id'])
                return jsonify(datos)
            else:
                return jsonify({'mensaje': "Error", 'exito': False,"ids":"not created"})
        else:
            return jsonify({'mensaje': "Parámetros inválidos...", 'exito': False})
    elif codigo == 'departments':
        if (validar_id(request.json['id']) and validar_string(request.json['department'])):
            datos = ModelDepartments.update(conexion,request.json['id'],request.json['department'].replace('"',"'"))
            return jsonify(datos)
        else:
            return jsonify({'mensaje': "Parámetros inválidos...", 'exito': False})
    elif codigo == 'jobs':
        if (validar_id(request.json['id']) and validar_string(request.json['job'])):
            datos = ModelJobs.update(conexion,request.json['id'],request.json['job'].replace('"',"'"))
            return jsonify(datos)
        else:
            return jsonify({'mensaje': "Parámetros inválidos...", 'exito': False})
    else:
        return jsonify({'mensaje': "Error", 'exito': False})

@app.route('/api/<codigo2>/<codigo>', methods=['DELETE'])
@login_required
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
@login_required
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
                            if codigo == 'jobs':
                                ModelJobs.update(conexion,data_id,data_job)
                            else:
                                ModelDepartments.update(conexion,data_id,data_job)
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
                                ModelHiredEmployees.update(conexion,data_id,data_name,data_datetime,data_department_id,data_job_id)
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

@app.route('/api/create_avro', methods=['POST'])
@login_required
def create_avro():
    try:
        name_table = request.json['name_table']
        name_avro = name_table +'_'+ request.json['name_avro']+'.avro'
        flgschema,schema = createschema(name_avro,name_table)
        if flgschema:
            CreateAvroFile(name_avro,schema,name_table,listar_complete(name_table))
            return jsonify({'mensaje': "OK",'name_avro_created':name_avro}) 
        else:
            return jsonify({'mensaje': "name table error"}) 
    except Exception as ex:
            return jsonify({'mensaje': "Error",'Detalle':str(ex)}) 


@app.route('/api/read_avro', methods=['POST'])
@login_required
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
@login_required
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
@login_required
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
            mensaje = ModelHiredEmployees.insert_avro(conexion,reader)
            return jsonify(mensaje)
        elif name_avro[:11] == 'departments':
            mensaje = ModelDepartments.insert_avro(conexion,reader)
            return jsonify(mensaje)
        elif name_avro[:4] == 'jobs':
            mensaje = ModelJobs.insert_avro(conexion,reader)
            return jsonify(mensaje)
        else:
            return jsonify({'mensaje': "Incorrect table"}) 
    else:
        return jsonify({'mensaje': "Avro file not find"}) 

@app.route('/api/stkhldr/hired', methods=['GET'])
@login_required
def list_hired():
    dato1 = request.args.get('group')
    dato1 = validar_dato1(dato1)

    dato2 = request.args.get('year')
    dato2 = validar_dato2(dato2)
    year_minor =  str(int(dato2))+'-01-01'
    year_mayor = str(int(dato2)+1)+'-01-01'

    dato3 = request.args.get('order')
    dato3 = validar_dato3(dato3)
    url_base = request.url.replace('/api/stkhldr/','/api/download/stkhldr/')
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
    return render_template('simple.html',  tables=[df.to_html(classes='data', header="true",index=False)], year=year_minor.split('-')[0],url_base=url_base)

@app.route('/api/download/stkhldr/hired', methods=['GET'])
@login_required
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
    return Response(df.to_csv(index=False),mimetype="text/csv",headers={"Content-Disposition":"attachment;filename=report_hired_"+year_minor.split('-')[0]+'.csv'})

@app.route('/api/stkhldr/department_hired', methods=['GET'])
@login_required
def list_department_hired():
    dato2 = request.args.get('year')
    dato2 = validar_dato2(dato2)
    dato4 = request.args.get('mean')
    dato4 = validar_dato4(dato4)
    url_base = request.url.replace('/api/stkhldr/','/api/download/stkhldr/')
    year_minor =  str(int(dato2))+'-01-01'
    year_mayor = str(int(dato2)+1)+'-01-01'
    sql = consulta_sql2(year_minor,year_mayor,dato4)
    cursor = conexion.connection.cursor()
    cursor.execute(sql)
    datos = cursor.fetchall()
    df = pd.DataFrame(datos, columns =['id', 'department', 'hired'])
    df = df.astype({'id':'int','hired':'int'})
    return render_template('simple2.html',  tables=[df.to_html(classes='data', header="true",index=False)], year=year_minor.split('-')[0],url_base=url_base)

@app.route('/api/download/stkhldr/department_hired', methods=['GET'])
@login_required
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
    return Response(df.to_csv(index=False),mimetype="text/csv",headers={"Content-Disposition":"attachment;filename=department_hired_mean"+year_minor.split('-')[0]+'.csv'})

def pagina_no_encontrada(error):
    return "<h1>Página no encontrada</h1>", 404

@login_manager_app.user_loader
def load_user(id):
    return ModelUser.get_by_id(conexion, id)

@app.route('/')
def index():
    return redirect(url_for('login'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    try:
        try:
            username = request.json['username']
            password = request.json['password']
            flaghtml = False
        except:
            flaghtml = True
        if flaghtml:
            if request.method == 'POST':
                user = User(0, request.form['username'], request.form['password'])
                logged_user = ModelUser.login(conexion, user)
                if logged_user != None:
                    if logged_user.password:
                        login_user(logged_user)
                        return redirect(url_for('home'))
                    else:
                        flash("Invalid password...")
                        return render_template('auth/login.html')
                else:
                    flash("User not found...")
                    return render_template('auth/login.html')
            else:
                return render_template('auth/login.html')
        else:
            user = User(0, username, password)
            logged_user = ModelUser.login(conexion, user)
            if logged_user != None:
                if logged_user.password:
                    login_user(logged_user)
                    return jsonify({'mensaje': "ok",'exito':True}) 
                else:
                    return jsonify({'mensaje': "Invalid password",'exito':False}) 
            else:
                return jsonify({'mensaje': "User not found",'exito':False}) 
    except Exception as ex :
        return jsonify({'mensaje': "Error",'exito':False,"Detalle":str(ex)}) 





@app.route('/logout')
def logout():
    logout_user()
    return redirect(url_for('login'))


@app.route('/home')
def home():
    return render_template('home.html')


@app.route('/protected')
@login_required
def protected():
    return "<h1>Esta es una vista protegida, solo para usuarios autenticados.</h1>"


def status_401(error):
    return redirect(url_for('login'))


def status_404(error):
    return "<h1>Página no encontrada</h1>", 404


if __name__ == '__main__':
    app.config.from_object(config['development'])
    app.register_error_handler(401, status_401)
    app.register_error_handler(404, status_404)
    #app.register_error_handler(404, pagina_no_encontrada)
    app.run(host='0.0.0.0', port=5000)