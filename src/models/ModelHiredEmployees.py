from .entities.HiredEmployees import HiredEmployees
from flask_jsonpify import jsonpify


class ModelHiredEmployees():

    @classmethod
    def get_by_id(self, conexion, id):
        try:
            cursor = conexion.connection.cursor()
            sql = "SELECT id,name,datetime,department_id,job_id FROM hired_employees WHERE id = {}".format(id)
            cursor.execute(sql)
            row = cursor.fetchone()
            if row != None:
                return HiredEmployees(row[0], row[1], row[2], row[3], row[4])
            else:
                return None
        except Exception as ex:
            return str(ex)

    @classmethod
    def get_list(self,conexion):
        try:
            cursor = conexion.connection.cursor()
            sql = "SELECT id,name,datetime,department_id,job_id FROM hired_employees "
            cursor.execute(sql)
            row = cursor.fetchall()
            if row != None:
                lista = []
                if row != None:
                    for dat in row:
                        lista.append({'id': dat[0], 'name': dat[1], 'datetime': dat[2], 'department_id': dat[3], 'job_id': dat[4]})
                return lista
            else:
                return None
        except Exception as ex:
            return str(ex)

    @classmethod
    def get_ids(self,conexion):
        try:
            cursor = conexion.connection.cursor()
            sql = "SELECT id FROM hired_employees "
            cursor.execute(sql)
            row = cursor.fetchall()
            if row != None:
                lista = []
                if row != None:
                    for dat in row:
                        lista.append(dat[0])
                return lista
            else:
                return None
        except Exception as ex:
            return str(ex)

    @classmethod
    def insert(self,conexion,id,name,datetime,department_id,job_id):
        try:
            cursor = conexion.connection.cursor()
            sql = """"INSERT INTO hired_employees (id,name,datetime,department_id,job_id) 
                VALUES ({0}, '{1}', '{2}', {3}, {4})""".format(id,name,datetime,department_id,job_id)
            cursor = conexion.connection.cursor()
            cursor.execute(sql)
            conexion.connection.commit() 
            return {'mensaje': "Registrado.", 'exito': True}
        except Exception as ex:
            return {'mensaje': "Error", 'exito': False}

    @classmethod
    def update(self,conexion,id,name,datetime,department_id,job_id):
        try:
            cursor = conexion.connection.cursor()
            sql = """"UPDATE hired_employees SET name = '{0}', datetime = '{1}' , department_id= {2}, job_id= {3} 
            WHERE id = {4}""".format(name.replace('"',"'"), datetime.replace('T',' ').replace('Z',''),department_id,job_id,id)
            cursor = conexion.connection.cursor()
            cursor.execute(sql)
            conexion.connection.commit() 
            return {'mensaje': "Registrado.", 'exito': True}
        except Exception as ex:
            return {'mensaje': "Error", 'exito': False}

    @classmethod
    def insert_avro(self,conexion,reader):
        try:
            numero_insert = 0
            sql2 = """DELETE FROM  hired_employees"""
            sql = """INSERT INTO hired_employees (id,name,datetime,department_id,job_id) VALUES """
            completar = ' '
            for user in reader:
                completar += """({0},"{1}",'{2}',{3},{4}), """.format(user['id'],user['name'],user['datetime'],user['department_id'],user['job_id'])
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
                return {'mensaje': "Success"}
            else:
                return {'mensaje': "Avro empty process abort"}
        except Exception as ex:
            return {'mensaje': "Error", 'exito': False}

