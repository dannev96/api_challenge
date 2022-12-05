from .entities.Departments import Departments
from flask_jsonpify import jsonpify


class ModelDepartments():

    @classmethod
    def get_by_id(self, conexion, id):
        try:
            cursor = conexion.connection.cursor()
            sql = "SELECT id,department FROM departments WHERE id = {}".format(id)
            cursor.execute(sql)
            row = cursor.fetchone()
            if row != None:
                return Departments(row[0], row[1])
            else:
                return None
        except Exception as ex:
            return str(ex)
            #raise Exception(ex)

    @classmethod
    def get_by_id(self, conexion, id):
        try:
            cursor = conexion.connection.cursor()
            sql = "SELECT id,department FROM departments WHERE id = {}".format(id)
            cursor.execute(sql)
            row = cursor.fetchone()
            if row != None:
                return Departments(row[0], row[1])
            else:
                return None
        except Exception as ex:
            return str(ex)

    @classmethod
    def get_list(self,conexion):
        try:
            cursor = conexion.connection.cursor()
            sql = "SELECT id,department FROM departments "
            cursor.execute(sql)
            row = cursor.fetchall()
            if row != None:
                lista = []
                if row != None:
                    for dat in row:
                        lista.append({'id': dat[0], 'department': dat[1]})
                return lista
            else:
                return None
        except Exception as ex:
            return str(ex)

    @classmethod
    def get_ids(self,conexion):
        try:
            cursor = conexion.connection.cursor()
            sql = "SELECT id FROM departments "
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
    def insert(self,conexion,id,department):
        try:
            cursor = conexion.connection.cursor()
            sql = """"INSERT INTO departments (id,department) 
                VALUES ({0}, '{1}')""".format(id,department)
            cursor = conexion.connection.cursor()
            cursor.execute(sql)
            conexion.connection.commit() 
            return {'mensaje': "Registrado.", 'exito': True}
        except Exception as ex:
            return {'mensaje': "Error", 'exito': False}


    @classmethod
    def update(self,conexion,id,department):
        try:
            cursor = conexion.connection.cursor()
            sql = """"UPDATE departments SET department = '{0}' 
            WHERE id = {1}""".format(department.replace('"',"'"),id)
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
            sql2 = """DELETE FROM  departments"""
            sql = """INSERT INTO departments (id,department) VALUES """
            completar = ' '
            for user in reader:
                completar += """({0},"{1}"), """.format(user['id'],user['department'])
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
            return {'mensaje': "Error", 'exito': False,'Detalle':str(ex)}