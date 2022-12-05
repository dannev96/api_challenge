from .entities.Jobs import Jobs
from flask_jsonpify import jsonpify


class ModelJobs():

    @classmethod
    def get_by_id(self, conexion, id):
        try:
            cursor = conexion.connection.cursor()
            sql = "SELECT id,job FROM jobs WHERE id = {}".format(id)
            cursor.execute(sql)
            row = cursor.fetchone()
            if row != None:
                return Jobs(row[0], row[1])
            else:
                return None
        except Exception as ex:
            return str(ex)

    @classmethod
    def get_list(self,conexion):
        try:
            cursor = conexion.connection.cursor()
            sql = "SELECT id,job FROM jobs "
            cursor.execute(sql)
            row = cursor.fetchall()
            if row != None:
                lista = []
                if row != None:
                    for dat in row:
                        lista.append({'id': dat[0], 'job': dat[1]})
                return lista
            else:
                return None
        except Exception as ex:
            return str(ex)

    @classmethod
    def get_ids(self,conexion):
        try:
            cursor = conexion.connection.cursor()
            sql = "SELECT id FROM jobs "
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
    def insert(self,conexion,id,job):
        try:
            cursor = conexion.connection.cursor()
            sql = """"INSERT INTO jobs (id,job) 
                VALUES ({0}, '{1}')""".format(id,job)
            cursor = conexion.connection.cursor()
            cursor.execute(sql)
            conexion.connection.commit() 
            return {'mensaje': "Registrado.", 'exito': True}
        except Exception as ex:
            return {'mensaje': "Error", 'exito': False}

    @classmethod
    def update(self,conexion,id,job):
        try:
            cursor = conexion.connection.cursor()
            sql = """"UPDATE departments SET job = '{0}' 
            WHERE id = {1}""".format(job.replace('"',"'"),id)
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
            sql2 = """DELETE FROM  jobs"""
            sql = """INSERT INTO jobs (id,job) VALUES """
            completar = ' '
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
                return {'mensaje': "Success"}
            else:
                return {'mensaje': "Avro empty process abort"}
        except Exception as ex:
            return {'mensaje': "Error", 'exito': False}