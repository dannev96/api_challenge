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
            C.department as department,B.job as job,A.Q1 as q1,A.Q2 as q2,A.Q3 as q3,A.Q4 as q4
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