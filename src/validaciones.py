# Valida el id (si es numérico y de longitud 6).
def validar_id(id: int) -> bool:
    try:
        id= int(id)
        return ( isinstance(id,int))
    except:
        return False

# Valida el string (si es un texto sin espacios en blanco de entre 1 y 30 caracteres).
def validar_dato1(dato1: str) -> bool:
    try:
        if dato1 == None :
            dato1f = ['department','job']
        else:
            dato1 = dato1.split(',')
            dato1f = []
            for veri in dato1:
                if veri.lower() == 'department' or veri.lower() == 'job' :
                    dato1f.append(veri.lower())
            if len(dato1f) == 0 :
                dato1f = ['department','job']
        return dato1f
    except:
        return ['department','job']

# Valida el string (si es un texto sin espacios en blanco de entre 1 y 30 caracteres).
def validar_dato4(dato4: str) -> bool:
    try:
        if dato4.lower() == 'upper' or dato4.lower() == 'lower' or dato4.lower() == 'all':
            dato4f=dato4.lower()
        else:
            dato4f = 'upper'

        if dato4 == None :
            dato4f = 'upper'
        return dato4f
    except:
        return 'upper'

# Valida el string (si es un texto sin espacios en blanco de entre 1 y 30 caracteres).
def validar_dato2(dato2: str) -> bool:
    try:
        if dato2 == None :
            dato2f = ['2021']
        else:
            dato2 = dato2.split(',')
            dato2f = []
            for veri in dato2:
                try:
                    veri = int(veri)
                    if veri >= 1000 and veri <= 9999:
                        dato2f.append(str(veri))
                    else:
                        dato2f.append('2021')
                except:
                    dato2f.append('2021')
            if len(dato2f) == 0 :
                dato2f = ['2021']
        return dato2f[0]
    except:
        return '2021'

# Valida el string (si es un texto sin espacios en blanco de entre 1 y 30 caracteres).
def validar_dato3(dato3: str) -> bool:
    try:
        if dato3 == None :
            dato3f = ['department asc','job asc']
        else:
            dato3 = dato3.split(',')
            dato3f = []
            for veri in dato3:
                temporal = veri.split(' ')
                cadena = ' '
                flag = 0
                for veri2 in temporal:
                    if( veri2.lower() == 'department' or veri2.lower() == 'job' or veri2.lower() == 'q1' or veri2.lower() == 'q2'or veri2.lower() == 'q3'or veri2.lower() == 'q4') and flag == 0:
                        flag = 1
                        cadena += veri2.lower()
                    if (veri2.lower() == 'asc' or veri2.lower() == 'desc') and flag == 1:
                        flag = 2
                        cadena += ' '+veri2.lower()
                    if flag == 2:
                        break
                if cadena != ' ':
                    dato3f.append(cadena)
            if len(dato3f) == 0 :
                dato3f = ['department asc','job asc']
        return dato3f
    except:
        return [' department asc ',' job asc ']

# Valida el string (si es un texto sin espacios en blanco de entre 1 y 30 caracteres).
def validar_datetime(string: str) -> bool:
    try:
        year=string[0:4]
        firstsep=string[4:5]
        month=string[5:7]
        secondsep=string[7:8]
        day=string[8:10]
        thirdsep=string[10:11]
        hour=string[11:13]
        fourthsep=string[13:14]
        minute=string[14:16]
        fithsep=string[16:17]
        second=string[17:19]
        sixthsep=string[19:20]
        year = int(year)
        month = int(month)
        day = int(day)
        hour = int(hour)
        minute = int(minute)
        second = int(second)
        if firstsep == '-' and secondsep == '-' and thirdsep == 'T' and fourthsep == ':' and fithsep == ':' and sixthsep == 'Z':
            return True  
    except:
        return False

# Valida el string (si es un texto sin espacios en blanco de entre 1 y 30 caracteres).
def validar_string(string: str) -> bool:
    string = string.strip()
    return (len(string) > 0 and len(string) <= 40)

# Valida el código (si es numérico y de longitud 6).
def validar_int(codigo: str) -> bool:
    try:
        return (str(codigo).isnumeric() )
    except:
        return False

# Valida el código (si es numérico y de longitud 6).
def validar_codigo(codigo: str) -> bool:
    return (codigo.isnumeric() and len(codigo) == 6)

# Valida el nombre (si es un texto sin espacios en blanco de entre 1 y 30 caracteres).
def validar_nombre(nombre: str) -> bool:
    nombre = nombre.strip()
    return (len(nombre) > 0 and len(nombre) <= 30)

# Valida que los créditos estén entre 1 y 9.
def validar_creditos(creditos: str) -> bool:
    creditos_texto = str(creditos)
    if creditos_texto.isnumeric():
        return (creditos >= 1 and creditos <= 9)
    else:
        return False