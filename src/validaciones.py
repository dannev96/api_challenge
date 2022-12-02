# Valida el id (si es numérico y de longitud 6).
def validar_id(id: int) -> bool:
    try:
        id= int(id)
        return ( isinstance(id,int))
    except:
        return False

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