class DevelopmentConfig():
    DEBUG = True
    MYSQL_HOST = 'localhost'
    MYSQL_USER = 'root'
    MYSQL_PASSWORD = 'root123'
    MYSQL_DB = 'challenge'


config = {
    'development': DevelopmentConfig
}