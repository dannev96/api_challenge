class Config:
    SECRET_KEY = 'V!2v4MAEu0D%^kkhOU*R^'

class DevelopmentConfig(Config):
    DEBUG = True
    MYSQL_HOST = 'localhost'
    MYSQL_USER = 'root'
    MYSQL_PASSWORD = 'root123'
    MYSQL_DB = 'challenge'


config = {
    'development': DevelopmentConfig
}