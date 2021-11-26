import time
import json
import pandas as pd
import pymongo
from twython import Twython #Necesario instalarlo la primera vez de forma aislada: pip install Twython
import timeit
from datetime import datetime

def get_data_user_timeline_all_pages(kid, page):
    try:
        '''
        'count' especifica el número de tweets que se deben intentar recuperar, hasta un máximo de 200
 por solicitud distinta. El valor del conteo se considera mejor como un límite para
 el número de tweets que se devolverán porque se eliminó el contenido suspendido o eliminado
 después de que el recuento ha sido aplicado Incluimos retweets en el conteo, incluso si
 include_rts no se suministra. Se recomienda que siempre envíe include_rts = 1 cuando
 utilizando este método API.
        '''
        d = twitter.get_user_timeline(screen_name=kid, count="200", page=page, include_entities="true", include_rts="1")
    except Exception as e:
        print ("Error reading id %s, exception: %s" % (kid, e))
        return None
    return d

APP_KEY = 'PyPLZfkAx7n9hTm7BfOD996fu' # API \
APP_SECRET = '8hCuVQRAVAhQGEJ649BZCHDCoHxaJ8PB3kOwAcJxM6mSVvzdZc' # API Secret Key
OAUTH_TOKEN = '1254863830682873863-BNHo8U504mKS1YELiZeMGDZ1Krfw22' # Access Token
OAUTH_TOKEN_SECRET = 'UnUaFxL5wLDOkJVoJDHcqb1GNhCCzQJKL9FyVfoyCp3Q1' # Access Token Secret

twitter = Twython(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)

dbName = 'Fundamentos_Big_Data'
dbCollectionA = 'cuentas'
dbCollectionT = 'tweets'
dbStringConnection = 'mongodb://localhost:27017/'
client = pymongo.MongoClient(dbStringConnection)

db = client[dbName]
accounts = db[dbCollectionA]
db[dbCollectionA].create_index([('Twitter_handle', pymongo.ASCENDING)], unique=True)
tweets = db[dbCollectionT]
db[dbCollectionT].create_index([('id_str', pymongo.ASCENDING)], unique=True)

# Metemos las cuentas en la base de datos.
df = pd.read_csv('accountsEMBS.csv', encoding='latin-1')

print ("Intentando insertar ", len(df), " cuentas de Twitter")

repetidas = 0
cuentasTwitter = json.loads(df.T.to_json()).values()

print(cuentasTwitter)

for cuenta in cuentasTwitter:
    try:
        accounts.insert_one(cuenta)
    except pymongo.errors.DuplicateKeyError as e:
        print (e, '\n')
        repetidas += 1

twitter_accounts = accounts.distinct('Twitter_handle')

for cuenta in twitter_accounts:
    duplicates = 0

    # Comprueba el ratio límite de llamadas por minuto en el API de twitter (900 peticiones/15-minutos)
    rate_limit = twitter.get_application_rate_limit_status()['resources']['statuses']['/statuses/user_timeline'][
        'remaining']
    print('\n', rate_limit, '# llamadas a la API restantes')

    page = 1

    while page < 16:
        d = get_data_user_timeline_all_pages(cuenta, page)
        cantidad = len(d)
        if cantidad < 200:
            print(f"esta cuenta tiene {cantidad} páginas")
            break
        page += 1