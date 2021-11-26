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

dbStringConnection = 'mongodb://localhost:27017/'

dbName = 'Fundamentos_Big_Data'
dbCollectionA = 'cuentas'
dbCollectionT = 'tweets'

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
for cuenta in cuentasTwitter:
    try:
        accounts.insert_one(cuenta)
    except pymongo.errors.DuplicateKeyError as e:
        print (e, '\n')
        repetidas += 1
print ("Insertadas ", len(df)-repetidas, " cuentas de Twitter. " , repetidas, " cuentas repetidas.")

# Crea la lista de cuentas de twitter para descargar tweets
twitter_accounts = accounts.distinct('Twitter_handle')

start_time = timeit.default_timer()
starting_count = tweets.count_documents({})

for s in twitter_accounts[:len(twitter_accounts)]: # s = cuenta
    #Fija el contador de duplicados para esta cuenta de twitter a cero
    duplicates = 0

    #Comprueba el ratio límite de llamadas por minuto en el API de twitter (900 peticiones/15-minutos)
    rate_limit = twitter.get_application_rate_limit_status()['resources']['statuses']['/statuses/user_timeline']['remaining']
    print ('\n', rate_limit, '# llamadas a la API restantes')

    #tweet_id = str(mentions.find_one( { "query_screen_name": s}, sort=[("id_str", 1)])["id_str"])
    print ('\nLeyendo tweets enviados por: ', s, '-- index: ', twitter_accounts.index(s))

    page = 1

    #Se pueden descargar 200 tweets por llamada y hasta 3.200 tweets totales, es decir, 16 páginas por cuenta
    while page < 17:
        print ("--- STARTING PAGE", page, '...llamadas a la API restantes estimadas: ', rate_limit)

        d = get_data_user_timeline_all_pages(s, page) # d = pagina
        if not d:
            print ("No hubo tweets devueltos........Desplazandose al siguiente ID")
            break
        if len(d)==0:    #Este registro es diferentes de las menciones y los ficheros DMS
            print ("No hubo tweets devueltos........Desplazandose al siguiente ID")
            break
        #if not d['statuses']:
        #    break


        #Decrementamos en 1 el contador rate_limit
        rate_limit -= 1
        print ('..........llamadas a la API restantes estimadas: ', rate_limit)


        ##### Escribimos los datos en MongoDB -- iteramos sobre cada tweet
        for entry in d: # entry = tweet
            entry['creador_del_trabajo'] = "Carlos de la Morena Coco"
            entry['fecha_volcado_a_DB'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            #Convertimos los datos de twitter para insertarlos en Mongo
            t = json.dumps(entry)
            #print 'type(t)', type(t)                   #<type 'str'>
            loaded_entry = json.loads(t)
            #print type(loaded_entry) , loaded_entry    #<type 'dict'>

            #Insertamos el tweet en la base de datos -- A menos que este ya existe
            try:
                 tweets.insert_one(loaded_entry)
            except pymongo.errors.DuplicateKeyError as e:
                print (e, '\n')
                duplicates += 1
                if duplicates>9:
                    break
                pass


        print ('--- PÁGINA FINALIZADA ', page, ' PARA ORGANIZAR ', s, "--", len(d), " TWEETS")

        #Si hay demasiados duplicados entonces saltamos a la siguietne cuenta
        if duplicates > 9:
            print ('\n********************Hay %s' % duplicates, 'duplicados....saltando al siguiente ID ********************\n')
            #continue
            break

        page += 1
        if page > 16:
            print ("NO SE ENCUENTRA AL FINAL DE LA PÁGINA 16")
            break

        # ESTE ES UN MÉTODO CRUDO QUE PONE EN UN CONTROL DE LÍMITE DE TASA API
        # EL LÍMITE DE VELOCIDAD PARA COMPROBAR CUÁNTAS SON LAS LLAMADAS DE API SON 180, LO QUE SIGNIFICA QUE NO PODEMOS
        if rate_limit < 5:
            print ('Se estiman menos de 5 llamadas API ... verifique y haga una pausa de 5 minutos si es necesario')
            rate_limit_check = twitter.get_application_rate_limit_status()['resources']['statuses']['/statuses/user_timeline']['remaining']
            print ('.......y el límite de ratio de acceso al API actual es: ', rate_limit_check)
            if rate_limit_check<5:
                print ('Quedan menos de 5 llamadas API ... pausando por 5 minutos')
                time.sleep(300) #PAUSA DE 300 SEGUNDOS
                rate_limit = twitter.get_application_rate_limit_status()['resources']['statuses']['/statuses/user_timeline']['remaining']
                print ('.......Este es el límite de ratio de acceso al API después de una pausa de 5 minutos: ', rate_limit)
                #si el rate_limit_check == 900:
                #    rate_limit = 900

    #if twitter.get_application_rate_limit_status()['resources']['search']['/search/tweets']['remaining']<5:
    if rate_limit < 5:
        print ('Quedan menos de 5 llamadas API estimadas ... pausando por 5 minutos...')
        time.sleep(300) #PAUSA POR 300 SEGUNDOS
