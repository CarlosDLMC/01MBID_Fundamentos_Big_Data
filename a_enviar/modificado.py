"""
Ejemplo de importación de Social Media en Mongo DB

Importación de twits en una base de datos Mongo.


ANTES DE EJECUTAR ESTE SCRIPT, ES NECESARIO:
    1. Tener instalado Python 3.7 o superior
    2. Haber creado un archivo CSV que contenga las cuentas de twitter que se desean descargar.
    3. Tener instancia en la nube de MongoDB Atlas, MongoDB instalado en local o en un servidor propio.

EL CÓDIGO SE DIVIDO EN 7 PARTES
    1. Importación de los paquetes de python necesarios
    2. Importación de Twython y Twitter app key and access token
       - ES NECESARIO MODIFICAR ESTA SECCIÓN CON EL FIN DE CONSEGUIR QUE EL SCRIPT FUNCIONE
    3. Definición para leer datos de Twitter
    4. Configurar la base de datos Mongo y las colecciones
    5. Leer las cuentas de Twitter (y añadir a MongoDB si es posible)
    6. Bucle principal sobre el cual las cuentas de Twitter se añaden a la colección de cuentas en MongoDB
    7. Impresión del número de tweets en la base de datos por cuenta.

"""


###### PARTE 1: Importación de los paquetes de python necesarios  ######
#import sys
import time
import json
import pandas as pd
import pymongo
from twython import Twython #Necesario instalarlo la primera vez de forma aislada: pip install Twython
import timeit
from datetime import datetime



###### PARTE 2: Importación de Twython y Twitter app key and access token
#      Información de las Key y Token disponible en la cuenta de Twitter Developer o la común compartida por el profesor
######

APP_KEY = 'PyPLZfkAx7n9hTm7BfOD996fu' # API Key
APP_SECRET = '8hCuVQRAVAhQGEJ649BZCHDCoHxaJ8PB3kOwAcJxM6mSVvzdZc' # API Secret Key
OAUTH_TOKEN = '1254863830682873863-BNHo8U504mKS1YELiZeMGDZ1Krfw22' # Access Token
OAUTH_TOKEN_SECRET = 'UnUaFxL5wLDOkJVoJDHcqb1GNhCCzQJKL9FyVfoyCp3Q1' # Access Token Secret

twitter = Twython(APP_KEY, APP_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)



###### PARTE 3: Definición para leer datos de Twitter ######

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

#Para visualizar los campos (keys del diccionario) introducidos
#    if len(d)>0:
#        print ("d.keys(): ", d[0].keys())


###### PARTE 4: Configurar la base de datos Mongo y las colecciones ######

#Establecimiento Conexión a MongoDB Atlas


# Datos de ejemplo, necesario modificar por vuestra instancia en MongoDB Atlas o local
# dbStringConnection = "mongodb+srv://viu:viuMBID@cluster0.f2esa.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
dbStringConnection = 'mongodb://localhost:27017/'

dbName = 'criptomonedas'
dbCollectionA = 'cuentas'
dbCollectionT = 'tweets'

client = pymongo.MongoClient(dbStringConnection)

# Definición de la base de datos MongoDB
db = client[dbName]

# Crea la colección accounts in la base de datos para los detalles de las cuentas de twitter
accounts = db[dbCollectionA]

# Crea un índice sobre la colección de cuentas para evitar la inserción de duplicados
db[dbCollectionA].create_index([('Twitter_handle', pymongo.ASCENDING)], unique=True)

# Muestra los índices sobre la colección accounts
#list(db.accounts.index_information())

#Muestra el número de cuentas en la colección accounts
#accounts.count_documents({})

# Crea la colleción donde se insertarán los tweets
tweets = db[dbCollectionT]

# Crea un índice de unicidad en la colección tweets para evitar duplicados
db[dbCollectionT].create_index([('id_str', pymongo.ASCENDING)], unique=True)

#Muestra los índices sobre la colección tweets
#list(db.tweets.index_information())

#Muestra el número de tweets en la tabla
#tweets.count_documents({})

#Para ver el listado de las bases de datos en MongoDB
#client.database_names()

#Para ver el listado de las colecciones en la base de datos 'MLB'
#db.collection_names()


###### PARTE 5: Leer las cuentas de Twitter (y añadir a MongoDB si es posible)

#Siempre trata de insertar las cuentas, si ya tenemos todas podemos cambiar a la otra versón que primero comprueba
#Substituir el fichero accountsEMBS.csv por el fichero csv propio con los datos de las cuentas de Twitter

df = pd.read_csv('cuentasCriptomonedas.csv', encoding='latin-1')

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
#records = json.loads(df.T.to_json()).values()
#try:
#    print(accounts.insert_many(records))
#except pymongo.errors.BulkWriteError:
#    print ('BulkWriteError\n');


# Alternativa, solo leer y cargar a MongoDB sí la colleción de cuentas es vacía
#if accounts.count_documents({}) < 1:
#    df = pd.read_csv('accountsEMBS.csv', encoding='latin-1')
#    records = json.loads(df.T.to_json()).values()
#    print ("No account data in MongoDB, attempting to insert", len(records), "records")
#    try:
#        accounts.insert_many(records)
#    except pymongo.errors.BulkWriteError:
#        print ('BulkWriteError\n');
        #pass
#else:
 #   print ("There are already", accounts.count_documents({}), "records in the *accounts* table")




# Listar documentos en la colección accounts
#list(accounts.find())[:1]

# Crea la lista de cuentas de twitter para descargar tweets
twitter_accounts = accounts.distinct('Twitter_handle')
#print len(twitter_accounts)
#twitter_accounts[:5]

###### PARTE 6: Bucle principal sobre el cual las cuentas de Twitter se añaden a la colección de cuentas en MongoDB   ######

start_time = timeit.default_timer()

starting_count = tweets.count_documents({})

#for s in twitter_accounts[:len(twitter_accounts)]:
for s in twitter_accounts[:len(twitter_accounts)]:
    # s = Twitter_handle
    #Fija el contador de duplicados para esta cuenta de twitter a cero
    duplicates = 0

    #Comprueba el ratio límite de llamadas por minuto en el API de twitter (900 peticiones/15-minutos)
    rate_limit = twitter.get_application_rate_limit_status()['resources']['statuses']['/statuses/user_timeline']['remaining']
    print ('\n', rate_limit, '# llamadas a la API restantes')

    #tweet_id = str(mentions.find_one( { "query_screen_name": s}, sort=[("id_str", 1)])["id_str"])
    print ('\nLeyendo tweets enviados por: ', s, '-- index: ', twitter_accounts.index(s))

    page = 1
    total_tweets = 0
    #Se pueden descargar 200 tweets por llamada y hasta 3.200 tweets totales, es decir, 16 páginas por cuenta
    while page < 17:
        print ("--- STARTING PAGE", page, '...llamadas a la API restantes estimadas: ', rate_limit)
        d = get_data_user_timeline_all_pages(s, page)

        #Decrementamos en 1 el contador rate_limit
        rate_limit -= 1
        print ('..........llamadas a la API restantes estimadas: ', rate_limit)

        ##### Escribimos los datos en MongoDB -- iteramos sobre cada tweet
        for i in range(len(d)):
            entry = d[i]
            entry['estudiante_creador'] = "Carlos de la Morena Coco"
            entry['fecha_de_volcado_a_DB'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            #Convertimos los datos de twitter para insertarlos en Mongo
            t = json.dumps(entry)
            #print 'type(t)', type(t)                   #<type 'str'>
            loaded_entry = json.loads(t)
            #print type(loaded_entry) , loaded_entry    #<type 'dict'>

            #Insertamos el tweet en la base de datos -- A menos que este ya existe
            try:
                 tweets.insert_one(loaded_entry)
                 if page == 1 and not i:
                     accounts.update_one({'Twitter_handle': s}, {'$set': {"earliest_tweet_in_db": loaded_entry}})

            except pymongo.errors.DuplicateKeyError as e:
                print (e, '\n')
                duplicates += 1
                if duplicates>9:
                    break
                pass

        if len(d) < 200:
            total_tweets = (page - 1) * 200 + len(d)
            print(f"Esta es la última página. Tweets totales de esta cuenta: {total_tweets}")
            accounts.update_one({'Twitter_handle': s}, {'$set': {"number_of_tweets_in_db": total_tweets}})
            break

        print ('--- PÁGINA FINALIZADA ', page, ' PARA ORGANIZAR ', s, "--", len(d), " TWEETS")

        #Si hay demasiados duplicados entonces saltamos a la siguietne cuenta
        if duplicates > 9:
            print ('\n********************Hay %s' % duplicates, 'duplicados....saltando al siguiente ID ********************\n')
            #continue
            break

        page += 1

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
    else:
        total_tweets = (page - 1) * 200
        print(f"Esta es la última página. Tweets totales de esta cuenta: {total_tweets}")
        accounts.update_one({'Twitter_handle': s}, {'$set': {"number_of_tweets_in_db": total_tweets}})

    #if twitter.get_application_rate_limit_status()['resources']['search']['/search/tweets']['remaining']<5:
    if rate_limit < 5:
        print ('Quedan menos de 5 llamadas API estimadas ... pausando por 5 minutos...')
        time.sleep(300) #PAUSA POR 300 SEGUNDOS


elapsed = timeit.default_timer() - start_time
print ('# de minutos: ', elapsed/60)
print ("Número de nuevos tweets añadidos en esta ejecución: ", tweets.count_documents({}) - starting_count)
print ("Número de tweets actuales en la BD: ", tweets.count_documents({}), '\n', '\n')





###### PARTE 7: Impresión del número de tweets en la base de datos por cuenta. ######

for org in db.tweets.aggregate([
    {"$group":{"_id":"$screen_name", "sum":{"$sum":1}}}
    ]):
    print (org['_id'], org['sum'])
