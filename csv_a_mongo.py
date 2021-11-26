from pymongo import MongoClient
import datetime


def sacar_info(line):
    elementos = line.split(',')
    fecha = datetime.datetime.strptime(elementos[1], "%Y-%m-%d")
    simbol = elementos[2]
    price = float(elementos[3])
    return {"fecha": fecha, "simbolo": simbol, "precio": price}

def volcar(moneda):
    with open(moneda) as coin:
        objetos = [sacar_info(line) for line in coin.readlines()[2:]]
    client = MongoClient('mongodb://localhost:27017/')
    db = client['criptomonedas']
    table = moneda.replace('.csv', '')
    collection = db[table]
    collection.insert_many(objetos)

volcar('bitcoin.csv')
# volcar('dogecoin.csv')
# volcar('ethereum.csv')
# volcar('verge.csv')
