import requests
import html
import json
from configparser import ConfigParser
import psycopg2 #Dependency used for connection to SparSql database
import time

def config(section, filename='properties.ini'):
    parser = ConfigParser()
    parser.read(filename)

    # Read specific section from config into a python dictionary
    conf = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            conf[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))
    # Returns config as a dict
    return conf

DB_CON = None
def getDbCon(params):
    # Get a connection to sparSql database using given parameters
    global DB_CON
    if DB_CON is None:
        try:
            print('Connecting to the PostgreSQL database...')
            DB_CON = psycopg2.connect(**params)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

def queryWikiData(query):
    # Make POST request to wikidata sparsql service
    url = 'https://query.wikidata.org/sparql'
    body = {'query': query,
            'format': 'json',
            'User-Agent': 'Wikidata schema extraction Bot/1.0 (https://github.com/vehiginters/wikidata_export, vehiginters@gmail.com)',}
    response = requests.post(url, data = body)
    if response.ok:
        return json.loads(response.text)
    elif response.status_code == 429: # To many requests in the last minute, let's wait some time till we make a new one
        sleepTime = 30
        if response.headers["Retry-After"]:
            sleepTime = int(response.headers["Retry-After"])
        print("Query Limit reached. Retrying after {}s".format(sleepTime))
        time.sleep(sleepTime)
        queryWikiData(query)
    else:
        print("WikiData returned response code - {}".format(response.status_code))

def insertClasses(connection, dict):
    # Insert classes from given dictionary into target database
    cur = connection.cursor()
    baseSql = "INSERT INTO sample.classes(iri, cnt, display_name) VALUES('{}', {}, '{}');\n"
    totalSql = ""
    for key, value in dict.items():
        labelValue = value['label']
        if "'" in labelValue:
            labelValue = labelValue.replace("'", "''")
        totalSql = totalSql + baseSql.format(key,value['subclasses'], labelValue)
    cur.execute(totalSql)
    connection.commit()
    cur.close()

def getClasses():
    # Get all of the relevant classes from WikiData with at least 1 instance or subclass
    # First get the classes with subclasses
    print("Getting list of subclass classes...")
    query = """
        select ?class (count(?y) as ?subclasses) where {
           ?y wdt:P279 ?class
        }
        GROUP BY ?class
        ORDER BY DESC(?subclasses)
    """
    responseDict = queryWikiData(query)
    resultDict = {}
    for i in responseDict['results']['bindings']:
        resultDict[i['class']['value']] = {'subclasses':i['subclasses']['value'], 'label': ""}
    # Then get the classes with instances
    print("Getting list of instance classes...")
    query = """
        select ?class (count(?y) as ?instances) where {
          ?y wdt:P31 ?class
        }
        GROUP BY ?class
        ORDER BY DESC(?instances)
    """
    responseDict = queryWikiData(query)
    for i in responseDict['results']['bindings']:
        if i['class']['value'] in resultDict:
            resultDict[i['class']['value']]['subclasses'] = resultDict[i['class']['value']]['subclasses'] + i['instances']['value']
        else:
            resultDict[i['class']['value']] = {'subclasses':i['instances']['value'], 'label': ""}
    return resultDict

def getClassLabels(classDict):
    # Get labels for classes in a given dictionary
    totalClasses = len(classDict)
    print("Getting class labels for {} classes...".format(totalClasses))
    query = """
        select ?class ?classLabel where {{
           VALUES ?class {{{}}}
           SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
        }}
    """
    i = 0
    classList = ""
    for key in classDict:
        i = i + 1
        classList = classList + " wd:" + key[31:]
        # Query wikidata in batches of 20000 to maximize query time and minimize amount of queries
        if ((i % 20000) == 0) or (i == totalClasses):
            responseDict = queryWikiData(query.format(classList))
            for j in responseDict['results']['bindings']:
                if j['class']['value'] in classDict:
                    classDict[j['class']['value']]['label'] = j['classLabel']['value']
            classList = ""
            print("{} done...".format(i))

if __name__ == '__main__': 
    dict = getClasses()
    getClassLabels(dict)
    conf = config('postgreSqlConnection')
    
    getDbCon(conf)
    insertClasses(DB_CON, dict)
    DB_CON.close()