import requests
import html
import json
from configparser import ConfigParser
import psycopg2 #Dependency used for connection to postgreSql database
import time
import math

#List used for keeping track of number of queries in the last minute
LAST_MINUTE_EVENTS = list()

def countPastQueries():
    # Function used to go over the last event list and cut off the list after finding the first expired event
    global LAST_MINUTE_EVENTS
    TIME_WINDOW = 60
    tim=time.time()  # called only once
    for idx in range(len(LAST_MINUTE_EVENTS)-1,-1,-1):
        if LAST_MINUTE_EVENTS[idx]+TIME_WINDOW<= tim:
            LAST_MINUTE_EVENTS[:idx+1]=""
            break
    return len(LAST_MINUTE_EVENTS)

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
            raise Exception("Failed to connect to PostgreSQL database - {}".format(error))
    return DB_CON

def queryWikiData(query, retries=0):
    global LAST_MINUTE_EVENTS
    # Make POST request to wikidata sparsql service
    lastMinuteEvents = countPastQueries()
    if lastMinuteEvents > 10:
        # If more than 10 queries been made in the last minute wait for a while, not to fail
        print("More than 10 queries in the last minute, waiting for 60s")
        time.sleep(61)
    if retries == 3:
        # TODO - Currently there is a problem, that a failing query even if it was because of too many requests,
        # the failing query will go into an endless fail loop
        print("Bad requests loop skipping query for now")
        return {}
    url = 'https://query.wikidata.org/sparql'
    body = {'query': query,
            'format': 'json',
            # Proper user-agent to identify the caller as specified by WikiData query API specification
            'User-Agent': 'Wikidata schema extraction Bot/1.0 (https://github.com/vehiginters/wikidata_export, vehiginters@gmail.com)',}
    LAST_MINUTE_EVENTS.append(time.time())
    response = requests.post(url, data = body)
    if response.ok:
        return json.loads(response.text)['results']['bindings']
    elif response.status_code == 429 or response.status_code == 503 : # To many requests in the last minute, let's wait some time till we make a new one
        sleepTime = 60
        if "Retry-After" in response.headers:
            sleepTime = int(response.headers["Retry-After"])
        print("Query Limit reached. Retrying after {}s".format(sleepTime))
        time.sleep(sleepTime+1)
        queryWikiData(query, retries+1)
    # TO-DO failed queries should be logged, not printed out in stdout
    elif response.status_code == 502: # Bad gateway server, let's just retry the query
        print("Got bad gateway server in response, retrying query...")
        time.sleep(30)
        queryWikiData(query, retries+1)
    elif response.status_code == 500: # Query timeout, can't do much about this besides skipping
        print("Query timed out, skipping query - {}".format(query))
        return {}
    else:
        print("WikiData returned response code - {}".format(response.status_code))
        print("Failed query - {}".format(query))

def insertClasses(connection, dict):
    # Insert classes from given dictionary into target database
    cur = connection.cursor()
    # Subclasses used while developing, just to see how many subclasses for relevant classes are there
    baseSql = "INSERT INTO sample.classes(iri, cnt, display_name, local_name, is_unique, subclasses) VALUES('{}', {}, '{}', '{}', true, '{}');\n"
    totalSql = ""
    for key, value in dict.items():
        labelValue = value['label']
        if "'" in labelValue:
            # " ' " needs to be escaped for postgresql 
            labelValue = labelValue.replace("'", "''")
        totalSql = totalSql + baseSql.format(key,value['instances'], labelValue, value['localname'], value['subclasses'])
    cur.execute(totalSql)
    connection.commit()
    cur.close()

def insertProperties(connection, dict):
    # Insert classes from given dictionary into target database
    cur = connection.cursor()
    baseSql = "INSERT INTO sample.properties(iri, cnt, display_name) VALUES('{}', {}, '{}');\n"
    totalSql = ""
    for key, value in dict.items():
        useCount = value['useCount']
        if useCount > 2100000000:
            # There is one property with more than 2'100'000'000, which goes out of properties table 'cnt' column integer range, so just put it at limit
            useCount = 2100000000
        labelValue = value['label']
        if "'" in labelValue:
            # Same as for classes " ' " needs to be escaped for postgresql
            labelValue = labelValue.replace("'", "''")
        totalSql = totalSql + baseSql.format(key, useCount, labelValue)
    cur.execute(totalSql)
    connection.commit()
    cur.close()

def insertClassPropertyRelations(cursor, relationList, outgoingRelations):
    # As the Python script has no idea about IDs of the classes, just tell the SQL to select them based on class and property iri's
    # Should watch out, as the iri technically could not be unique, as that could brake this SQL
    baseSql = '''
        INSERT INTO sample.cp_rels(class_id, property_id, type_id, cnt, object_cnt)
        SELECT (SELECT id from sample.classes WHERE iri = '{classIri}') AS cl_id,
        (SELECT id from sample.properties WHERE iri = '{propIri}') AS pr_id,
        (SELECT id from sample.cp_rel_types WHERE name = '{propertyDirection}'),
        {cnt},
        {objectCnt}
        HAVING (SELECT id from sample.classes WHERE iri = '{classIri}') IS NOT NULL
        AND (SELECT id from sample.properties WHERE iri = '{propIri}') IS NOT NULL;
    '''
    propertyDirectionString = "Outgoing" if outgoingRelations else "Incoming"
    totalSql = ""
    totalRelations = len(relationList)
    print("Inserting {} {} property relations into target database...".format(totalRelations, propertyDirectionString))
    i = 0
    for class1, propery, cnt, objectCnt  in relationList:
        i = i + 1
        totalSql = totalSql + baseSql.format(classIri = class1, propIri = propery, propertyDirection = propertyDirectionString, cnt = cnt, objectCnt = objectCnt)
        if ((i % 50000) == 0) or (i == totalRelations): 
            cursor.execute(totalSql)
            totalSql = ""

def updateClassPropertyRelations(cursor, relationList):
    baseSql = '''
        UPDATE sample.cp_rels
        SET object_cnt = {objectCnt}
        WHERE class_id = (SELECT id from sample.classes WHERE iri = '{classIri}')
        AND property_id = (SELECT id from sample.properties WHERE iri = '{propIri}')
        AND type_id = (SELECT id from sample.cp_rel_types WHERE name = 'Outgoing'));
    '''
    totalSql = ""
    totalRelations = len(relationList)
    print("Updating {} property relations into target database...".format(totalRelations))
    i = 0
    for class1, prop, objectCnt  in relationList:
        i = i + 1
        totalSql = totalSql + baseSql.format(classIri = class1, propIri = prop, objectCnt = objectCnt)
        if ((i % 50000) == 0) or (i == totalRelations):
            cursor.execute(totalSql)
            totalSql = ""

def insertClassClassRelations(cursor, relationList):
    print("Inserting class relations into target database")
    # As the Python script has no idea about IDs of the classes, just tell the SQL to select them based on class and property iri's
    # Should watch out, as the iri technically could not be unique, as that could brake this SQL
    baseSql = '''
        INSERT INTO sample.cc_rels(class_1_id, class_2_id, type_id)
        SELECT (SELECT id from sample.classes WHERE iri = '{class1Iri}') AS cl_id,
        (SELECT id from sample.classes WHERE iri = '{class2Iri}') AS cl2_id,
        (SELECT id from sample.cc_rel_types WHERE name = 'sub_class_of')
        HAVING (SELECT id from sample.classes WHERE iri = '{class1Iri}') IS NOT NULL
        AND (SELECT id from sample.classes WHERE iri = '{class2Iri}') IS NOT NULL;
    '''
    totalSql = ""
    totalRelations = len(relationList)
    i = 0
    for class1, class2  in relationList:
        totalSql = totalSql + baseSql.format(class1Iri = class1, class2Iri = class2)
        if ((i % 50000) == 0) or (i == totalRelations): 
            cursor.execute(totalSql)
            totalSql = ""
    # Don't commit transaction just yet, because these relations are inserted in batches and not all at once

def getProperties():
    print("Getting list of properties...")
    query = """
        SELECT DISTINCT ?property (COUNT(?item) as ?useCount) WHERE {{
           ?item ?property ?propValue
        }}
        GROUP BY ?property
        ORDER BY DESC(?useCount)
    """
    responseDict = queryWikiData(query)
    resultDict = {}
    if responseDict is not None:
        for i in responseDict:
            resultDict[i['property']['value']] = {'useCount':int(i['useCount']['value']), 'label': ""}
    return resultDict

def getPropertyLabels(propertiesDict):
    # Get labels for classes in a given dictionary
    totalProps = len(propertiesDict)
    print("Getting property labels for {} properties...".format(totalProps))
    query = """
        SELECT DISTINCT ?property ?propLabel WHERE {{
          VALUES ?property {{ {} }}
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
          ?prop wikibase:directClaim ?property
        }}
    """
    i = 0
    propertyList = ""
    for key in propertiesDict:
        i = i + 1
        propertyList = propertyList + " <" + key + ">"
        # Query wikidata in batches of 15000 to maximize query time and minimize amount of queries
        # Can't query in much bigger batches as then queries start to reach payload limit
        if ((i % 15000) == 0) or (i == totalProps):
            responseDict = queryWikiData(query.format(propertyList))
            if responseDict is not None:
                for j in responseDict:
                    if j['property']['value'] in propertiesDict:
                        propertiesDict[j['property']['value']]['label'] = j['propLabel']['value']
            propertyList = ""
            print("{:.1%} done...".format(i/float(totalProps)))

def getClassClassRelations(connection, classDict):
    print("Getting Class-Class relations...")
    # A little complicated function, that gets all subclass relations between all relevant classes
    query = """
        SELECT DISTINCT ?class ?subclass WHERE {{
          ?subclass wdt:P279 ?class.
          VALUES ?class {{ {} }}
        }}
    """
    i = 0
    relationsCounter = 0
    totalClasses = len(classDict)
    classList = ""
    relationList = []
    collectedClasses = 0
    cur = connection.cursor()
    totalInsertedRelations = 0
    # Iteration logic: for first iteration just take the class values and continue, for rest first check if to many subclasses collected and only after query collect the current class
    # This is done so that we can kinda look at the number of subclasses for next class in dictionary
    for key, value in classDict.items():
        i = i + 1
        if i == 1 or i == totalClasses:
            collectedClasses = collectedClasses + 1
            classList = classList + " <" + key + ">"
            relationsCounter = relationsCounter + int(value['subclasses'])
            if i == 1:
                continue
        # First check if either the max number of subclasses for collected classes go over 1mil, a total of 15000 classes is collected or if its the last class
        if ((relationsCounter + int(value['subclasses'])) > 1000000) or (collectedClasses  == 15000) or (i == totalClasses):
            # If either check is true, query wikidata to get related classes for batch of the collected classes
            responseDict = queryWikiData(query.format(classList))
            # It was a bit heavy to check if the subclass is relevant on wikidata, so it is done within Python
            # But then again we get max 1mil of result rows in response, which can take up to 1GB RAM for a few secs, while the response is processed
            if responseDict is not None:
                for j in responseDict:
                    if j['subclass']['value'] in classDict:
                        relationList.append((j['class']['value'], j['subclass']['value']))
                responseDict.clear() # Clear the response dict as fast as we can, to free up used memory
            classList = ""
            relationsCounter = 0
            collectedClasses = 0
            currentRelations = len(relationList)
            print("Relations for {}/{} classes done...".format(i, totalClasses))
            # After we collect more then 50k relations, or we are at the end, insert the class relations, but dont commit the transaction yet
            if (currentRelations > 50000) or (i == totalClasses):
                insertClassClassRelations(cur, relationList)
                totalInsertedRelations = totalInsertedRelations + currentRelations
                print("{} Class relations collected".format(totalInsertedRelations))
                relationList.clear()
        collectedClasses = collectedClasses + 1
        classList = classList + " <" + key + ">"
        relationsCounter = relationsCounter + int(value['subclasses'])
    connection.commit()
    cur.close()

def getClassPropertyRelations(connection, classDict, outgoingRelations=True):
    # Implements a very similar algorithm as for class-class relations, but just collecting classes based on instance count
    # Outgoing properties - 400k instance limit, otherwise timeouts
    # Incoming properties - 2mil instance limit, otherwise timeouts
    # This goes on for quite a while, taking up to 4 hours to get the outgoing and incoming properties
    propertyLine = "?x ?property ?y \n"
    classInstanceLimit = 400000
    classInstanceLimit2 = 200000
    classAmountLimit = 5000
    propertyDirectionString = "Outgoing" if outgoingRelations else "Incoming"
    if not outgoingRelations:
        # For Incoming relations we can't batch together too many classes, as class instance amount doesn't perfectly correlate to query time 
        classInstanceLimit = 2000000
        propertyLine = "?y ?property ?x. \n"
    query = """
        SELECT ?property ?class (COUNT(?y) AS ?propertyInstances) WHERE {{
           ?x wdt:P31 ?class.""" + propertyLine + """ VALUES ?class {{ {} }}
        }}
        GROUP BY ?property ?class
    """
    i = 0
    k = 0
    instanceCounter = 0
    totalClasses = len(classDict)
    classList = ""
    relationList = []
    collectedClasses = 0
    cur = connection.cursor()
    totalInsertedRelations = 0
    print("Getting {} class-property relations for {} classes...".format(propertyDirectionString, totalClasses))
    for key, value in classDict.items():
        i = i + 1
        if int(value['instances']) > classInstanceLimit:
            # TO-DO should specifically process these larger classes, not just skip them
            print("Class {} has too many instances, query will timeout, so skipping for now".format(key))
            continue
        # K: Counter to know which is the first processed class, can't use i for this, as we need also way to tell if it's the last class overall
        k = k + 1
        if k == 1 or i == totalClasses:
            collectedClasses = collectedClasses + 1
            classList = classList + " <" + key + ">"
            instanceCounter = instanceCounter + int(value['instances'])
            if k == 1:
                continue
        if not outgoingRelations:
            # Calculate batch limits, so that for smaller classes as many classes are batched together, query doesn't time out. Problematic only for incoming relations
            power = math.floor(math.log(i, 10))
            classInstanceLimit2 = 1000000/pow(2, power)
            classAmountLimit = pow(10, power) if power < 4 else 1000
        if ((instanceCounter + int(value['instances'])) > classInstanceLimit2) or (collectedClasses  == classAmountLimit) or (i == totalClasses):
            responseDict = queryWikiData(query.format(classList))
            if responseDict is not None:
                for j in responseDict:
                    objectCnt = 0
                    if not outgoingRelations:
                        objectCnt = int(j['propertyInstances']['value'])
                    relationList.append((j['class']['value'], j['property']['value'], int(j['propertyInstances']['value']), objectCnt))
                responseDict.clear() # Clear the response dict as fast as we can, to free up used memory
            classList = ""
            instanceCounter = 0
            collectedClasses = 0
            currentRelations = len(relationList)
            print("{} property relations for {}/{} classes done...".format(propertyDirectionString, i, totalClasses))
            if (currentRelations > 50000) or (i == totalClasses):
                insertClassPropertyRelations(cur, relationList, outgoingRelations)
                totalInsertedRelations = totalInsertedRelations + currentRelations
                print("{} {} relations collected".format(propertyDirectionString, totalInsertedRelations))
                relationList.clear()
        collectedClasses = collectedClasses + 1
        classList = classList + " <" + key + ">"
        instanceCounter = instanceCounter + int(value['instances'])
    connection.commit()
    cur.close()

def updateClassPropertyObjCount(connection, classDict):
    query = """
        SELECT ?property ?class (COUNT(?y) AS ?objectCnt) WHERE {{
           ?x wdt:P31 ?class.
           ?x ?property ?y.
           FILTER  isIRI(?y)
           VALUES ?class {{ {} }}
        }}
        GROUP BY ?property ?class
    """
    i = 0
    k = 0
    instanceCounter = 0
    totalClasses = len(classDict)
    classList = ""
    relationList = []
    collectedClasses = 0
    cur = connection.cursor()
    totalUpdatedRelations = 0
    print("Updating outgoing class-property relation object count for {} classes...".format(totalClasses))
    for key, value in classDict.items():
        i = i + 1
        if int(value['instances']) > 400000:
            # TO-DO should specifically process these larger classes, not just skip them
            print("Class {} has too many instances, query will timeout, so skipping for now".format(key))
            continue
        # K: Counter to know which is the first processed class, can't use i for this, as we need also way to tell if it's the last class overall
        k = k + 1
        if k == 1 or i == totalClasses:
            collectedClasses = collectedClasses + 1
            classList = classList + " <" + key + ">"
            instanceCounter = instanceCounter + int(value['instances'])
            if k == 1:
                continue
        if ((instanceCounter + int(value['instances'])) > 400000) or (collectedClasses  == 5000) or (i == totalClasses):
            responseDict = queryWikiData(query.format(classList))
            if responseDict is not None:
                for j in responseDict:
                    relationList.append((j['class']['value'], j['property']['value'], int(j['objectCnt']['value'])))
                responseDict.clear() # Clear the response dict as fast as we can, to free up used memory
            classList = ""
            instanceCounter = 0
            collectedClasses = 0
            currentRelations = len(relationList)
            print("Outgoing class-property relation object count for {}/{} updated...".format(i, totalClasses))
            if (currentRelations > 50000) or (i == totalClasses):
                updateClassPropertyRelations(cur, relationList)
                totalUpdatedRelations = totalUpdatedRelations + currentRelations
                print("{} outgoing relations updated".format(totalUpdatedRelations))
                relationList.clear()
        collectedClasses = collectedClasses + 1
        classList = classList + " <" + key + ">"
        instanceCounter = instanceCounter + int(value['instances'])
    connection.commit()
    cur.close()

def getClasses():
    # Get all of the relevant classes from WikiData with at least 1 instance
    # First get the classes with their instance count
    print("Getting list classes...")
    query = """
        SELECT ?class (COUNT(?y) as ?instances) WHERE {{
           ?y wdt:P31 ?class.
        }}
        GROUP BY ?class
        ORDER BY DESC(?instances)
    """
    responseDict = queryWikiData(query)
    classDict = {}
    if responseDict is not None:
        for i in responseDict:
            localName = ""
            if i['class']['value'][:31] == "http://www.wikidata.org/entity/":
                localName = i['class']['value'][31:]
            classDict[i['class']['value']] = {'instances':int(i['instances']['value']), 'label': "", 'subclasses': 0, 'localname': localName}
    print("{} classes retrieved".format(len(classDict)))
    # Then count the number of subclasses for each class, later used for getting class relations
    print("Counting class subclasses...")
    query = """
        SELECT ?class (COUNT(?y) as ?subclasses) where {{
           ?y wdt:P279 ?class.
        }}
        GROUP BY ?class
        ORDER BY DESC(?subclasses)
    """
    responseDict = queryWikiData(query)
    if responseDict is not None:
        for i in responseDict:
            if i['class']['value'] in classDict:
                classDict[i['class']['value']]['subclasses'] = int(i['subclasses']['value'])
    return classDict

def getClassLabels(classDict):
    # Get labels for classes in a given dictionary
    totalClasses = len(classDict)
    print("Getting class labels for {} classes...".format(totalClasses))
    query = """
        SELECT ?class ?classLabel WHERE {{
           VALUES ?class {{ {} }}
           SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
        }}
    """
    i = 0
    classList = ""
    for key in classDict:
        i = i + 1
        classList = classList + " <" + key + ">"
        # Query wikidata in batches of 15000 to maximize query time and minimize amount of queries
        if ((i % 15000) == 0) or (i == totalClasses):
            responseDict = queryWikiData(query.format(classList))
            if responseDict is not None:
                for j in responseDict:
                    if j['class']['value'] in classDict:
                        classDict[j['class']['value']]['label'] = j['classLabel']['value']
            classList = ""
            print("{:.1%} done...".format(i/float(totalClasses)))

if __name__ == '__main__': 
    conf = config('postgreSqlConnection')

    databaseCon = getDbCon(conf)

    propDict = getProperties()
    getPropertyLabels(propDict)
    insertProperties(databaseCon, propDict)
    propDict.clear() # Clear the massive dictionary, to not take up RAM space

    classDict = getClasses()
    getClassLabels(classDict)
    insertClasses(databaseCon, classDict)
    getClassPropertyRelations(databaseCon, classDict, outgoingRelations=False)
    getClassPropertyRelations(databaseCon, classDict, outgoingRelations=True)
    updateClassPropertyObjCount(databaseCon, classDict)
    getClassClassRelations(databaseCon, classDict)
    classDict.clear()

    databaseCon.close()
