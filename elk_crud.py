from dotenv import load_dotenv
from flask import Flask,request
from elasticsearch import Elasticsearch,helpers
from script import *
import pandas as pd
import os 
import csv


load_dotenv()
CLOUD_ID = os.getenv("Cloud_ID")
ELASTIC_PASSWORD = os.getenv("Elastic_Password")
es_index1="datas_2"
es_index2="ip_status"

client = Elasticsearch(
    cloud_id=CLOUD_ID,
    basic_auth=("elastic",ELASTIC_PASSWORD),
)

app = Flask(__name__,template_folder="templates")

@app.route("/")
def hello_world():
    print(client.info())
    return "<p>Root</p>"


#******************** SEARCH COUNT ********************
#Route For Getting The Query Result Count 
@app.route("/search/count",methods = ['POST'])
def search_count():
    req = request.json
    column = req["column"]
    value = req["value"]
    colLenght = len(column)
    valueLenght = len(value) 

    
    if(colLenght != 0 and valueLenght!=0):
        search_query = {
            "query": {
                "prefix": {
                    f"{column}.keyword": value
                }
            }
        }
    elif valueLenght != 0:
        # İlk sayfadaki belgeleri çek
        result = client.search(index=es_index1, size=1)
        # İlk belgeyi al ve içindeki alan isimlerini döndür
        fields = result["hits"]["hits"][0]["_source"].keys() if result["hits"]["hits"] else []
        should_clauses = [{"prefix": {f"{field}.keyword": value}} for field in fields]

        search_query = {
            "query": {
                "bool": {
                    "should": should_clauses,
                    "minimum_should_match": 1
                }
            }
        }
    else:
        search_query = {"query": {"match_all": {}}}

    size_response = client.count(index=es_index1,body=search_query)
    return {"result":"success","count":size_response["count"]}

#******************** SEARCH PAGE ********************
#Route For Paginating the Query Results/ Max rows to Paginate is 10000
@app.route("/search/page",methods = ['POST'])
def search_page():
    req = request.json
    column = req["column"]
    value = req["value"]
    pageNumber = req["pageNumber"]
    pageNumber=int(pageNumber)
    skipValues=pageNumber*10-10

    colLenght = len(column)
    valueLenght = len(value) 
    sort =  [{"pkSeqID.keyword": "desc"}]   

    if(colLenght != 0 and valueLenght!=0):
        search_query = {
            "size": 10,
            "from":skipValues,
            "query": {
                "prefix": {
                    f"{column}.keyword": value
                }
            },
            "sort":sort
        }
    elif valueLenght != 0:
        # İlk sayfadaki belgeleri çek
        result = client.search(index=es_index1, size=1)
        # İlk belgeyi al ve içindeki alan isimlerini döndür
        fields = result["hits"]["hits"][0]["_source"].keys() if result["hits"]["hits"] else []
        should_clauses = [{"prefix": {f"{field}.keyword": value}} for field in fields]

        search_query = {
            "size": 10,
            "from":skipValues,
            "query": {
                "bool": {
                    "should": should_clauses,
                    "minimum_should_match":1
                }
            },
            "sort": sort
        }
    else:
        search_query = {
            "size": 10,
            "query": {
                "match_all": {}
            },
            "sort": sort
        }

    search_response = client.search(index=es_index1,body=search_query)
    df = pd.DataFrame([hit["_source"] for hit in search_response["hits"]["hits"] ])
    if(df.empty):
        return {"result":"empty",}
    else:
        list = df.to_dict('records')
        return {"result":"success","data":list}

#******************** SEARCH AGGREGATION QUERY ********************
#Route For Getting The Aggreation Counts From the Query Result 
@app.route("/search/aggregations", methods = ['POST'] )
def search_aggregation_query():
    
    req = request.json
    column = req["column"]
    value = req["value"]
    sort = req["sort"]
    colLenght = len(column)
    valueLenght = len(value) 

    aggregation = {
            "agg_name": {
                "terms": {
                    "field": f"{sort}.keyword"
                }
            }
    }
    if(colLenght != 0 and valueLenght!=0):
        aggregation_query = {
            "size": 0,
            "query": {
                "prefix": {
                    f"{column}.keyword": value
                }
            },
            "aggs": aggregation
        }
    elif valueLenght != 0:
        # İlk sayfadaki belgeleri çek
        result = client.search(index=es_index1, size=1)
        # İlk belgeyi al ve içindeki alan isimlerini döndür
        fields = result["hits"]["hits"][0]["_source"].keys() if result["hits"]["hits"] else []
        should_clauses = [{"prefix": {f"{field}.keyword": value}} for field in fields]

        aggregation_query = {
            "size": 0,
            "query": {
                "bool": {
                    "should": should_clauses,
                    "minimum_should_match": 1
                }
            },
            "aggs": aggregation
        }
    else:
        aggregation_query = {
            "size": 0,
            "query": {
                "match_all": {}
            },
            "aggs": aggregation
        }

    aggregation_response =  client.search(index=es_index1,body=aggregation_query)
    aggregation_list = aggregation_response["aggregations"]["agg_name"]["buckets"]
    if(len(aggregation_list)==0):
        return {"result":"empty","aggregations":aggregation_list}
    else:
        return {"result":"success","aggregations":aggregation_list}


#********************  ADDRESS AGGREGATION FROM QUERY ********************
#Route For Getting The Ip Adressess From Query Result with Given Category Type 
@app.route("/search/aggregations/getAddresses", methods = ['POST'])
def get_saddr_from_query():
    req = request.json
    column = req["column"]
    colValue = req["colValue"]
    categoryType = req["categoryType"]

    colLenght = len(column)
    valueLenght = len(colValue) 

    aggregation = {
            "agg_name": {
                "multi_terms":{
                    "terms":[ 
                        {"field": "daddr.keyword"},
                        {"field": "saddr.keyword"},
                    ]
                }
            }
        }
    if(colLenght != 0 and valueLenght!=0):
        aggregation_query = {
            "query": {
                "bool" : {
                    "must":[
                        {
                        "match": {
                            f"{column}.keyword" : colValue
                            }
                        },
                        {
                        "match": {
                            "category.keyword": categoryType
                            }
                        }
                    ]
                }
            },
            "size":0,
            "aggs": aggregation
        }
    elif valueLenght != 0:
        # İlk sayfadaki belgeleri çek
        result = client.search(index=es_index1, size=1)
        # İlk belgeyi al ve içindeki alan isimlerini döndür
        fields = result["hits"]["hits"][0]["_source"].keys() if result["hits"]["hits"] else []
        should_clauses = [{"prefix": {f"{field}.keyword": colValue}} for field in fields]

        aggregation_query = {
            "size": 0,
            "query": {
                "bool": {
                    "must": [
                        {"match": {"category.keyword": categoryType}},
                        {"bool": {"should": should_clauses, "minimum_should_match": 1}}
                    ]
                }
            },
            "aggs": aggregation
        }
    else:
        aggregation_query = {
            "size": 0,
            "query": {
                "bool": {
                    "must":{
                        "match":{"category.keyword": categoryType}
                    }
                }
            },
            "aggs": aggregation
        }

    aggregation_response =  client.search(index=es_index1,body=aggregation_query)
    aggregation_list = aggregation_response["aggregations"]["agg_name"]["buckets"]
    if(len(aggregation_list)==0):
        return {"result":"empty","aggregations":aggregation_list}
    else:
        return {"result":"success","aggregations":aggregation_list}




@app.route("/search/latesthour/<categoryType>")
def search_latest_hour_saddr(categoryType):
    query1={
        "query": {
            "match_all": {}
        },
        "size": 1,
        "sort": [
            {
            "stime.keyword": {
                "order": "desc"
            }
            }
        ]
    }
    
    response =  client.search(index=es_index1,body=query1)
    latest_row = float(response["hits"]["hits"][0]["_source"]["stime"])
    #Epoch Time
    lesserThenValue= str(float(latest_row)).lower()
    greaterThenValue=  str(float(latest_row)-3600).lower()      
    query2={
        "size":0,
        "query":{
            "bool":{
                "must":{
                    "match": {
                        "category.keyword": categoryType
                        }
                },
                "filter":[
                    {"range": {
                        "stime.keyword": {
                            "gte": greaterThenValue,
                            "lte": lesserThenValue
                            }
                        }
                    }
                ],
            },
        },
        "aggs":{
                "agg_name": {
                    "multi_terms":{
                        "terms":[ 
                            {"field": "daddr.keyword"},
                            {"field": "saddr.keyword"},
                        ]
                    }
                }
            }

    }
    response =  client.search(index=es_index1,body=query2)
    aggregation_list = response["aggregations"]["agg_name"]["buckets"]
    if(len(aggregation_list)==0):
        return {"result":"empty","aggregations":aggregation_list}
    else:
        return {"result":"success","aggregations":aggregation_list}


@app.route("/search/lasthour/<categoryType>")
def search_last_hour_saddr(categoryType):
    query = {
        "size":0,
        "query":{
            "bool":{
                "must":{
                    "match": {
                        "category.keyword": categoryType
                        }
                },
                "filter":[
                    {"range": {
                            "@timestamp": {
                                "gte": "now-1h",
                                "lt": "now"
                            }
                        }
                    }
                ],
            },
            
        },
        "aggs":{
                "agg_name": {
                    "multi_terms":{
                        "terms":[ 
                            {"field": "daddr.keyword"},
                            {"field": "saddr.keyword"},
                        ]
                    }
                }
            }
    }
    response =  client.search(index=es_index1,body=query)
    aggregation_list = response["aggregations"]["agg_name"]["buckets"]
    if(len(aggregation_list)==0):
        return {"result":"empty","aggregations":aggregation_list}
    else:
        return {"result":"success","aggregations":aggregation_list}


#Route For Bulk Adding csv file To ElasticSearch
#!!!If You want to use this route to import csv to ElasticCloud you need to add  ".kewyord" to all of the Fields of Queries.
# Example  | previous_field_name | new_field_name
#          |        proto      |  proto.kewyord
@app.route("/create")
def bulk_add():
    with open("data_1.csv", 'r') as x:
        reader = csv.DictReader(x)
        
        try:
            helpers.bulk(client,reader,index=es_index1)
        except:
            {"result":"error"}
      
    return {"result":"successfully added"}



@app.route("/status/update/one",methods = ['POST'])
def status_update_one():
    req = request.json

    SourceAddress = req["saddr"]
    DestinationAddress = req["daddr"]

    res = HpConfig(destination=SourceAddress,source=DestinationAddress)

    document = {
        "saddr": SourceAddress,
        "daddr": DestinationAddress,
        "status": res["status"]    
        }
    doc_id = f"{SourceAddress}_{DestinationAddress}"

    doc_exists = client.exists(index=es_index2, id=doc_id)

    if doc_exists:
        # if doc exists update
        try:
            client.update(index=es_index2, id=doc_id, body={"doc": document})
        except:
            return {"result":"failed"} 
         
    else:
        # if doc doesn't exists create
        try:
            client.index(index=es_index2, id=doc_id, body=document)
        except:
            return {"result":"failed"} 
    
    return {"result":"success"} 



@app.route("/status/update/list",methods = ['POST'])
def status_update_list():
    list = request.json["list"]
    for pair in list[:5]:
        #Send each of first 5 row to HpConfig and wait for response
        res = HpConfig(destination=pair["key"][0],source=pair["key"][1])
        #Work with Response
        document = {
            "saddr": pair["key"][1],
            "daddr": pair["key"][0],
            "status": res["status"]
        }
        doc_id = f"{pair["key"][1]}_{pair["key"][0]}"

        doc_exists = client.exists(index=es_index2, id=doc_id)

        if doc_exists:
            # if doc exists update
            try:
                client.update(index=es_index2, id=doc_id, body={"doc": document})
            except:
                return {"result":"failed"} 
            
        else:
            # if doc doesn't exists create
            try:
                client.index(index=es_index2, id=doc_id, body=document)
            except:
                return {"result":"failed"} 

    return {"result":"success"} 


@app.route("/status/get/page",methods = ['POST'])
def get_address_status():
    req = request.json

    SourceAddress = req["saddr"]
    DestinationAddress = req["daddr"]
    Status = req["status"]
    pageNumber=int(req["pageNumber"])

    skipValues=pageNumber*10-10
    should_clauses = []
    if len(SourceAddress)!=0:
        should_clauses.append({"prefix": {"saddr.keyword": SourceAddress}})
    if len(DestinationAddress)!=0:
        should_clauses.append({"prefix": {"daddr.keyword": DestinationAddress}})
    if len(Status)!=0:
        should_clauses.append({"prefix": {"status.keyword": Status}})


    if(len(SourceAddress)==0 and len(DestinationAddress)==0 and len(Status)==0 ):
        should_clauses.append({"match_all": {}})

    query = {
        "size": 10,
        "from":skipValues,
        "query":{
            "bool":{
                "must":[
                    {"bool": {
                        "should": should_clauses,
                        "minimum_should_match": len(should_clauses)}
                    }
                ]
            }
        },
        "sort": [
            {"saddr.keyword": "desc"},
        ]
    }
    response = client.search(index=es_index2, body=query)
    df = pd.DataFrame([hit["_source"] for hit in response["hits"]["hits"] ])
    if(df.empty):
        return {"result":"empty",}
    else:
        list = df.to_dict('records')
        return {"result":"success","data":list}

@app.route("/status/get/count",methods = ['POST'])
def get_status_count():
    req = request.json

    SourceAddress = req["saddr"]
    DestinationAddress = req["daddr"]
    Status = req["status"]

    should_clauses = []
    must_clauses = []
    if len(SourceAddress)!=0:
        should_clauses.append({"prefix": {"saddr.keyword": SourceAddress}})
    if len(DestinationAddress)!=0:
        should_clauses.append({"prefix": {"daddr.keyword": DestinationAddress}})
    if len(Status)!=0:
        should_clauses.append({"prefix": {"status.keyword": Status}})

    if(len(SourceAddress)==0 and len(DestinationAddress)==0 and len(Status)==0 ):
        should_clauses.append({"match_all": {}})

    query = {
        "query":{
            "bool":{
                "must":[
                    {"bool": {
                        "should": should_clauses,
                        "minimum_should_match": len(should_clauses)}
                    }
                ]
            }
        }
    }
    response = client.count(index=es_index2, body=query)
    return {"result":"success","count":response["count"]}

#For Server Shutdown
@app.get('/shutdown')
def shutdown():
    print("Shutting down server...")
    os._exit(0)
