from dotenv import load_dotenv
from flask import Flask,request
from elasticsearch import Elasticsearch,helpers
import pandas as pd
import os 
import csv


load_dotenv()
CLOUD_ID = os.getenv("Cloud_ID")
ELASTIC_PASSWORD = os.getenv("Elastic_Password")
es_index="datas"

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
@app.route("/search/count/<column>/<value>")
def search_count(column,value):
    size_query = {
        "query": {
            "bool" : {
                "must":{
                    "query_string": {
                        "escape": True,
                        "fields": [f"{column}.keyword"],
                        "query": value
                    }
                }
            }
        }
    }
    size_response = client.count(index=es_index,body=size_query)
    size = size_response["count"]
    return {"result":"success","count":size_response["count"]}

#******************** SEARCH PAGE ********************
#Route For Paginating the Query Results/ Max rows to Paginate is 10000
@app.route("/search/page/<column>/<value>/<pageNumber>")
def search_page(column,value,pageNumber):
    pageNumber=int(pageNumber)
    skipValues=pageNumber*10-10
    search_query = {
        "size": 10,
        "from":skipValues,
        "query": {
            "term" : {
                f"{column}.keyword" : value
            }
        },
        "sort": [
            {"pkSeqID.keyword": "desc"},
        ]
    }
    search_response = client.search(index=es_index,body=search_query)
    df = pd.DataFrame([hit["_source"] for hit in search_response["hits"]["hits"] ])
    if(df.empty):
        return {"result":"empty",}
    else:
        list = df.to_dict('records')
        return {"result":"success","data":list}

#******************** SEARCH AGGREGATION QUERY ********************
#Route For Getting The Aggreation Counts From the Query Result 
@app.route("/search/aggregations/<column>/<value>/<sort>")
def search_aggregation_query(column,value,sort):
    
    aggregation_query = {
        "query": {
            "bool" : {
                "must":{
                    "query_string": {
                        "escape": True,
                        "fields": [f"{column}.keyword"],
                        "query": value
                    }
                }
            }
        },
        "size":0,
        "aggs": {
            "agg_name": {
                "terms": {
                    "field": f"{sort}.keyword"
                }
            }
        },
    }
    aggregation_response =  client.search(index=es_index,body=aggregation_query)
    aggregation_list = aggregation_response["aggregations"]["agg_name"]["buckets"]
    if(len(aggregation_list)==0):
        return {"result":"empty","aggregations":aggregation_list}
    else:
        return {"result":"success","aggregations":aggregation_list}


#******************** TOTAL SOURCE ADDRESS AGGREGATION  ********************
#Route For Getting The Top 5 Ip Adressess from Given Category Type 
@app.route("/search/aggregations/<categoryType>/")
def total_saddr_aggregation(categoryType):
    aggregation_query = {
        "size":0,
        "query": {
            "bool" : {
                "must":[
                    {
                    "match": {
                        "category.keyword": categoryType
                        }
                    }
                ]
            }
        },
        "aggs": {
            "agg_name": {
                "terms": {
                    "field": "saddr.keyword"
                }
            }
        },
    }
    aggregation_response =  client.search(index=es_index,body=aggregation_query)
    aggregation_list = aggregation_response["aggregations"]["agg_name"]["buckets"]
    if(len(aggregation_list)==0):
        return {"result":"empty","aggregations":aggregation_list}
    else:
        return {"result":"success","aggregations":aggregation_list[:5]}


#******************** SOURCE ADDRESS AGGREGATION FROM QUERY ********************
#Route For Getting The Ip Adressess From Query Result with Given Category Type 
@app.route("/report/get/aggregations/<column>/<colValue>/<categoryType>")
def get_saddr_from_query(column,colValue,categoryType):
    
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
        "aggs": {
            "agg_name": {
                "terms": {
                    "field": "saddr.keyword"
                }
            }
        },
    }
    aggregation_response =  client.search(index=es_index,body=aggregation_query)
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
    
    response =  client.search(index=es_index,body=query1)
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
                "agg_name1":{
                    "terms":{
                        "field":"saddr.keyword"
                    }
                }
            }

    }
    response =  client.search(index=es_index,body=query2)
    aggregation_list = response["aggregations"]["agg_name1"]["buckets"]
    if(len(aggregation_list)==0):
        return {"result":"empty","aggregations":aggregation_list}
    else:
        return {"result":"success","aggregations":aggregation_list}


@app.route("/search/lasthour/<categoryType>")
def search_last_hour_saddr(categoryType):
    query = {
       "query": {
            "range": {
                "@timestamp": {
                    "gte": "now-1h",
                    "lt": "now"
                }
            }
        },
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
                "agg_name1":{
                    "terms":{
                        "field":"saddr.keyword"
                    }
                }
            }
    }
    response =  client.search(index=es_index,body=query)
    aggregation_list = response["aggregations"]["agg_name1"]["buckets"]
    if(len(aggregation_list)==0):
        return {"result":"empty","aggregations":aggregation_list}
    else:
        return {"result":"success","aggregations":aggregation_list}



#Route For Getting The Top 5 Aggregation From Frontend and Getting Them Ready for The ML Script
@app.route("/report/post/aggregations/<aggregationList>")
def get_saddr_from_front(aggregationList):
    return f"<p>{aggregationList}</p>"



#Route For Bulk Adding csv file To ElasticSearch
@app.route("/create")
def bulk_add():
    with open("data_1.csv", 'r') as x:
        reader = csv.DictReader(x)
        
        try:
            helpers.bulk(client,reader,index="datas")
        except:
            print("error")
    return "<p>Bulk Add To Elastic </p>"
