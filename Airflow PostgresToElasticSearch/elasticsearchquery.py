from elasticsearch import Elasticsearch

es = Elasticsearch() 

doc={"query":{"match":{"city":"North Ronald"}}}
res=es.search(index="frompostgres",body=doc,size=10)
print(res['hits']['hits'])