from pyelasticsearch from ElasticSearch

es = ElasticSearch('http://localhost:9200/')

query = {
    'query': {
        'filtered': {
            'filter': {
                'range': {
                    'date': {
                        'gte': '2015-01-01',
                        'lte': 'now',
                        'time_zone': '+5:00'
                    }
                }
            }
        },
    },
}

print es.search(query, index='cssa')["hits"]["total"]
