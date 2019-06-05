#!/usr/bin/python3

import psycopg2
import re
import string
import sys

_PUNCTUATION = frozenset(string.punctuation)

def _remove_punc(token):
    """Removes punctuation from start/end of token."""
    i = 0
    j = len(token) - 1
    idone = False
    jdone = False
    while i <= j and not (idone and jdone):
        if token[i] in _PUNCTUATION and not idone:
            i += 1
        else:
            idone = True
        if token[j] in _PUNCTUATION and not jdone:
            j -= 1
        else:
            jdone = True
    return "" if i > j else token[i:(j+1)]

def _get_tokens(query):
    rewritten_query = []
    tokens = re.split('[ \n\r]+', query)
    for token in tokens:
        cleaned_token = _remove_punc(token)
        if cleaned_token:
            if "'" in cleaned_token:
                cleaned_token = cleaned_token.replace("'", "''")
            rewritten_query.append(cleaned_token)
    return rewritten_query



def search(query, query_type, page_num):
    
    rewritten_query = _get_tokens(query)

    """TODO
    Your code will go here. Refer to the specification for projects 1A and 1B.
    But your code should do the following:
    1. Connect to the Postgres database.
    2. Graciously handle any errors that may occur (look into try/except/finally).
    3. Close any database connections when you're done.
    4. Write queries so that they are not vulnerable to SQL injections.
    5. The parameters passed to the search function may need to be changed for 1B. 
    """

    rows = [] 
    try:
        #1. 
        #reference used: http://www.postgresqltutorial.com/postgresql-python/connect/

        #need to connect to searchengine database
        keywords = query.split()
        keywords_count = len(keywords)
        connection_object = psycopg2.connect("dbname=searchengine user=cs143")

        # create a cursor
        cursor = connection_object.cursor()
        input_statement = "where "
        for i in range(len(keywords) - 1):
            input_statement += "token = '" + keywords[i] + "' or "
        input_statement += "token = '" + keywords[len(keywords) -1] + "'"
        query_builder = ""

        #testing connection
        print('postgres database interpretation: ')

        drop_view = "drop materialized view if exists results_view;"
        cursor.execute(drop_view)
        if(query_type == 'or'):
            query_builder = "create materialized view results_view as select sname, A.aname, url from project1.tfidf L inner join project1.song R on L.sid = R.sid inner join project1.artist A on R.aid = A.aid where token = ANY(%s) group by L.sid, sname, A.aid, url order by sum(tfidf) desc;"
            cursor.execute(query_builder, (keywords,))

        if(query_type == 'and'):
            query_builder = "create materialized view results_view as select sname, A.aname, url from project1.tfidf L inner join project1.song R on L.sid = R.sid inner join project1.artist A on R.aid = A.aid where token= ANY(%s) group by L.sid, sname, A.aid, url having count( distinct token) = %s order by sum(tfidf) desc;"
            cursor.execute(query_builder, (keywords, len(keywords),))

        # cursor.execute(query_builder)
        
        
        view_results = "select * from results_view limit 20 offset {};".format(20*(page_num-1))
        cursor.execute(view_results)
        #display the query from above

        rows = cursor.fetchall()

        results_q = "SELECT count(*) from results_view;"
        cursor.execute(results_q)

        total_results = cursor.fetchall()

        real_num = total_results[0][0]

        print(real_num)
        # ans = cursor.fetchall()

        # rows = cursor.fetchall()
        
        for result in rows:
            aname, aid, link = result
            print("{}, {}, {}".format(aname, aid, link))

        #close the cursor
        cursor.close()

        

    except psycopg2.OperationalError as error:
        print(error)
    finally:
        if connection_object is not None:
            connection_object.close()
            print('db closed')
        

    return rows, real_num

if __name__ == "__main__":
    if len(sys.argv) > 2:
        result = search(' '.join(sys.argv[2:]), sys.argv[1].lower())
        print(result)
    else:
        print("USAGE: python3 search.py [or|and] term1 term2 ...")



