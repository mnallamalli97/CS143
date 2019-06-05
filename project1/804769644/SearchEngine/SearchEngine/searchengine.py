#!/usr/bin/python3

from flask import Flask, render_template, request

import search

application = app = Flask(__name__)
app.debug = True

@app.route('/search', methods=["GET"])
def dosearch():
    query = request.args['query']
    qtype = request.args['query_type']
    page_number = int(request.args['page_num'])
    if page_number != 0:
        button_dir = request.args['button_dir']
        if button_dir == 'Previous':
            page_number -= 1
        else:
            page_number += 1
    else:
        page_number+= 1



    """
    TODO:
    Use request.args to extract other information
    you may need for pagination.
    """

    search_results, real_num = search.search(query, qtype, int(page_number))
    return render_template('results.html',
            query=query,
            results=len(search_results),
            search_results=search_results,
            page_num = page_number, 
            qtype = qtype,
            total_results = real_num)

@app.route("/", methods=["GET"])
def index():
    if request.method == "GET":
        pass
    return render_template('index.html')

if __name__ == "__main__":
    app.run(host='0.0.0.0')
