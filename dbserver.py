from flask import Flask, request
import json
from lib.query import Query
from lib.dbengine import DBEngine
from tqdm import tqdm
import sys
import os
app = Flask(__name__)

DATABASE_PATH = sys.argv[1]

@app.route("/query")
def hello():
    pred_json = json.loads(request.args.get('pred'))
    gold_json = json.loads(request.args.get('gold'))
    dataset = request.args.get('dataset')

    engine = DBEngine(os.path.join(DATABASE_PATH, "{}.db".format(dataset)))

    exact_match = []
    grades = []

    for lp, ls in tqdm(zip(pred_json, gold_json), total=len(gold_json)):
        eg = ls
        ep = lp
        qg = Query.from_dict(eg['sql'])
        gold = engine.execute_query(eg['table_id'], qg, lower=True)
        pred = ep['error']
        qp = None
        if not ep['error']:
            try:
                qp = Query.from_dict(ep['query'])
                pred = engine.execute_query(eg['table_id'], qp, lower=True)
            except Exception as e:
                pred = repr(e)
        correct = pred == gold
        match = qp == qg
        grades.append(correct)
        exact_match.append(match)

    ex_accuracy = sum(grades) / len(grades)
    lf_accuracy = sum(exact_match) / len(exact_match)
    return json.dumps({"ex_accuracy": ex_accuracy, "lf_accuracy": lf_accuracy})



if __name__ == '__main__':
    app.run(debug=True, port=8080)