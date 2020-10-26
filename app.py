from flask import Flask, request, Response
import pandas as pd
from sqlalchemy import text
from flask_migrate import Migrate
from flask_cors import CORS
from werkzeug.exceptions import BadRequestKeyError
import json
from models import Dataset, db



app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://sumit:sumit@db:5432/datasets"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)
db.create_all(app=app)

CORS(app)



@app.route('/upload', methods=['POST'])
def upload_csv():
    '''
        view to upload a csv and write to a new table with the given table name
        if the table with that already exists, it return error
    '''

    # read the file as a pandas dataframe
    try:
        data = pd.read_csv(request.files['dataset'])
    except BadRequestKeyError:
        return{
            'ack': False,
            'details': 'dataset not found in request'
        }
    
    # write the dataframe to database using the given name as table name
    try:
        data.to_sql(request.values['name'], con=db.engine)
        db.session.add(Dataset(name=request.values['name']))
        db.session.commit()
    except ValueError:
        # error when the name is duplicate
        return {
            'ack': False,
            'details': 'Table with that name already exists. Please choose a different name.'
        }

    return {
        'ack': True,
        'details': 'Dataset saved to db'
    }

@app.route('/dataset/<string:name>', methods=['GET'])
def detail_dataset(name):
    '''
        view to see the dataset details : column names and data types
    '''
    # sql to get the column name and data type
    sql_text = text("select column_name from information_schema.columns where table_name = :table_name")
    column_names = db.engine.execute(sql_text, table_name=name).fetchall()
    
    # create json serializable data from result
    result = {
        'columns': [name[0] for name in column_names],
        'functions': ['sum', 'count', 'min',  'max', 'none']
    }

    return Response(json.dumps({
        'ack': True,
        'details': 'Success',
        'data': result
    }), status=200, content_type='application/json')

@app.route('/dataset', methods=['GET'])
def list_datasets():
    '''
        list all the datasets that have been uploaded
    '''
    result = []
    i = 0
    temp = []
    for dataset in Dataset.query.all():
        temp.append(dataset.name)
        i = i + 1
        if i == 3:
            result.append(temp)
            temp = list()
            i = 0

    
    
    return Response(json.dumps({
        'ack': True,
        'details': 'Success',
        'data': result
    }), status='200', content_type='application/json')

@app.route('/points/<string:name>', methods=['POST'])
def visualize(name):
    '''
        generate all the x, y and f according to user defined value
    '''
    aggregate_logic = {
        'sum' : lambda x, y: 'select "' + x + '", sum ("' + y + '") from "' + name + '" group by "' + x + '";',
        'none' : lambda x, y: 'select "' + x + '", "' + y + '" from "' + name + '";',
    }
    try:
        result = db.engine.execute(aggregate_logic[request.json['f']](request.json['x'], request.json['y'])).fetchall()
    except BadRequestKeyError:
        return Response(json.dumps({
            'ack': False,
            'details': 'bad request required parameters not found'
        }), status='400', content_type='application/json')
    except KeyError:
        return Response(json.dumps({
            'ack': False,
            'details': 'aggregate function not found'
        }), status='400', content_type='application/json')

    data = {'x': [], 'y': []}

    for entry in result:
        data['x'].append(entry[0])
        data['y'].append(entry[1])

    return Response(json.dumps({
        'ack': True,
        'details': 'Success',
        'data': data
    }), status='200', content_type='application/json')

app.run(host='0.0.0.0', debug=True, port=8000)
