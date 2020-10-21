from flask import Flask, request
import pandas as pd
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text
from flask_migrate import Migrate

db = SQLAlchemy()

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://sumit:sumit@localhost:5432/datasets"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)
migrate = Migrate(app, db)


@app.route('/upload', methods=['POST'])
def upload_csv():
    '''
        view to upload a csv and write to a new table with the given table name
        if the table with that already exists, it return error
    '''

    # read the file as a pandas dataframe
    data = pd.read_csv(request.files['dataset'])
    
    # write the dataframe to database using the given name as table name
    try:
        data.to_sql(request.values['name'], con=db.engine)
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
def visualization(name):
    '''
        view to see the dataset details : column names and data types
    '''
    # sql to get the column name and data type
    sql_text = text("select column_name, data_type from information_schema.columns where table_name = :table_name")
    column_names = db.engine.execute(sql_text, table_name=name).fetchall()
    
    # create json serializable data from result
    result = []
    for data in column_names:
        result.append([data[0], data[1]])

    return {
        'ack': True,
        'details': 'Success',
        'data': result
    }

@app.route('/dataset', methods=['GET'])
def list_datasets():
    '''
        list all the datasets that have been uploaded
    '''
    return {
        'ack': True,
        'details': 'Success',
        'data': db.engine.table_names()
    }

app.run(debug=True, port=8000)