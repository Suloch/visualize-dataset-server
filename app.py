from flask import Flask, request
import pandas as pd
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate

db = SQLAlchemy()

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://sumit:sumit@localhost:5432/datasets"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)
migrate = Migrate(app, db)

@app.route('/upload', methods=['POST'])
def upload_csv():
    data = pd.read_csv(request.files['dataset'])
    
    try:
        data.to_sql(request.values['name'], con=db.engine)
    except ValueError:
        return {
            'ack': False,
            'details': 'Table with that name already exists. Please choose a different name.'
        }

    return {
        'ack': False,
        'details': 'Not implemented'
    }

@app.route()
app.run(debug=True, port=8000)