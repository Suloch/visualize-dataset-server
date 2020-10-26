from sqlalchemy import text, Integer, String, Column
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()

class Dataset(db.Model):
    __tablename__ = 'dataset'

    id = Column(Integer, primary_key=True)
    name = Column(String)

