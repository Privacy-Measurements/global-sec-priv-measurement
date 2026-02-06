import os
from dotenv import load_dotenv
from pymysql.cursors import DictCursor


load_dotenv()

db_params = dict(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    db=os.getenv("DB_NAME"),
    cursorclass=DictCursor

)