
#dag - directed acyclic graph

#tasks : 1) fetch amazon data (extract) 2) clean data (transform) 3) create and store data in table on postgres (load)
#operators : Python Operator and PostgresOperator
#hooks - allows connection to postgres
#dependencies

from datetime import datetime, timedelta
from airflow import DAG
import requests
import pandas as pd
from bs4 import BeautifulSoup
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import random

#1) fetch amazon data (extract) 2) clean data (transform)


def get_amazon_data_books(num_books, ti):

    # Limit to the requested number of books
    books = [{"Title":"Fundamentals of Data Engineering","Author":"Joe Reis, Matt Housley","Price":45,"Rating":4.7},{"Title":"The Data Warehouse Toolkit","Author":"Ralph Kimball, Margy Ross","Price":55,"Rating":4.6},{"Title":"Designing Data-Intensive Applications","Author":"Martin Kleppmann","Price":50,"Rating":4.8},{"Title":"Data Pipelines Pocket Reference","Author":"James Densmore","Price":30,"Rating":4.6},{"Title":"Data Engineering with Python","Author":"Paul Crickard","Price":40,"Rating":4.5},{"Title":"The Big Data Handbook","Author":"Arvind Sathi","Price":38,"Rating":4.3},{"Title":"Data Engineering on Azure","Author":"Vlad Riscutia","Price":42,"Rating":4.5},{"Title":"Streaming Systems","Author":"Tyler Akidau, Slava Chernyak","Price":48,"Rating":4.6},{"Title":"Data Management for Data Science","Author":"Krishna Sankar","Price":35,"Rating":4.4},{"Title":"Snowflake: The Definitive Guide","Author":"Marcin Żukowski","Price":50,"Rating":4.7},{"Title":"The Practitioner's Guide to Graph Data","Author":"Denise Koessler Gosnell","Price":45,"Rating":4.5},{"Title":"Hadoop: The Definitive Guide","Author":"Tom White","Price":48,"Rating":4.3},{"Title":"Data Engineering with Apache Spark","Author":"Gerard Maas","Price":46,"Rating":4.5},{"Title":"Google BigQuery: The Definitive Guide","Author":"Valliappa Lakshmanan","Price":42,"Rating":4.7},{"Title":"Data Science on AWS","Author":"Chris Fregly, Antje Barth","Price":52,"Rating":4.6},{"Title":"The Modern Data Warehouse in Azure","Author":"Matt How","Price":44,"Rating":4.5},{"Title":"Cloud Data Management","Author":"Danil Zburivsky","Price":39,"Rating":4.4},{"Title":"SQL for Data Scientists","Author":"Renee M. P. Teate","Price":35,"Rating":4.4},{"Title":"Mastering Snowflake Solutions","Author":"Tayo Koleoso","Price":38,"Rating":4.3},{"Title":"DataOps: The Missing Piece","Author":"John Michaloudis","Price":40,"Rating":4.5}]
    
    # Convert the list of dictionaries into a DataFrame
    df = pd.DataFrame(books)
    
    # Remove duplicates based on 'Title' column
    df.drop_duplicates(subset="Title", inplace=True)
    
    # Push the DataFrame to XCom
    ti.xcom_push(key='book_data', value=df.to_dict('records'))

#3) create and store data in table on postgres (load)
    
def insert_book_data_into_postgres(ti):
    book_data = ti.xcom_pull(key='book_data', task_ids='fetch_book_data')
    if not book_data:
        raise ValueError("No book data found")

    postgres_hook = PostgresHook(postgres_conn_id='books_connection')
    insert_query = """
    INSERT INTO books (title, authors, price, rating)
    VALUES (%s, %s, %s, %s)
    """
    for book in book_data:
        postgres_hook.run(insert_query, parameters=(book['Title'], book['Author'], book['Price'], book['Rating']))


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 23),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fetch_and_store_amazon_books',
    default_args=default_args,
    description='A simple DAG to fetch book data from Amazon and store it in Postgres',
    schedule_interval=timedelta(days=1),
)

#operators : Python Operator and PostgresOperator
#hooks - allows connection to postgres


fetch_book_data_task = PythonOperator(
    task_id='fetch_book_data',
    python_callable=get_amazon_data_books,
    op_args=[50],  # Number of books to fetch
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='books_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS books (
        id SERIAL PRIMARY KEY,
        title TEXT NOT NULL,
        authors TEXT,
        price TEXT,
        rating TEXT
    );
    """,
    dag=dag,
)

insert_book_data_task = PythonOperator(
    task_id='insert_book_data',
    python_callable=insert_book_data_into_postgres,
    dag=dag,
)

#dependencies

fetch_book_data_task >> create_table_task >> insert_book_data_task

