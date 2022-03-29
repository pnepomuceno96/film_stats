#!/usr/bin/env python3
import os
import csv
import psycopg2
from psycopg2 import Error

try:
    conn = psycopg2.connect(user = "francisc.nepomuceno",
                        password="*",
                        host="192.168.1.225",
                        port="5432",
                        database="francisc.nepomuceno"
                        )

    cur = conn.cursor()
    # Create table and header
    create_table_query = '''drop table if exists public.films; CREATE TABLE public.films
        (
        movie_title varchar not null,
        director varchar,
        year int,
        rating varchar,
        rewatch varchar,
        tags varchar,
        date_watched varchar
        ); '''
    cur.execute(create_table_query)
    conn.commit()
    print("Table created successfully in PostgreSQL")
    with open('diary.csv', 'r', encoding = 'iso-8859-1') as file:
        reader = csv.DictReader(file)
        # Build contents of table from csv file
        for film in reader:
            if "'" in film['Name']:
                film['Name'] = film['Name'].replace("'", "''")
            if "'" in film['Director']:
                film['Director'] = film['Director'].replace("'", "''")
            insert_query = ''' INSERT INTO films (movie_title, director, year, rating, rewatch, tags, date_watched) VALUES ('{}', '{}', {}, '{}', '{}', '{}', '{}')'''.format(film['Name'], film['Director'], film['Year'], film['Rating'], film['Rewatch'], film['Tags'], film['Watched Date'])
            #print(insert_query)
            cur.execute(insert_query)
            conn.commit()
        print("Film data inserted successfully")
    # Result
    cur.execute("SELECT * from films")
    print("Result ", cur.fetchall())
except(Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    if conn:
        cur.close()
        conn.close()
        print("PostgreSQL connection is closed")
