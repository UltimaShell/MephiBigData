import random
import psycopg2
import datetime
from faker import Faker
from random import randrange

TYPE_ENTITY_COUNT: int = 4
MAIN_ENTITY_COUNT = 10
AUX_ENTITY_COUNT = 25
NULL = 'NULL'

# Create russian faker
faker = Faker('ru_RU')


def add_brackets(s):
    return "'" + str(s) + "'" if s != NULL else s


def create_row(row):
    '''
    Function to return row str from tuple
    '''
    return '(' + ','.join(add_brackets(e) for e in row) + ')'


# Create DB connection
connection = psycopg2.connect("dbname=testdb user=postgres password=1234")
cursor = connection.cursor()

query = "CREATE TABLE Journal (university_id INTEGER, person_id	INTEGER, check_time	TIMESTAMP, type	BOOLEAN); CREATE TABLE Publication(university_id INTEGER, authors INTEGER, pub_name VARCHAR(255), pub_date DATE)"
cursor.execute(query)
connection.commit()

# Journal
for _ in range(5000):
    query = "INSERT INTO Journal (university_id, person_id, check_time, type) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING"
    univer=random.randint(1,25)
    piople=random.randint(1, 100)
    startdate=faker.date_time()
    enddate=startdate+datetime.timedelta(hours=randrange(5))
    cursor.execute(query, (univer, piople, startdate, 'true'))
    cursor.execute(query, (univer, piople, enddate, 'false'))
connection.commit()
# Journal
#
# Publication
cursor.execute("SELECT person_id FROM Journal")
Persons = [c[0] for c in cursor.fetchall()]

cursor.execute("SELECT university_id FROM Journal")
Univers = [d[0] for d in cursor.fetchall()]

query = "INSERT INTO Publication (university_id, authors, pub_name, pub_date) VALUES "
Publication = list()
for _ in range(5000):
    Publication.append(create_row((random.choice(Univers), random.choice(Persons), faker.text(max_nb_chars=50), faker.date())))
query_data = ','.join(Publication)
cursor.execute(query + query_data + "ON CONFLICT DO NOTHING")
connection.commit()
# Publication

# Commit changes to DB
connection.commit()


# connection.commit()
print('DB has been successfully created and seeded')
# Close DB connection
cursor.close()
connection.close()
