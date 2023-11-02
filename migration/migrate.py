import psycopg2
import csv

# Configuraciones

db_host=""
db_name=""
db_user=""
db_password=""
db_port=""
db_schema=""

# Migration

conn = psycopg2.connect(host=db_host,
    database=db_name,
    user=db_user,
    password=db_password, port=db_port)

cursor = conn.cursor()

def insertBook(b_isbn, b_titulo, b_anho, e_id):
    cur.execute("SELECT isbn FROM %s.libro WHERE isbn = %s LIMIT 1", [db_schema, b_isbn])

    r = cur.fetchone()

    if not (r):
        cur.execute("INSERT INTO %s.libro (isbn,titulo,anho,editorial_id) VALUES (%s, %s, %s, %s,%s) RETURNING isbn", [db_schema, b_isbn, b_titulo, b_anho, e_id])

def findBook(b_isbn):
    cur.execute("SELECT isbn FROM %s.libro WHERE isbn = %s LIMIT 1", [db_schema, b_isbn])

    r = cur.fetchone()

    if (r):
        return True
    else:
        return False

def findOrInsertByName(tb_name, nombre):
    cur.execute("SELECT id FROM %s.%s WHERE nombre ILIKE %s LIMIT 1", [db_schema, tb_name, nombre])

    r = cur.fetchone()

    if (r):
        return r[0]
    else:
        cur.execute("INSERT INTO %s.%s (nombre) VALUES (%s) RETURNING id", [db_schema, tb_name, nombre])

        return cur.fetchone()[0]

def insertWritten(a_id, b_isbn):
    cur.execute("SELECT * FROM %s.escribe WHERE a_id = %s AND l_isbn = %s", [db_schema, a_id, b_isbn])

    r = cur.fetchone()

    if not (r):
        cur.execute("INSERT INTO %s.escribe (a_id, l_isbn) VALUES (%s, %s)", [db_schema, a_id, b_isbn])

def insertUser(u_id, u_location, u_age):
    cur.execute("SELECT isbn FROM %s.usuario WHERE id = %s LIMIT 1", [db_schema, u_id])

    r = cur.fetchone()

    if not (r):
        cur.execute("INSERT INTO %s.usuario (id, residencia, edad) VALUES (%s, %s, %s) RETURNING isbn", [db_schema, u_id, u_location, u_age])

def findUser(u_id, u_location, u_age):
    cur.execute("SELECT isbn FROM %s.usuario WHERE id = %s LIMIT 1", [db_schema, u_id])

    r = cur.fetchone()

    if (r):
        return True
    else:
        return False

def insertReview(b_isbn, u_id, rating):
    cur.execute("SELECT isbn FROM %s.review WHERE b_isbn = %s AND u_id = %s LIMIT 1", [db_schema, b_isbn, u_id])

    r = cur.fetchone()

    if not (r):
        cur.execute("INSERT INTO %s.review (book_isbn, user_id, rating) VALUES (%s, %s, %s) RETURNING isbn", [db_schema, b_isbn, u_id, rating])

with open("./bookreviews/BX_Books.csv") as csv_books:

    reader = csv.reader(csv_books, delimiter=";",quotechar='"')

    i=0

    for row in reader:

        i += 1

        if(i==1):
            continue

        book_isbn = row[0]

        book_title = row[1]

        book_year = row[3]

        if (publisher_name != ""):
            e_id = findOrInsertByName("Editorial", publisher_name)
            if (book_isbn != "" and book_title != "" and book_year != ""):
                insertBook(book_isbn, book_title, int(book_year), e_id)

        author_name = row[2]

        if (author_name != ""):
            a_id = findOrInsertByName("autor", author_name)
            if (findBook(book_isbn)):
                insertWritten(a_id, book_isbn)

        publisher_name = row[4]


with open("./bookreviews/BX_Users.csv") as csv_books:

    reader = csv.reader(csv_books, delimiter=";",quotechar='"')

    i=0

    for row in reader:

        i += 1

        if(i==1):
            continue

        u_id = row[0]

        u_location = row[1]

        u_age = row[2]

        if (u_id != "" and u_location != "" and u_age != ""):
            insertUser(int(u_id), u_location, int(u_age))

with open("./bookreviews/BX_Book_Ratings.csv") as csv_books:

    reader = csv.reader(csv_books, delimiter=";",quotechar='"')

    i=0

    for row in reader:

        i += 1

        if(i==1):
            continue

        u_id = row[0]

        b_isbn = row[1]

        rating = row[2]

        if (u_id != "" and b_isbn = "" and rating != ""):
            if (findUser(int(u_id)) and findBook(b_isbn)):
                insertReview(b_isbn, int(u_id), int(rating))
