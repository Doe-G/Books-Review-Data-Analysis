import psycopg2
import csv

# Configuraciones

db_host="localhost"
db_name="libros"
db_user="su"
db_password="quieroTremendo7<3"
db_port="5432"
db_schema="data"

# Migration

conn = psycopg2.connect(host=db_host,
    database=db_name,
    user=db_user,
    password=db_password,
    port=db_port)

cur = conn.cursor()

# Insert books on the libro table
def insertLibro(b_isbn, b_titulo, b_anho, e_id):
    cur.execute("SELECT isbn FROM " + db_schema + ".libro WHERE isbn = %s LIMIT 1", [b_isbn])

    r = cur.fetchone()

    if not (r):
        cur.execute("INSERT INTO " + db_schema + ".libro (isbn,editorial_id,titulo,anho) VALUES (%s, %s, %s, %s)", [b_isbn, e_id, b_titulo, b_anho])

# Review if a book by isbn exists on the libro table
def findLibro(b_isbn):
    cur.execute("SELECT isbn FROM " + db_schema + ".libro WHERE isbn = %s LIMIT 1", [b_isbn])

    r = cur.fetchone()

    if (r):
        return True
    else:
        return False

# Review if a name exists on a table if not exists it insert the element(It works for autor, editorial and genero tables)
def findOrInsertByNombre(tb_name, nombre):
    cur.execute("SELECT id FROM " + db_schema + "." + tb_name + " WHERE nombre ILIKE %s LIMIT 1", [nombre])

    r = cur.fetchone()

    if (r):
        return r[0]
    else:
        cur.execute("INSERT INTO " + db_schema + "." + tb_name + " (nombre) VALUES (%s) RETURNING id", [nombre])

        return cur.fetchone()[0]

# Insert a element on autor_libro
def insertAutorLibro(a_id, b_isbn):
    cur.execute("SELECT * FROM " + db_schema + ".autor_libro WHERE autor_id = %s AND libro_isbn ILIKE %s", [a_id, b_isbn])

    r = cur.fetchone()

    if not (r):
        cur.execute("INSERT INTO " + db_schema + ".autor_libro (autor_id, libro_isbn) VALUES (%s, %s)", [a_id, b_isbn])

# Insert a element on User
def insertUsuario(u_id, u_location, u_age):
    cur.execute("SELECT id FROM " + db_schema + ".usuario WHERE id = %s LIMIT 1", [u_id])

    r = cur.fetchone()

    if not (r):
        cur.execute("INSERT INTO " + db_schema + ".usuario (id, residencia, edad) VALUES (%s, %s, %s)", [u_id, u_location, u_age])

# Checks if a user exists
def findUsuario(u_id):
    cur.execute("SELECT id FROM " + db_schema + ".usuario WHERE id = %s LIMIT 1", [u_id])

    r = cur.fetchone()

    if (r):
        return True
    else:
        return False

# It inserts a element on the review template
def insertLibroUsuario(b_isbn, u_id, rating):
    cur.execute("SELECT * FROM " + db_schema + ".libro_usuario WHERE libro_isbn ILIKE %s AND usuario_id = %s LIMIT 1", [b_isbn, u_id])

    r = cur.fetchone()

    if not (r):
        cur.execute("INSERT INTO " + db_schema + ".libro_usuario (usuario_id, libro_isbn, rating) VALUES (%s, %s, %s)", [u_id, b_isbn, rating])

# Insert a element on book and genre
def insertGeneroLibro(a_id, b_isbn):
    cur.execute("SELECT * FROM " + db_schema + ".genero_libro WHERE genero_id = %s AND libro_isbn = %s", [a_id, b_isbn])

    r = cur.fetchone()

    if not (r):
        cur.execute("INSERT INTO " + db_schema + ".genero_libro (genero_id, libro_isbn) VALUES (%s, %s)", [a_id, b_isbn])

# Limpia la string
def stringFilter(string):

    return string.strip().replace('"', '').replace("'", "").replace("\\","").replace(";","")

with open("./datasets/BX_Books.csv", encoding='latin-1') as csv_books:

    reader = csv.reader(csv_books, delimiter=";",quotechar='"')

    i=0

    for row in reader:

        i += 1

        if(i==1):
            continue

        book_isbn = stringFilter(row[0]).upper()

        book_title = stringFilter(row[1])

        book_year = stringFilter(row[3])

        publisher_name = stringFilter(row[4])

        author_name = stringFilter(row[2])

        # Inserta todas las editoriasles y todos los libros
        if (publisher_name != ""):
            e_id = findOrInsertByNombre("Editorial", publisher_name)
            if (book_isbn != "" and book_title != "" and book_year != ""):
                insertLibro(book_isbn, book_title, int(book_year), e_id)


        # Inserta todos los autores y crea todas las relaciones entre libro-autor
        if (author_name != ""):
            a_id = findOrInsertByNombre("autor", author_name)
            if (findLibro(book_isbn)):
                insertAutorLibro(a_id, book_isbn)

        conn.commit()


with open("./datasets/BX_Users.csv") as csv_books:

    reader = csv.reader(csv_books, delimiter=";",quotechar='"')

    i=0

    for row in reader:

        i += 1

        if(i==1):
            continue

        u_id = stringFilter(row[0])

        u_location = stringFilter(row[1].split(",")[-1])

        u_age = stringFilter(row[2])

        # Inserta todo los usuarios
        if (u_id != "" and u_location != "" and u_age != "" and u_age != "NULL"):
            insertUsuario(int(u_id), u_location, int(u_age))

        conn.commit()


with open("./datasets/BX_Book_Ratings.csv", encoding='latin-1') as csv_books:

    reader = csv.reader(csv_books, delimiter=";",quotechar='"')

    i=0

    for row in reader:

        i += 1

        if(i==1):
            continue

        u_id = stringFilter(row[0])

        b_isbn = stringFilter(row[1]).upper()

        rating = stringFilter(row[2])

        # Inserta todas las reviews
        if (u_id != "" and b_isbn != "" and rating != ""):
            if (findUsuario(int(u_id)) and findLibro(b_isbn)):
                insertLibroUsuario(b_isbn, int(u_id), int(rating))

        conn.commit()

with open("./datasets/GoodReads_100k_books.csv") as csv_books:

    reader = csv.reader(csv_books, delimiter=",",quotechar='"')

    i=0

    for row in reader:

        i += 1

        if(i==1):
            continue

        b_isbn = stringFilter(row[5]).upper()
        b_genre = [stringFilter(x) for x in row[3].split(",")]

        # Insert todas las relaciones y la relacion entre ellas y los libros
        if(findLibro(b_isbn)):
            for j in b_genre:
                id_genre = findOrInsertByNombre("genero", j)
                insertGeneroLibro(id_genre, b_isbn)

        conn.commit()
