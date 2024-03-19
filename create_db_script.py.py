import mysql.connector as mdb
import util


def create_table(cursor, query, table_name):
    try:
        cursor.execute(query)
    except mdb.Error as err:
        print(f"Error while creating table '{table_name}':")
        print(err)
        return False
    return True

def create_full_text_index(cursor, table_name, column_name):
    try:
        cursor.execute(f"ALTER TABLE {table_name} ADD FULLTEXT({column_name})")
    except mdb.Error as err:
        print(f"Error while creating full-text index on '{table_name}.{column_name}':")
        print(err)
        return False
    return True

def create_index(cursor, table_name, column_name):
    try:
        cursor.execute(f"CREATE INDEX {column_name}_index ON {table_name} ({column_name})")
    except mdb.Error as err:
        print(f"Error while creating index on '{table_name}.{column_name}':")
        print(err)
        return False
    return True

def create_indexes():
    full_index_table_column_list = [("movie","release_date"), ("movie","rating")]
    
    con = mdb.connect(host=util.HOSTNAME, port=util.PORT, database=util.DATABASE, user=util.USERNAME, password=util.PASSWORD)
    cur = con.cursor()

    check_indexes = True
    for table_name, column_name in full_index_table_column_list:
        check_indexes = create_index(cur, table_name, column_name)
        if not check_indexes:
            con.rollback()
            print("An error occurred during index creation - rollback.")
            break
    if check_indexes:
        con.commit()
        print("Indexes created successfully.")
    cur.close()
    con.close()
    return check_indexes


def create_full_text_indexes():
    full_index_table_column_list = [("movie","description"), ("person","full_name")]
    
    con = mdb.connect(host=util.HOSTNAME, port=util.PORT, database=util.DATABASE, user=util.USERNAME, password=util.PASSWORD)
    cur = con.cursor()

    check_full_text_indexes = True
    for table_name, column_name in full_index_table_column_list:
        check_full_text_indexes = create_full_text_index(cur, table_name, column_name)
        if not check_full_text_indexes:
            con.rollback()
            print("An error occurred during full-text index creation - rollback.")
            break
    if check_full_text_indexes:
        con.commit()
        print("Full-text indexes created successfully.")
    cur.close()
    con.close()
    return check_full_text_indexes

def create_tables():
    table_name_list = ["movie","person","genre","role","movie_genre","movie_role"]
    query_dict = {
        "movie" : """CREATE TABLE IF NOT EXISTS movie
            (id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(256) NOT NULL,
            release_date DATE NOT NULL,
            runtime INT NOT NULL,
            description VARCHAR(1024),
            rating FLOAT NOT NULL,
            production_budget INT,
            marketing_budget INT,
            revenue INT
            );""",
        "person" : """CREATE TABLE IF NOT EXISTS person 
            (id INT AUTO_INCREMENT PRIMARY KEY,
            full_name VARCHAR(256) NOT NULL,
            birth_date DATE NOT NULL,
            death_date DATE
            )""",
        "genre" : """CREATE TABLE IF NOT EXISTS genre 
            (id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(128) NOT NULL
            )""",
        "role" : """CREATE TABLE IF NOT EXISTS role 
            (id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(128) NOT NULL
            )""",
        "movie_genre" : """CREATE TABLE IF NOT EXISTS movie_genre 
            (movie_id INT,
            genre_id INT,
            FOREIGN KEY (movie_id) REFERENCES movie(id),
            FOREIGN KEY (genre_id) REFERENCES genre(id)
            )""",
        "movie_role" : """CREATE TABLE IF NOT EXISTS movie_role 
            (movie_id INT,
            person_id INT,
            role_id INT,
            FOREIGN KEY (movie_id) REFERENCES movie(id),
            FOREIGN KEY (person_id) REFERENCES person(id),
            FOREIGN KEY (role_id) REFERENCES role(id)
        )""",
    }
    
    con = mdb.connect(host=util.HOSTNAME, port=util.PORT, database=util.DATABASE, user=util.USERNAME, password=util.PASSWORD)
    cur = con.cursor()
    
    check_tables = True
    for table_name in table_name_list:
        check_tables = create_table(cur, query_dict[table_name], table_name)
        if not check_tables:
            con.rollback()
            print("An error occurred during table creation - rollback.")
            break
    if check_tables:
        con.commit()
        print("Tables created successfully.")
    cur.close()
    con.close()
    return check_tables

def create_db():
    no_error_check = create_tables()
    if no_error_check:
        no_error_check = create_indexes()
    if no_error_check:
        no_error_check = create_full_text_indexes()
    if no_error_check:
        print("DB creation successful.") 
    else:
        print("Aborting DB creation.") 

if __name__ == '__main__':
    create_db()