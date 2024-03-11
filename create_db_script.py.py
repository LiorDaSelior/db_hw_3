import MySQLdb as mdb
import util


def create_table(connection, cursor, query, table_name):
    try:
        cursor.execute(query)
    except connection.Error as err:
        print(f"Error while creating table '{table_name}':")
        print(err)
        return False
    return True
    
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
            )""",
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
    
    for table_name in table_name_list:
        check = create_table(con, cur, query_dict[table_name], table_name)
        if not check:
            con.rollback()
            print("An error occurred during table creation - rollback.")
            break
    if check:
        con.commit()
        print("Tables created successfully.")
    cur.close()
    con.close()

if __name__ == '__main__':
    create_tables()