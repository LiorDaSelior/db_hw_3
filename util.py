USERNAME = 'root'
PASSWORD = '1234'
HOSTNAME = 'localhost'
DATABASE = 'try'
PORT = 3306

add_movie = ("""INSERT INTO movie
             (id, title, release_date, runtime, description, rating, production_budget, marketing_budget, revenue)
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
             """)
add_person = ("""INSERT INTO person
             (id, full_name, birth_date, death_date)
             VALUES (%s, %s, %s, %s)
             """)
add_role = ("""INSERT INTO role
             (id, title)
             VALUES (%s, %s)
             """)
add_genre = ("""INSERT INTO genre
             (id, title)
             VALUES (%s, %s)
             """)
add_movie_genre = ("""INSERT INTO movie_genre
             (movie_id, genre_id)
             VALUES (%s, %s)
             """)
add_movie_role = ("""INSERT INTO movie_role
             (movie_id, person_id, role_id)
             VALUES (%s, %s, %s)
             """)
