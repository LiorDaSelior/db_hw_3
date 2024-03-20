import mysql.connector as mdb
import pandas as pd
import util
import datetime


def fill_movie_table(cursor, movie_df):
    table_name = "movie"
    try:
        arr = zip(movie_df['id'], movie_df['primaryTitle'], movie_df['releaseDate'], movie_df['runtimeMinutes'],
                  movie_df['overview'], movie_df['rating'], movie_df['productionBudget'], movie_df['marketingBudget'], movie_df['boxOffice'])
        for movie_id, title, release_date, runtime, description, rating, production_budget, marketing_budget, revenue in arr:
            temp_release_date = datetime.datetime.strptime(release_date, '%d/%m/%Y').strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute(util.add_movie, (movie_id, title, temp_release_date, runtime, description, rating, production_budget, marketing_budget, revenue))  
    except mdb.Error as err:
        print(f"Error while adding data to table '{table_name}':")
        print(err)
        return False
    return True

def fill_person_table(cursor, person_df):
    table_name = "person"
    try:
        arr = zip(person_df['id'], person_df['primaryName'], person_df['birth_date'], person_df['death_year'])
        for person_id, full_name, birth_date, death_date in arr:
            temp_birth_date = datetime.datetime.strptime(birth_date, '%d/%m/%Y').strftime('%Y-%m-%d %H:%M:%S')
            if not isinstance(death_date, float):
                temp_death_date = datetime.datetime.strptime(death_date, '%d/%m/%Y %H:%M').strftime('%Y-%m-%d %H:%M:%S')
            else:
                temp_death_date = None
            cursor.execute(util.add_person, (person_id, full_name, temp_birth_date, temp_death_date))  
    except mdb.Error as err:
        print(f"Error while adding data to table '{table_name}':")
        print(err)
        return False
    return True

def fill_genre_table(cursor, genre_df):
    table_name = "genre"
    try:
        for genre_id, title in zip(genre_df['index'], genre_df['genreName']):
            cursor.execute(util.add_genre, (genre_id, title))  
    except mdb.Error as err:
        print(f"Error while adding data to table '{table_name}':")
        print(err)
        return False
    return True

def fill_role_table(cursor, role_df):
    table_name = "role"
    try:
        for role_id, title in zip(role_df['index'], role_df['categoryName']):
            cursor.execute(util.add_role, (role_id, title))
    except mdb.Error as err:
        print(f"Error while adding data to table '{table_name}':")
        print(err)
        return False
    return True

def fill_movie_genre_table(cursor, movie_genre_df):
    table_name = "movie_genre"
    try:
        for movie_id, genre_id in zip(movie_genre_df['movie_id'], movie_genre_df['genre_id']):
            cursor.execute(util.add_movie_genre, (movie_id, genre_id))
    except mdb.Error as err:
        print(f"Error while adding data to table '{table_name}':")
        print(err)
        return False
    return True

def fill_movie_role_table(cursor, movie_role_df):
    table_name = "movie_role"
    try:
        for movie_id, person_id, role_id in zip(movie_role_df['movie_id'], movie_role_df['person_id'], movie_role_df['category_id']):
            cursor.execute(util.add_movie_role, (movie_id, person_id, role_id))
    except mdb.Error as err:
        print(f"Error while adding data to table '{table_name}':")
        print(err)
        return False
    return True
    
if __name__ == '__main__':
    con = mdb.connect(host=util.HOSTNAME, port=util.PORT, database=util.DATABASE, user=util.USERNAME, password=util.PASSWORD)
    cur = con.cursor()
    
    movie_df = pd.read_csv("data/movie_data.csv")
    person_df = pd.read_csv("data/person_data.csv")
    genre_df = pd.read_csv("data/genre_data.csv")
    role_df = pd.read_csv("data/category_data.csv")
    movie_genre_df = pd.read_csv("data/genre_movie_data.csv")
    movie_role_df = pd.read_csv("data/movie_person_data.csv")
    arr = [(fill_movie_table, movie_df),
           (fill_person_table, person_df),
           (fill_genre_table, genre_df),
           (fill_role_table, role_df),
           (fill_movie_genre_table, movie_genre_df),
           (fill_movie_role_table, movie_role_df)]
    
    for f, df in arr:
        check = f(cur, df)
        if not check:
            con.rollback()
            print("An error occurred during filling table - rollback.")
            break
    if check:
        con.commit()
        print("Tables filled successfully.")
    cur.close()
    con.close()