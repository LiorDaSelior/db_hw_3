import mysql.connector as mdb
import util

def query_1(role, genre):
    """
    Return a list of persons who had a better average rating in a specific genre while performing the given role.
    If role or genre titles are not present in the DB an appropriate message is printed.

    :param role: Desired role performed by the person, such as 'Actor' or 'Director' .
    :param genre: Movie genre on which the metric will be run, such as 'Action' or 'Horror'.
    :return: None
    """
    con = mdb.connect(host=util.HOSTNAME, port=util.PORT, database=util.DATABASE, user=util.USERNAME,
                      password=util.PASSWORD)
    cur = con.cursor()

    try:
        cur.execute("""
            SELECT 
                p.full_name AS person_name,
                AVG(m.rating) AS average_rating_in_genre
            FROM 
                person p
            JOIN 
                movie_role mr ON p.id = mr.person_id
            JOIN 
                role r ON mr.role_id = r.id
            JOIN 
                movie_genre mg ON mr.movie_id = mg.movie_id
            JOIN 
                movie m ON mg.movie_id = m.id
            JOIN 
                genre g ON mg.genre_id = g.id
            WHERE 
                r.title = %s AND g.title = %s
            GROUP BY 
                p.full_name
            HAVING 
                AVG(m.rating) > (
                    SELECT 
                        AVG(m2.rating) AS avg_genre_rating
                    FROM 
                        movie_genre mg2
                    JOIN 
                        movie m2 ON mg2.movie_id = m2.id
                    JOIN 
                        genre g2 ON mg2.genre_id = g2.id
                    WHERE 
                        g2.title = %s
                )
            ORDER BY average_rating_in_genre DESC
        """, (role, genre, genre))

        results = cur.fetchall()
        if len(results) > 0:
            print(f"Full name   |   Average rating in {genre} genre")
            print("-------------------------")
            for row in results:
                print(f"{row[0]:<10} |   ${row[1]}")
        else:
            print("No results, please check your input.")
    except mdb.Error as e:
        print("Error:", e)
    finally:
        cur.close()
        con.close()


def query_2(starting_release_year):
    """
    Computes the total revenue for each movie genre since the specified start year (inclusive).

    :param: starting_release_date: The starting release date for movies to consider.
    :return: None
    """
    con = mdb.connect(host=util.HOSTNAME, port=util.PORT, database=util.DATABASE, user=util.USERNAME,
                      password=util.PASSWORD)
    cur = con.cursor()
    try:
        cur.execute("""
            SELECT 
                g.title,
                SUM(m.revenue) AS Total_Revenue
            FROM 
                try.genre AS g
            JOIN 
                try.movie_genre AS mg ON g.id = mg.genre_id
            JOIN 
                try.movie AS m ON mg.movie_id = m.id
            WHERE 
                YEAR(m.release_date) >= %s
            GROUP BY 
                g.id, g.title
            ORDER BY 
                Total_Revenue DESC
        """, (starting_release_year,))

        results = cur.fetchall()
        if len(results) > 0:
            print("Genre   |   Total Revenue")
            print("-------------------------")
            for row in results:
                print(f"{row[0]:<10} |   ${row[1]}")
        else:
            print("No results, please check your input.")
    except mdb.Error as e:
        print("Error:", e)
    finally:
        cur.close()
        con.close()

def query_3(role, starting_release_year):
    """
    Retrieves the top 100 most active people in a specific role since a given starting release year.

    :param: role: Desired role performed by the person, such as 'Actor' or 'Director' .
    :param: starting_release_date: The starting release date for movies to consider.
    :return: None
    """
    con = mdb.connect(host=util.HOSTNAME, port=util.PORT, database=util.DATABASE, user=util.USERNAME,
                      password=util.PASSWORD)
    cur = con.cursor()
    try:
        cur.execute("""
            SELECT 
                p.full_name AS person_name,
                COUNT(*) AS role_count
            FROM 
                person p
            JOIN 
                movie_role mr ON p.id = mr.person_id
            JOIN 
                role r ON mr.role_id = r.id
            JOIN 
                movie m ON mr.movie_id = m.id
            WHERE 
                r.title = %s AND YEAR(m.release_date) >= %s
            GROUP BY 
                p.id, p.full_name
            ORDER BY 
                role_count DESC
            LIMIT 100
        """, (role, starting_release_year,))

        results = cur.fetchall()
        if len(results) > 0:
            print(f"Full name   |   Movie count as {role} since {starting_release_year}")
            print("-------------------------")
            for row in results:
                print(f"{row[0]:<10} |   {row[1]}")
        else:
            print("No results, please check your input.")
    except mdb.Error as e:
        print("Error:", e)
    finally:
        cur.close()
        con.close()