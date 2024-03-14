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

def snip_desc(desc, target_string1, target_string2, target_string3, k = 2): # desc must contain each target_string
    res = []
    desc_word_list = desc.split(" ")
    
    temp_res = snip_desc_helper(desc_word_list, target_string1, k)
    res.append(temp_res) if temp_res != "" else True
    
    if target_string2 != target_string1:
        temp_res = snip_desc_helper(desc_word_list, target_string2, k)
        res.append(temp_res) if temp_res != "" else True
        
    if target_string3 != target_string1 and target_string3 != target_string2:
        temp_res = snip_desc_helper(desc_word_list, target_string3, k)
        res.append(temp_res) if temp_res != "" else True
    
    return [value for value in res if value != ""]
    
def snip_desc_helper(desc_word_list, target_string, k):
    i = 0
    while i < len(desc_word_list):
        if target_string in desc_word_list[i]:
            break
        i += 1
    if i != len(desc_word_list):
        start_index = max(i - k, 0)
        end_index = min(i + k + 1, len(desc_word_list))
        return " ".join(desc_word_list[start_index:end_index])
    return ""