from queries_db_script import *

if __name__ == "__main__":
    query_1("Director", "Action")
    
    print("\n***\n")
    
    query_2(2015)
    
    print("\n***\n")
    
    query_3("Actor", 2018)
    
    print("\n***\n")
    
    query_4("johnson")
    
    print("\n***\n")
    
    query_5("war", "world", "battle") 
    
    print("\n***\n")
    
    query_5("love", "fire", "fire") # example of search with 1 or 2 keywords instead - just repeat a used keyword in parameters