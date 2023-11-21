
def main():
    import sys
    try:
        password = sys.argv[1]
        user = sys.argv[2]
        host = sys.argv[3]
    except:
        print('enter credentials')
        return 0
        
    from data_challenge import my_db
    import pandas as pd

    user = 'postgres'
    host = 'localhost' 
    my_db = my_db(password, user, host)
    
    my_db.delete_all()
    print(len(my_db.read_all()), 'records in table')

if __name__ == '__main__':
    main()