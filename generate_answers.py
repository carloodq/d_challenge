# password "postgre123"



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
    from data_challenge import my_df
    import pandas as pd

    # read database
    user = 'postgres'
    host = 'localhost' 
    my_db = my_db(password, user, host)
    all = my_db.read_all()

    # create df
    my_df = my_df(all)

    # clean df, add labels from metadata and generate csvs for answers
    my_df.clean()
    my_df.add_labels()
    my_df.generate_csv()

if __name__ == "__main__":
    main()

