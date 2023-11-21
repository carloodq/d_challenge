import psycopg2 
import pandas as pd

class my_db:
    def __init__(self, password, user, host):
        self.password = password
        self.user = user
        self.host = host

    def connect_db(self):

        conn = psycopg2.connect(
                host=self.host,
                database="trips",
                user=self.user,
                password = self.password)
        
        cur = conn.cursor()

        return conn, cur



    def create_table(self):

        conn, cur = self.connect_db()

        cur.execute("""
            CREATE TABLE trips2(
        id serial PRIMARY KEY,
        TravelMotives  text,
        Population text,
        TravelModes text,
        RegionCharacteristics  text,
        Periods  text,
        Trip_in_a_year integer,
        Km_travelled_in_a_year   integer, 
        Hours_travelled_in_a_year  real, 
        UserId  integer
        )
        """)

        conn.commit()


    def add_rows(self, df_csv):

        try:
            
            print('started adding rows..')

            conn, cur = self.connect_db()


            def process_row(all_values):
                all_values = [str(x).strip() if len(str(x).strip()) > 1 else None for x in all_values][1:]
                new_row = []
                if all_values[0]:
                    new_row.append(str(all_values[0]))
                else:
                    new_row.append(None)

                if all_values[1]:
                    new_row.append(str(all_values[1]))
                else:
                    new_row.append(None)

                if all_values[2]:
                    new_row.append(str(all_values[2]))
                else:
                    new_row.append(None)

                if all_values[3]:
                    new_row.append(str(all_values[3]))
                else:
                    new_row.append(None)

                if all_values[4]:
                    new_row.append(str(all_values[4]))
                else:
                    new_row.append(None)

                if all_values[5]:
                    new_row.append(int(all_values[5]))
                else:
                    new_row.append(None)

                if all_values[6]:
                    new_row.append(int(all_values[6]))
                else:
                    new_row.append(None)

                if all_values[7]:
                    new_row.append(float(all_values[7]))
                else:
                    new_row.append(None)

                if all_values[8]:
                    new_row.append(int(all_values[8]))
                else:
                    new_row.append(None)


                return new_row

            for i in range(len(df_csv)):
                try:
                    unprocessed_row =  list(df_csv.values[i])
                    new_row = process_row(unprocessed_row)
                    cur.execute('''INSERT INTO trips2
                        (TravelMotives  ,
                        Population ,
                        TravelModes ,
                        RegionCharacteristics  ,
                        Periods  ,
                        Trip_in_a_year ,
                        Km_travelled_in_a_year   , 
                        Hours_travelled_in_a_year  , 
                        UserId  )
                        VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s)''', tuple(new_row))
                except:
                    print('error', new_row)
            


            conn.commit()
            
            print('rows added')

            return 'ok'
        
        except:
            return 'error with inserting records'
        

    def delete_all(self):

        try:

            
            conn, cur = self.connect_db()

            cur.execute('DELETE FROM trips2')
            conn.commit()
            
            print('rows deleted')



            return 'ok'

        except:

            return 'deletion failed'
        
    
    def read_all(self):

        conn, cur = self.connect_db()


        cur.execute('SELECT * FROM trips2')
        all = cur.fetchall()

        return all
    


class my_df:
    def __init__(self, all):

        self.df = pd.DataFrame(all, columns=['ID',	'TravelMotives',	
                            'Population',	'TravelModes',
                           	'RegionCharacteristics'	,'Periods',
                            'Trip_in_a_year','Km_travelled_in_a_year',
                        	'Hours_travelled_in_a_year'	,'UserId'])
        
    def clean(self):

        print('cleaning..')

        stats = ['Trip_in_a_year'	,'Km_travelled_in_a_year',	'Hours_travelled_in_a_year']

        def drop_negatives(df, stats = stats):
            for s in stats:
                negatives = df[(df[s] < 0)]
                print('Dropping', len(negatives), 'negatives')
                df = df.drop(negatives.index)
            return df
        
        def drop_outliers(df, stats = stats):
            for s in stats:
                # calculate IQR
                Q1 = df[s].quantile(0.25)
                Q3 = df[s].quantile(0.75)
                IQR = Q3 - Q1
                # identify outliers
                threshold = 5
                outliers = df[(df[s] < Q1 - threshold * IQR) | (df[s] > Q3 + threshold * IQR)]
                # dropping outliers
                print('Dropping', len(outliers), 'outliers')
                df = df.drop(outliers.index)
            return df
        
        def clean_invalid(df):
            # cleaning row with invalid motives code
            travel_motives = ['2030170','2030190',   '2030200', 
                              '2030210', '2030220','2030230',
                                '2030240', '2030250', '2820740']
            before = df.shape[0]
            df = df[df['TravelMotives'].isin(travel_motives)]
            after = df.shape[0]
            print('Dropped', before-after, 'invalid motives rows')

            # cleaning row with invalid population code
            before = df.shape[0]
            df = df[df['Population'].isin(['A048710', 'A048709'])]
            after = df.shape[0]
            print('Dropped', before-after, 'invalid population rows')

           # there is a travel mode that is not in the metadata file, but I'll keep is because it's associated with > 1k rows

            # cleaning row with invalid regions code
            before = df.shape[0]
            df = df[df['RegionCharacteristics'].isin(['PV23', 'PV30', 'PV31', 'PV20', 'PV26', 'PV25', 'PV29', 'PV21',   'PV28', 'PV27', 'PV24', 'PV22'])]
            after = df.shape[0]
            print('Dropped', before-after, 'invalid region rows')

            # cleaning wrong periods
            before = df.shape[0]
            df = df[df['Periods'].isin(['2020JJ00', '2019JJ00', '2022JJ00',
                                         '2021JJ00', '2018JJ00'])]
            after = df.shape[0]
            print('Dropped', before-after, 'invalid periods rows')

            return df
        
        self.df = drop_negatives(self.df)
        self.df = drop_outliers(self.df)
        self.df = clean_invalid(self.df)

        return 'ok'
    
    def add_labels(self, urbanization_level = 'urbanization_level.csv',
                    region = 'region.csv', 
                    travel_motives = 'travel_motives.csv',
                    travel_mode =  'travel_mode.csv',
                    population = 'population.csv'):

        # load metadata csvs
        urbanization = pd.read_csv(urbanization_level, delimiter=';')
        regions = pd.read_csv(region)
        region_info = pd.merge(urbanization, regions, left_on = "provinces", 
                               right_on = "region", how=  'right')
        region_info['code'] = region_info['code'].apply(lambda x: x.strip())
        travel_motives = pd.read_csv(travel_motives)
        travel_motives['code'] = travel_motives['code'].apply(lambda x: str(x))
        travel_mode = pd.read_csv(travel_mode, delimiter = '|')
        population = pd.read_csv(population)
        
        # add the area as a column
        self.df = pd.merge(self.df, region_info, left_on='RegionCharacteristics', right_on='code')
     
        # add the motives as a column
        travel_motives['TravelMotives'] = travel_motives['code']
        self.df = pd.merge(self.df, travel_motives[['motive', 'TravelMotives']], on = 'TravelMotives')
      
        # add the mode as a column
        travel_mode['TravelModes'] = travel_mode['code']
        self.df = pd.merge(self.df, travel_mode[['mode', 'TravelModes']], on = 'TravelModes')
      
        # add the population as a column
        population['Population'] = population['code']
        self.df = pd.merge(self.df, population[['population', 'Population']], on = 'Population')

        # fixing error with level_urbanization
        def stripstring(x):
            if x == "Moderately urbanised\t":
                x = "Moderately urbanised"
            return x
        self.df['level_urbanization'] = self.df['level_urbanization'].apply(lambda x: stripstring(x))

        # making level_urbanization categorical with order
        self.df['level_urbanization'] = pd.Categorical(self.df['level_urbanization'], [ 'Hardly urbanised', 'Moderately urbanised', 'Strongly urbanised', 'Extremely urbanised'])


        return 'ok'
    

    def generate_csv(self):



        def generate_answer_one(df = self.df):
            res1 = df.sort_values("level_urbanization")[df['TravelMotives'] == '2030200'].groupby(['Periods', 'mode', 'level_urbanization'], as_index=False)['ID'].count()
            res1.to_csv('qn1.csv')
            res1b = df.sort_values("level_urbanization")[df['TravelMotives'] == '2030200'].groupby(['Periods', 'mode', 'level_urbanization'], as_index=False)['Trip_in_a_year'].sum()
            res1b.to_csv('qn1b.csv')
            print('qn1.csv and qn1b.csv saved')

            return 'ok'
        
        def generate_answer_two(df = self.df):
            # filter only west regions
            west_regions = ['PV26', 'PV24']
            df_west = df[df['RegionCharacteristics'].isin(west_regions)]

            # assuming across the whole period
            bike_travellers = df_west[df_west['TravelModes'] == 'A018984'].groupby('UserId')[['Km_travelled_in_a_year']].sum().sort_values(by = 'Km_travelled_in_a_year', ascending = False)
            bike_travellers.to_csv('qn2.csv')
            print('qn2.csv saved')

            return 'ok'
        
        def generate_answer_three(df = self.df):
            topbike = df[df['Population'] == 'A048709'][df['TravelModes'] == 'A018984'].groupby('UserId')[['Km_travelled_in_a_year']].sum().sort_values(by = 'Km_travelled_in_a_year', ascending = False).head(8)
            topbike = list(topbike.index.values)
            qn3 = df[df['UserId'].isin(topbike)][df['Periods'] == '2022JJ00']
            qn3.to_csv('qn3.csv')

            # same but for top 100 else there are not many results
            topbike = df[df['Population'] == 'A048709'][df['TravelModes'] == 'A018984'].groupby('UserId')[['Km_travelled_in_a_year']].sum().sort_values(by = 'Km_travelled_in_a_year', ascending = False).head(100)
            topbike = list(topbike.index.values)
            qn3b = df[df['UserId'].isin(topbike)][df['Periods'] == '2022JJ00']
            # common here is intended as how many trips are made 
            qn3b = qn3b.groupby(['motive'], as_index=False)[['Trip_in_a_year']].sum().sort_values('Trip_in_a_year')
            qn3b.to_csv('qn3b.csv')

            print('qn3.csv and qn3b.csv saved')

            return 'ok'


        def generate_answer_four(df = self.df):
            going_to_edu = df[df['TravelMotives'] == '2030210']
            going_to_edu_sums = going_to_edu
            # for finding the people with least number of kms we need to remove the nas for kms and trips
            going_to_edu_sums = going_to_edu[going_to_edu['Km_travelled_in_a_year'].notna()]
            going_to_edu_sums = going_to_edu[going_to_edu['Trip_in_a_year'].notna()]

            # keep 10
            going_to_edu_sums = going_to_edu_sums.groupby('UserId').sum().sort_values('Km_travelled_in_a_year').head(10)
            ppl_q3 = list(going_to_edu_sums.head(10).index)
            dfq4 = going_to_edu[going_to_edu['UserId'].isin(ppl_q3)]
            qn4 = dfq4.groupby(['Periods']).mean()[['Trip_in_a_year']]

            # reformat years for Power BI
            qn4 = qn4.reset_index()
            qn4['Periods'] = qn4['Periods'].apply(lambda x: x[:4])
            qn4.to_csv('qn4.csv')

            print('qn4.csv saved')
            return 'ok'
        
        generate_answer_one()
        generate_answer_two()
        generate_answer_three()
        generate_answer_four()

        return 'ok'


        


        



    

















        








        


        
        





    


    








    