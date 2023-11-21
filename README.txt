Instructions for data challenge:

1. Install postgresql if you don't have it on your machine

2. Create a database
Using psql terminal, run the command
CREATE DATABASE trips;

3. Create a new python environment
Install libraries with command 'pip install -r requirements.txt'
Note: if you want to also run the .ipynb uncomment all the requirements

4. Create a table 
Create a table called trips2
'python create_table.py <pwd> <user> <host>'

(4b. If needed eliminate all table records with 'python empty_table.py <pwd> <user> <host>')

5. Populate the table 
Note: data and metadata must be in the same folder
'python populate_table.py <pwd> <user> <host>'

6. Create the .csvs for PowerBI
'python generate_answers.py <pwd> <user> <host>'
Note: they will be saved in the same directory.