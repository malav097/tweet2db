import tweepy
import boto3
import mysql.connector
import sys

auth1 = ""
auth2 = ""
token1 = ""
token2 = ""

db_host = ""
db_user = ""
db_pass = ""
db_name = ""


auth = tweepy.OAuthHandler(auth1,
                           auth2)
auth.set_access_token(token1,
                      token)
api = tweepy.API(auth)

mydb = mysql.connector.connect(
    host= db_host,
    user= db_user,
    passwd= db_pass,
    database= db_name
)

mycursor = mydb.cursor()

delete = "DROP TABLE tweets ;"
create = "CREATE TABLE tweets (created_at VARCHAR(20));"

mycursor.execute(delete)
mycursor.execute(create)


def tableupdate(alcalde):

    try:
        for status in tweepy.Cursor(api.user_timeline, screen_name=alcalde).items(10):
            tweet = status._json
            columns = []
            separator = ','
            parameters = []
            parameter = '%s'
            values = []

            try:

                for key in tweet:
                    print(key)
                    columns.append(key)
                    parameters.append(parameter)

                    create_column = "ALTER TABLE tweets ADD " + key + " varchar(255);"
                    insert = "INSERT INTO tweets(" + key + ") VALUES(%s);"
                    value = str(tweet[key])
                    value = value.replace("'", '"')

                    values.append(value)

                    try:
                        mycursor.execute(create_column)
                        mydb.commit()

                    except:
                        pass
#
                columns_str = separator.join(columns)
                parameters_str = separator.join(parameters)
                values_tuple = tuple(values)
                print(values_tuple)

                query = "INSERT INTO tweets(" + columns_str + ") VALUES(" + parameters_str + ");"
                print(query)
                mycursor.execute(query, values_tuple)
                mydb.commit()

            except:
                pass
    except:
        print(alcalde + " not found")


file = sys.argv[1]

alcaldes = [line.rstrip('\n') for line in open(file)]


for alcalde in alcaldes:

    print(alcalde)
    tableupdate(alcalde)
