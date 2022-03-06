import io
import json
from pprint import pprint
import instagram_scraper

import mysql.connector
from flask import Flask, request

from cloudfunction.util import loadJsonFromS3, saveJsonToDB
from secrets import aws_secret_access_key, aws_access_key_id
import boto3
import pandas as pd

app = Flask(__name__)


@app.route('/')
def hello_world() :
    data = [
        (20, 16, 23),
        (30, None, 11),
        (40, 34, 11),
        (50, 35, None),
        (60, 40, 13)
    ]

    # creating a DataFrame object
    df = pd.DataFrame(data, index=['a', 'b', 'c', 'd', 'e'],
                      columns=['x', 'y', 'z'])
    print(len(df.index))
    return 'Hello World!'

# @app.route('/test_scrap')
# def test_scrap() :  # put application's code here
#     name = request.args.get("username")
#     print(name)
#     args = {"login_user":"vectorwild","login_pass":"011298Vector"}
#     insta_scraper = instagram_scraper.InstagramScraper(**args)
#     insta_scraper.authenticate_with_login()
#     shared_data = insta_scraper.get_shared_data_userinfo(username=name)
#
#     arr = []
#
#     for item in insta_scraper.query_media_gen(shared_data) :
#         arr.append(item)
#
#     pprint(arr[0])
#     return arr[0]


@app.route('/saveProfileIG')
def saveProfileIG():

    cnx = mysql.connector.connect(user='admin', password='awsproject',
                                  host='mysql-project.clgag50aevk1.eu-west-3.rds.amazonaws.com',
                                  database='instagram_data')
    name = request.args.get("username")
    args = {"login_user" : "vectorwild", "login_pass" : "011298Vector"}
    insta_scrapper = instagram_scraper.InstagramScraper(**args)
    insta_scrapper.authenticate_with_login()
    shared_data = insta_scrapper.get_shared_data_userinfo(username=name)
    arr = []

    for item in insta_scrapper.query_media_gen(shared_data) :
        arr.append(item)

    name = request.args.get("username")
    res = saveJsonToDB(arr, name, cnx)
    return res

    return name + " Saved. " + res

@app.route('/ETLtoS3')
def ETLtoS3() :
    cnx = mysql.connector.connect(user='admin', password='awsproject',
                                  host='mysql-project.clgag50aevk1.eu-west-3.rds.amazonaws.com',
                                  database='instagram_data')
    client = boto3.client('s3',
                          aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key)
    query = "SELECT * FROM data_ig WHERE user_name = \"" + request.args.get("username") + "\""
    try :
        df = pd.read_sql(query, con=cnx)
    finally :
        cnx.commit()
        cnx.close()

    username = df["username"][0]
    min_like = df["nb_like"].min()
    max_like =df["nb_like"].max()
    min_comment = df["nb_comment"].min()
    max_comment =df["nb_comment"].max()
    average_like =df["nb_like"].mean()
    average_comment=df["nb_comment"].mean()
    total_like= df["nb_like"].sum()
    total_comment=df["nb_comment"].sum()
    total_post=len(df.index)

    KPI = {
        "Nom utilisateur" : username,
        "min_like" : min_like,
        "max_like":max_like,
        "min_comment":min_comment,
        "max_comment":max_comment,
        "average_like":average_like,
        "average_comment":average_comment,
        "total_like":total_like,
        "total_comment":total_comment,
        "total_post":total_post
    }

    json_response = json.dumps(KPI, indent=4)




    return json_response


if __name__ == '__main__' :
    app.run(host='0.0.0.0', port=3000)
