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
def hello_world() :  # put application's code here
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
    # save
    # recup
    # loadindb
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
    #
    # res = saveJsonToDB(arr, name, cnx)
    name = request.args.get("username")
    # response = loadJsonFromS3(file_name)
    # response = json.loads(response)
    res = saveJsonToDB(arr, name, cnx)
    return res

    return name + " Saved. " + res

@app.route('/ETLtoS3')
def S3toRDS() :
    cnx = mysql.connector.connect(user='admin', password='awsproject',
                                  host='mysql-project.clgag50aevk1.eu-west-3.rds.amazonaws.com',
                                  database='project')


    return "response"


# @app.route('/RDStoS3')
# def RDStoS3() :


if __name__ == '__main__' :
    app.run(host='0.0.0.0', port=3000)
