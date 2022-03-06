import io
import json

import mysql.connector
from flask import Flask

from cloudfunction.util import loadJsonFromS3, saveJsonToDB
from secrets import aws_secret_access_key, aws_access_key_id
import boto3
import pandas as pd

app = Flask(__name__)


@app.route('/')
def hello_world() :  # put application's code here
    return 'Hello World!'

@app.route('/saveProfileIG')
def saveProfileIG():
    # save
    # recup
    # loadindb
    print("Here")
    cnx = mysql.connector.connect(user='admin', password='awsproject',
                                  host='mysql-project.clgag50aevk1.eu-west-3.rds.amazonaws.com',
                                  database='instagram_data')
    print("here2")
    file_name = "cedric_andriam"
    response = loadJsonFromS3(file_name)
    response = json.loads(response)
    res = saveJsonToDB(response, cnx)
    return res

@app.route('/S3toRDS')
def S3toRDS() :
    cnx = mysql.connector.connect(user='admin', password='awsproject',
                                  host='mysql-project.clgag50aevk1.eu-west-3.rds.amazonaws.com',
                                  database='project')
    client = boto3.client('s3',
                          aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key)
    s3_object = client.get_object(Bucket='tokyaws', Key='AWS_project.csv')
    body = s3_object['Body']
    response = body.read().decode("utf-8")
    data = io.StringIO(response)
    df = pd.read_csv(data, sep=";", header=0)
    print(df.head())
    print(df.columns)
    add_matiere = ("INSERT INTO matiere (matiere, description, nbheure) VALUES (%s,%s,%s)")
    try :
        cursor = cnx.cursor()
        print("Insert all from csv")
        for i in df.index :
            new_matiere = (df["Matière"][i], df["Description"][i], int(df[" Nb heure"][i]))
            print(new_matiere)
            cursor.execute(add_matiere, new_matiere)
    finally :
        cnx.commit()
        cursor.close()
        cnx.close()
    return response


# @app.route('/RDStoS3')
# def RDStoS3() :


if __name__ == '__main__' :
    app.run(host='0.0.0.0', port=3000)
