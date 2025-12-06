from flask import Flask,request,jsonify,redirect,url_for
from dotenv import load_dotenv
from markupsafe import escape
import os
import mysql.connector
import hashlib
import json
import base64
import pandas as pd
from flask import session
import logging

app = Flask(__name__)
app.json.sort_keys=False

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()
db_user=os.environ['DB_USER']
db_host=os.environ['HOST']
db_name=os.environ['DB_NAME']
db_password=os.environ['DB_PASSWORD']
partneruid=os.environ['PARTNERUID']
apipassword=os.environ['API_PASSWORD']
partneruid_uni=os.environ['PARTNERUID_UNI']
username=os.environ['USER_']


def get_connection():
    mydb = mysql.connector.connect(
    host=db_host,
    user=db_user,
    password=db_password,
    database=db_name,
    auth_plugin='mysql_native_password'
    )
    return mydb

logging.info("MySQL connection configured for partner: %s", partneruid)






# Set the secret key to some random bytes. Keep this really secret!
app.secret_key = b'_5#y2L"F4Q8z\n\xec]/'



def passCheckMultiBill(username,timestamp,partneruid):
    sample_string=username+str(timestamp)
    password=hashlib.sha512(sample_string.encode('utf-8')).hexdigest()
    password=str(partneruid)+':'+password
    sample_string_bytes=password.encode('ascii')
    base64_bytes = base64.b64encode(sample_string_bytes)
    return base64_bytes.decode()

def passwordcheck(timestamp,partneruid,apipassword):
    hashed_str=str(timestamp)+str(apipassword)+str(partneruid)
    res=hashlib.md5(hashed_str.encode()).hexdigest()

    return res


def formatdata(result):
    result=result[0]
    localdict={
      "billId": result[4],
      "billTo": result[5],
      "billAmount":result[6] ,
      "billCurrency":result[7] ,
      "billNumber":result[2] ,
      "dueDate": result[8],
      "status":"pending",
      "partialPayAllowed": "1",
      "description": result[3]
         }
    return localdict




def getValue(query):
    logging.info("Executing query: %s", query)
    mydb=get_connection()
    mycursor = mydb.cursor()
    result=pd.read_sql(query,mydb)
    mydb.close()
    return result
# Single Bill Implementation
@app.route("/api/values/queryBillInfo",methods=['POST'])
def queryBillInfoApi():
    mydb=get_connection()
    print(request.json['requestBody'])
    print(request.json['requestHeader']['timestamp'],partneruid,apipassword)
    logging.info("QueryBillInfo request received: %s", request.json)

    password=passwordcheck(request.json['requestHeader']['timestamp'],partneruid,apipassword)

    if password==request.json['requestHeader']['apiPassword']:
        response={
            "schemaVersion": "1.0", #
            "requestId":request.json['requestId'],
            "responseHeader": {
                "timestamp": request.json['requestHeader']['timestamp'],
                "resultCode": "",
                "resultMessage": ""
            }

        }

        try:
            mycursor = mydb.cursor()
            mycursor.callproc('QueryBillInfo', [request.json['requestBody']['invoiceId']])
            myresult = []
            for result in mycursor.stored_results():
                myresult.extend(result.fetchall())

            if myresult:
                print("======")
                billInfo=[]

                response['responseHeader']['resultCode']="0"
                response['responseHeader']['resultMessage']="SUCCESS"
                myresult= formatdata(myresult)
                billInfo.append(myresult)
                print(billInfo)

                response['billInfo']=billInfo
                mydb.close()
                logging.info("QueryBillInfo response: %s", response)
                return jsonify(response)
            else:
                response['responseHeader']['resultMessage']="Not Found"
                response['responseHeader']['resultCode']="401"
                mydb.close()
                logging.warning("QueryBillInfo not found for invoiceId: %s", request.json['requestBody']['invoiceId'])
                return jsonify(response), 401
        except mysql.connector.Error as err:
            logging.error("Database error in QueryBillInfo: %s", err)
            response['responseHeader']['resultMessage'] = "Database error"
            response['responseHeader']['resultCode'] = "500"
            mydb.close()
            return jsonify(response), 500

    else:
        logging.warning("Invalid password for QueryBillInfo request.")
        response = {
            "schemaVersion": "1.0",
            "requestId": request.json.get('requestId'),
            "responseHeader": {
                "timestamp": request.json.get('requestHeader', {}).get('timestamp'),
                "resultCode": "401",
                "resultMessage": "Invalid password"
            }
        }
        return jsonify(response), 401


def getTrasactionInfo(transactionId,req):
    mydb = get_connection()
    mycursor = mydb.cursor(dictionary=True)
    mycursor.callproc('ResolveBillPayment', [transactionId])
    data = None
    for result in mycursor.stored_results():
        data = result.fetchone()
    mydb.close()
    return data




@app.route("/api/values/payBillNotification",methods=['POST'])
def payNotificationBillApi():

    mydb=get_connection()
    logging.info("PayNotificationBill request received: %s", request.json)

    
    password=passwordcheck(request.json['requestHeader']['timestamp'],partneruid,apipassword)

    if password==request.json['requestHeader']['apiPassword']:
        response={
            "schemaVersion": "1.0",
            "requestId":request.json['requestId'],
            "responseHeader": {
                "timestamp": request.json['requestHeader']['timestamp'],
                "resultCode": "",
                "resultMessage": ""
            }

        }


        try :
            bill_info = request.json['requestBody']['billInfo']
            trans_info = request.json['requestBody']['transacionInfo']
            mycursor = mydb.cursor()
            args = [
                bill_info['invoiceId'], bill_info['paidBy'], bill_info['paidAt'], bill_info['paidDate'],
                bill_info['billTo'], bill_info['dueDate'], bill_info['description'], bill_info['billerName'],
                bill_info['isPrepaid'], trans_info['tansactionId'], trans_info['amount'], trans_info['currency']
            ]
            mycursor.callproc('PayBillNotification', args)
            mydb.commit()

            response['responseHeader']['resultCode']="0"
            response['responseHeader']['resultMessage']="SUCCESS"
            response["confirmationId"]=request.json['requestBody']['transacionInfo']['tansactionId']
            logging.info("PayBillNotification successful for transactionId: %s", trans_info['tansactionId'])
            return jsonify(response)
        except mysql.connector.errors.Error as ex:
            logging.error("Database error in PayNotificationBill: %s", ex)
            response['responseHeader']['resultMessage']="error occured"
            response['responseHeader']['resultCode']="401"
            mydb.close()
            return jsonify(response), 401

    else:
        logging.warning("Invalid password for PayNotificationBill request.")
        response = {
            "schemaVersion": "1.0",
            "requestId": request.json.get('requestId'),
            "responseHeader": {
                "timestamp": request.json.get('requestHeader', {}).get('timestamp'),
                "resultCode": "401",
                "resultMessage": "Wrong credentials"
            }
        }
        return jsonify(response), 401

### Integration for Universities  Start here

def getDecodedData(payload):
    token=payload
    token_payload = token.split(".")[1]
    token_payload_decoded = str(base64.b64decode(token_payload + "=="), "utf-8")
    data = json.loads(token_payload_decoded)
    return data



@app.route("/api/values/query", methods=['POST'])
def queryBillInfoForMutliBill():
    logging.info("MultiBill Query request received: %s", request.json)
    p = passCheckMultiBill(username, request.json['payload']['timestamp'], partneruid_uni)

    if p == request.headers.get('Authorization'):

        response = {
            "schemaVersion": "1.0",
            "requestId": request.json['payload']['requestId'],
            "responseHeader": {
                "timestamp": request.json['payload']['timestamp'],
                "resultCode": "",
                "resultMessage": ""
            }
        }

        bills = []
        mydb = None
        cursor = None
        
        try:
            jsonData = getDecodedData(request.json['payload']['requestBody'])
            student_id = jsonData['studentId']
            
            logging.info(f"Querying bills for student: {student_id}")

            mydb = get_connection()
            cursor = mydb.cursor(dictionary=True)
            
            # Call stored procedure
            cursor.callproc('QueryMultiBillInfo', [student_id])

            bill_total = None
            data = []
            
            # Convert generator to list to access results
            results = list(cursor.stored_results())
            logging.info(f"Number of result sets returned: {len(results)}")
            
            if len(results) >= 1:
                # First result set - bill total
                result_set_1 = results[0]
                row = result_set_1.fetchone()  # Already a dict because cursor(dictionary=True)
                if row:
                    bill_total = row
                    logging.info(f"Bill total retrieved: {bill_total}")
            
            if len(results) >= 2:
                # Second result set - bills list
                result_set_2 = results[1]
                rows = result_set_2.fetchall()  # Already list of dicts
                if rows:
                    data = rows
                    logging.info(f"Number of bills retrieved: {len(data)}")

            if bill_total:
                response['responseHeader']['resultCode'] = "0"
                response['responseHeader']['resultMessage'] = "SUCCESS"
                response.update(bill_total)
                response['bills'] = data
                return jsonify(response)
            else:
                logging.warning(f"No bill data found for student: {student_id}")
                response['responseHeader']['resultMessage'] = "No data found"
                response['responseHeader']['resultCode'] = "404"
                return jsonify(response), 404

        except mysql.connector.Error as db_err:
            logging.error(f"Database error in MultiBill Query: {db_err}")
            logging.error(f"Error code: {db_err.errno}, SQL State: {db_err.sqlstate}")
            response['responseHeader']['resultMessage'] = "Database Error"
            response['responseHeader']['resultCode'] = "500"
            return jsonify(response), 500
            
        except Exception as e:
            logging.error(f"Error in MultiBill Query: {e}", exc_info=True)
            response['responseHeader']['resultMessage'] = "Internal Server Error"
            response['responseHeader']['resultCode'] = "500"
            return jsonify(response), 500
            
        finally:
            # Always close cursor and connection
            if cursor:
                cursor.close()
            if mydb:
                mydb.close()

    else:
        logging.warning("Unauthorized access to MultiBill Query.")
        response = {
            "schemaVersion": "1.0",
            "requestId": request.json.get('payload', {}).get('requestId'),
            "responseHeader": {
                "timestamp": request.json.get('payload', {}).get('timestamp'),
                "resultCode": "401",
                "resultMessage": "Unauthorized"
            }
        }
        return jsonify(response), 401
        
@app.route("/api/values/pay",methods=['POST'])
def queryInfoForMultiBillPayment():
    logging.info("MultiBill Payment request received: %s", request.json)
    p=passCheckMultiBill(username,request.json['payload']['timestamp'],partneruid_uni)

    response={
            "schemaVersion": "1.0",
            "requestId":request.json['payload']['requestId'],
            "responseHeader": {
                "timestamp": request.json['payload']['timestamp'],
                "resultCode": "",
                "resultMessage": ""
            }

        }


    if p == request.headers.get('Authorization'):
        mydb=get_connection()
        jsonData=getDecodedData(request.json['payload']['requestBody'])
        logging.info("Decoded payload for payment: %s", json.dumps(jsonData))

        response['responseHeader']['resultCode']="0"
        response['responseHeader']['resultMessage']="SUCCESS"
        response["confirmationId"]=jsonData['transacionInfo']['tansactionId']

        for i in range(len(jsonData['bills'])):
            try :
                mycursor = mydb.cursor()
                bill = jsonData['bills'][i]
                args = [
                    jsonData['transacionInfo']['tansactionId'],
                    jsonData['payerInfo']['studentId'],
                    jsonData['payerInfo']['studentName'],
                    jsonData['transacionInfo']['totalAmount'],
                    jsonData['payerInfo']['paidAt'],
                    bill['dueDate'],
                    bill['accountNumber'],
                    bill['accountTitle'],
                    bill['description'],
                    bill['amount']
                ]
                mycursor.callproc('PayMultiBill', args)
                mydb.commit()
                logging.info("Processed payment for bill: %s", bill['accountNumber'])

            except mysql.connector.errors.Error as ex:
                logging.error("Database error in MultiBill Payment for account %s: %s", bill['accountNumber'], ex)
                response['responseHeader']['resultMessage']="error occured"
                response['responseHeader']['resultCode']="401"
                mydb.close()


        return response
    else:
        logging.warning("Unauthorized access to MultiBill Payment.")
        response = {
            "schemaVersion": "1.0",
            "requestId": request.json.get('payload', {}).get('requestId'),
            "responseHeader": {
                "timestamp": request.json.get('payload', {}).get('timestamp'),
                "resultCode": "401",
                "resultMessage": "Wrong credentials"
            }
        }
        return jsonify(response), 401


if __name__ == '__main__':
    # run app in debug mode on port 5000
    app.run(port=5000, host='0.0.0.0', debug=True)
