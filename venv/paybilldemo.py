from flask import Flask,request,jsonify
from dotenv import load_dotenv
from markupsafe import escape
import os
import mysql.connector
import hashlib
import json
import base64
import pandas as pd

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

print("mysql Query executed",partneruid)


app = Flask(__name__)
app.json.sort_keys=False


@app.route("/")
def hello_world():
    print()
    return "<p>Hello, World!</p>"

def passCheckMultiBill(username,timestamp,partneruid):
    sample_string=username+str(timestamp)
    password=hashlib.sha512(sample_string.encode('utf-8')).hexdigest()
    password=partneruid+':'+password
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
    print("query"+query)
    mydb=get_connection()
    mycursor = mydb.cursor()
    result=pd.read_sql(query,mydb)
    
    return result

@app.route("/api/values/queryBillInfo",methods=['POST'])
def queryBillInfoApi():
    mydb=get_connection()
    print(request.json['requestBody'])
    print(request.json['requestHeader']['timestamp'],partneruid,apipassword)

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
        
        mycursor = mydb.cursor()
        mycursor.execute(f" select *from paybilltest where billId= {request.json['requestBody']['invoiceId']}")
        print(f" select *from paybilltest where billId= {request.json['requestBody']['invoiceId']}")
        myresult = mycursor.fetchall()
        
        if  myresult :
             print("======")
             billInfo=[]
             
             response['responseHeader']['resultCode']="0"
             response['responseHeader']['resultMessage']="SUCCESS"
             myresult= formatdata(myresult)
             billInfo.append(myresult)
             print(billInfo)
             
             response['billInfo']=billInfo
             mydb.close()
             return jsonify(response)
        else:
            response['responseHeader']['resultMessage']="Not Found"
            response['responseHeader']['resultCode']="401"
            mydb.close()
            
            return jsonify(response)
        mydb.close()        
      
        
    else:
        return "Invalid password"
        mydb.close()
   
    #app.logger.info("/users was requested")
    #app.logger.debug('A value for debugging')
    #app.logger.warning('A warning occurred (%d apples)', 42)
    #app.logger.error('An error occurred')
    #users = ['hassan','hassan','saalah']
 
    #app.logger.error("/users was requested")
    #mycursor = mydb.cursor()
    #test=mycursor.execute("select * from resolvedtransfers limit 1000")
    #myresult = mycursor.fetchall()

def getTrasactionInfo(transactionId,req):
    data=getValue(f"select *from paynotificationbill where tansactionId={transactionId}")
    
    return data




@app.route("/api/values/payBillNotification",methods=['POST'])
def payNotificationBillApi():

    mydb=get_connection()
    print(request.json['requestBody']['transacionInfo'])
    print(request.json)

    ### For resolve bill payment 


    if 'operation' in request.json['requestBody']['transacionInfo']:
        data=getTrasactionInfo(request.json['requestBody']['transacionInfo']['tansactionId'],request.json)
        if data.notna().any(axis=None):
            response={
            "schemaVersion": "1.0",
            "requestId":request.json['requestId'],
            "responseHeader": {
                "timestamp": request.json['requestHeader']['timestamp'],
                "resultCode": "",
                "resultMessage": ""
            }
           
            }
            response['responseHeader']['resultCode']="0"
            response['responseHeader']['resultMessage']="SUCCESS"
            response["confirmationId"]=request.json['requestBody']['transacionInfo']['tansactionId']
            return response
        else:
            response={
            "schemaVersion": "1.0",
            "requestId":request.json['requestId'],
            "responseHeader": {
                "timestamp": request.json['requestHeader']['timestamp'],
                "resultCode": "401",
                "resultMessage": "Not found"
            }
            }
            return response

    
    
    password=passwordcheck(request.json['requestHeader']['timestamp'],partneruid,apipassword)

    if password==request.json['requestHeader']['apiPassword']:
        response={
            "schemaVersion": "1.0",
            "requestId":request.json['requestId'],
            "responseHeader": {
                "timestamp": request.json['requestHeader']['timestamp'],
                "resultCode": "401",
                "resultMessage": "Not found"
            }
           
        }
        

        try : 
            mycursor = mydb.cursor()
            query=f"""INSERT INTO `billingserver`.`paynotificationbill`
                        (
                `invoiceId`,`paidBy`,`paidAt`,`paidDate`,
                `billTo`,`dueDate`,`description`,`billerName`,
                `isPrepaid`,`tansactionId`,`amount`,`currency`)
            VALUES
            ({repr(request.json['requestBody']['billInfo']['invoiceId'])},
                {repr(request.json['requestBody']['billInfo']['paidBy'])},
                {repr(request.json['requestBody']['billInfo']['paidAt'])},
                {repr(request.json['requestBody']['billInfo']['paidDate'])},
                {repr(request.json['requestBody']['billInfo']['billTo'])},
            {repr(request.json['requestBody']['billInfo']['dueDate'])},
            {repr(request.json['requestBody']['billInfo']['description'])},
                {repr(request.json['requestBody']['billInfo']['billerName'])},
                {repr(request.json['requestBody']['billInfo']['isPrepaid'])},
                {repr(request.json['requestBody']['transacionInfo']['tansactionId'])},
                {request.json['requestBody']['transacionInfo']['amount']},
                {repr(request.json['requestBody']['transacionInfo']['currency'])}) 
            """       
  
            print(query)
            mycursor.execute(query)
            
            mydb.commit()
            print(mydb.commit())
            response['responseHeader']['resultCode']="0"
            response['responseHeader']['resultMessage']="SUCCESS"
            response["confirmationId"]=request.json['requestBody']['transacionInfo']['tansactionId']
            return jsonify(response)
        except mysql.connector.errors.Error as ex:
            print(ex)
            response['responseHeader']['resultMessage']="error occured"
            response['responseHeader']['resultCode']="401"
            mydb.close()
              
      
        
    else:
        return "Invalid password"
        mydb.close()
  
    
### Integration for Universities  Start here

def getDecodedData(payload):
    token=payload
    token_payload = token.split(".")[1]
    token_payload_decoded = str(base64.b64decode(token_payload + "=="), "utf-8")
    data = json.loads(token_payload_decoded)
    return data



@app.route("/api/billing/query",methods=['POST'])
def queryBillInfoForMutliBill():
   

    p=passCheckMultiBill(username,request.json['payload']['timestamp'],partneruid_uni)
    print(username,request.json['payload']['timestamp']+partneruid_uni)
    
  
    if p == request.headers['Authorization'] :

        response={
            "schemaVersion": "1.0",
            "requestId":request.json['payload']['requestId'],
            "responseHeader": {
                "timestamp": request.json['payload']['timestamp'],
                "resultCode": "",
                "resultMessage": ""
            }
           
        }
        
        bills=[]
        jsonData=getDecodedData(request.json['payload']['requestBody'])
        query=f'''select studentId,studentName,sum(billAmount) as totalAmount,currencyCode from paybilluniquerybill where studentId={repr(jsonData['studentId'])}  group by studentId,studentName,currencyCode'''
        billTotal=getValue(query)
        query=f"select accountNumber,accountTitle,billAmount,description,dueDate,isPartialPayAllowed  from paybilluniquerybill where studentId={repr(jsonData['studentId'])}"
        print(query)
        data=getValue(query)
        if data.notna().any(axis=None):    
            response['responseHeader']['resultCode']="0"
            response['responseHeader']['resultMessage']="SUCCESS" 
            for i,v in billTotal.iterrows():
                std=v.to_dict()
                response['studentId']=std['studentId']
                response['studentName']=std['studentName']
                response['totalAmount']=std['totalAmount']
                response['currencyCode']=std['currencyCode']
            for index, rows in data.iterrows():
                bills.append(rows.to_dict())
                print(rows.to_dict())
            response['bills']=bills
            return response
        else :
            response['responseHeader']['resultMessage']="error occured"
            response['responseHeader']['resultCode']="401"

    return "ok"
    
@app.route("/api/billing/pay",methods=['POST'])  
def queryInfoForMultiBillPayment():
    p=passCheckMultiBill(username,request.json['payload']['timestamp'],partneruid_uni)
    print(username,request.json['payload']['timestamp']+partneruid_uni)
    print(request.json)
    response={
            "schemaVersion": "1.0",
            "requestId":request.json['payload']['requestId'],
            "responseHeader": {
                "timestamp": request.json['payload']['timestamp'],
                "resultCode": "",
                "resultMessage": ""
            }

        }


    if p == request.headers['Authorization'] :
        mydb=get_connection()
        jsonData=getDecodedData(request.json['payload']['requestBody'])
        print(json.dumps(jsonData))

        response['responseHeader']['resultCode']="0"
        response['responseHeader']['resultMessage']="SUCCESS"
        response["confirmationId"]=jsonData['transacionInfo']['tansactionId']


        for i in range(len(jsonData['bills'])):
            try : 
                mycursor = mydb.cursor()
                query=f"""INSERT INTO `billingserver`.`multibillpayment`
                    (
                    `transactionId`,
                    `studentId`,
                    `studentName`,
                    `totalAmount`,
                    `paidAt`,
                    `dueDate`,
                    `datePaid`,accountNumber,
                    `accountTitle`,
                    `desction`,
                    `billAmount`)
                VALUES
                ({repr(jsonData['transacionInfo']['tansactionId'])},
                    {repr(jsonData['payerInfo']['studentId'])},
                    {repr(jsonData['payerInfo']['studentName'])},
                    {repr(jsonData['transacionInfo']['totalAmount'])},
                    {repr(jsonData['payerInfo']['paidAt'])},
                     {repr(jsonData['bills'][i]['dueDate'])},
                    sysdate(),
                      {repr(jsonData['bills'][i]['accountNumber'])},  
                {repr(jsonData['bills'][i]['accountTitle'])},
                {repr(jsonData['bills'][i]['description'])},
                {repr(jsonData['bills'][i]['billAmount'])}
                ) 
                """       
    
                print(query)
                mycursor.execute(query)
                
                mydb.commit()
                print(mydb.commit())
                response['responseHeader']['resultCode']="0"
                response['responseHeader']['resultMessage']="SUCCESS"
                response["confirmationId"]=jsonData['transacionInfo']['tansactionId']
                print(jsonData['bills'][i])
                
            except mysql.connector.errors.Error as ex:
                print(ex)
                response['responseHeader']['resultMessage']="error occured"
                response['responseHeader']['resultCode']="401"
                mydb.close()








                
        return response
    else:
        return "wrong credentials"


if __name__ == '__main__':
    # run app in debug mode on port 5000
    app.run(port=5000, host='0.0.0.0')



