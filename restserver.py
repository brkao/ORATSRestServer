import datetime
import os
import boto3
import botocore.response

from flask import Flask
from flask import request

ORATS_BUCKET_NAME       = "orats-snapshots"

columns = ["ticker",
    "tradeDate",
    "expirDate",
    "dte",
    "strike",
    "stockPrice",
    "callVolume",
    "callOpenInterest",
    "callBidSize",
    "callAskSize",
    "putVolume",
    "putOpenInterest",
    "putBidSize",
    "putAskSize",
    "callBidPrice",
    "callValue",
    "callAskPrice",
    "putBidPrice",
    "putValue",
    "putAskPrice",
    "callBidIv",
    "callMidIv",
    "callAskIv",
    "smvVol",
    "putBidIv",
    "putMidIv",
    "putAskIv",
    "residualRate",
    "delta",
    "gamma",
    "theta",
    "vega",
    "rho",
    "phi",
    "driftlessTheta",
    "callSmvVol",
    "putSmvVol",
    "extSmvVol",
    "extCallValue",
    "extPutValue",
    "spotPrice",
    "quoteDate",
    "updatedAt",
    "snapShotEstTime",
    "snapShotDate",
    "expiryTod",
]

app = Flask(__name__)

@app.route('/ORATSGet', methods=['GET'])
def orats_get():
    timeframe = request.args.get('timeframe')
    ticker = request.args.get('ticker')
    date = request.args.get('date')
    
    if timeframe not in {"1", "5", "10", "30", "60", "120", "240"}:
        return {
            'statusCode': 400,
            'body': "Invalid timeframe input"
        }
    if ticker is None:
        return {
            'statusCode': 400,
            'body': "Invalid ticker input"
        }
    if date is None:
        return {
            'statusCode': 400,
            'body': "Invalid date input"
        }

    print("Looking up ORATS date %s ticker %s timeframe %s" % (date, ticker, timeframe))

    access_key = os.environ.get('ACCESS_KEY')
    secret_key = os.environ.get('SECRET_KEY')
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key)
    s3 = session.client('s3')

    keys = list()
    for key in s3.list_objects(Bucket=ORATS_BUCKET_NAME, Prefix=date)['Contents']:
        tokens = key['Key'].split('/')
        if tokens[0] == date:
            keys.append(key['Key'])
    print("Found %d keys in bucket" % len(keys))
    if len(keys) == 0:
        return {
            'statusCode': 200,
            'body': "No Data Found For Date " + date
        }

    start = datetime.datetime.now()

    api_resp = list()
    process_count = 0

    for key in keys:
        filename = os.path.splitext(key)[0]
        filename = os.path.splitext(filename)[0]
        ts = filename.split('_')[1]

        if int(ts) % int(timeframe) != 0:
            continue
        print("Processing ", process_count, key)
        process_count += 1
        resp = s3.select_object_content(
            Bucket=ORATS_BUCKET_NAME,
            Key=key,
            ExpressionType='SQL',
            Expression="SELECT * FROM s3object s where s.ticker = '" + ticker + "'",
            InputSerialization = {'CSV': {"FileHeaderInfo": "Use"}, 'CompressionType': 'GZIP'},
            OutputSerialization ={'CSV': {}},
        )

        for event in resp['Payload']:
            if 'Records' in event:
                records = event['Records']['Payload'].decode('utf-8')
                records_list = records.rstrip().split("\r\n")
                api_resp = api_resp + records_list
            elif 'Stats' in event:
                statsDetails = event['Stats']['Details']
                #print("Bytes scanned ", statsDetails['BytesScanned'])

    print(api_resp)
    end = datetime.datetime.now()
    print(end - start)
    return {
        'statusCode': 200,
        'count': len(api_resp),
        'columns' : columns,
        'rows': api_resp
    }

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
