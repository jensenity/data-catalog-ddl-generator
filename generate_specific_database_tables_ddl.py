import time
import boto3
import pandas as pd

class QueryAthena:

    def __init__(self, query, database, query_output_bucket):
        self.database = database
        self.folder = 'temp/ddl/'
        self.bucket = query_output_bucket
        self.s3_output =  's3://' + self.bucket + '/' + self.folder
        self.query = query

    def load_conf(self, q):
        try:
            self.client = boto3.client('athena')
            response = self.client.start_query_execution(
                QueryString = q,
                    QueryExecutionContext={
                    'Database': self.database
                    },
                    ResultConfiguration={
                    'OutputLocation': self.s3_output,
                    }
            )
            self.filename = response['QueryExecutionId']
            print('Execution ID: ' + response['QueryExecutionId'])

        except Exception as e:
            print(e)
        return response                

    def run_query(self):
        queries = [self.query]
        for q in queries:
            res = self.load_conf(q)
        try:              
            query_status = None
            while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
                query_status = self.client.get_query_execution(QueryExecutionId=res["QueryExecutionId"])['QueryExecution']['Status']['State']
                print(query_status)
                if query_status == 'FAILED' or query_status == 'CANCELLED':
                    raise Exception('Athena query with the string "{}" failed or was cancelled'.format(self.query))
                time.sleep(10)
            print('Query "{}" finished.'.format(self.query))

            create_ddl = self.obtain_data()
            self.clean_up()
            return create_ddl
            
        except Exception as e:
            print(e)      

    def obtain_data(self):
        try:
            self.resource = boto3.resource('s3')

            response = self.resource \
            .Bucket(self.bucket) \
            .Object(key= self.folder + self.filename + '.txt') \
            .get()
            
            ddl = (response['Body'].read()).decode('utf-8')
            return ddl
        except Exception as e:
            print(e)  

    def clean_up(self):
        self.s3 = boto3.resource('s3')
        bucket = self.s3.Bucket(self.bucket)
        for obj in bucket.objects.filter(Prefix='Query-Results/'):
            self.s3.Object(bucket.name,obj.key).delete()

        for obj in bucket.objects.filter(Prefix='temp/ddl'):
            self.s3.Object(bucket.name, obj.key).delete()

if __name__ == "__main__":
    database = input("Enter Database Name ")
    output_bucket = input("Enter Query Output Bucket Name ")
    client = boto3.client('glue')
    try:
        responseGetTables = client.get_tables(DatabaseName=database)
        tableList = responseGetTables['TableList']
        for tableDict in tableList:
            TableName = tableDict['Name']
            query = f"SHOW CREATE TABLE {database}.{TableName};"
            qa = QueryAthena(query=query, query_output_bucket=output_bucket, database=database)
            ddl = qa.run_query()
            print(ddl)
    except Exception as e:
        print(e)  
