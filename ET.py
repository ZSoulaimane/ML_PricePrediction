from pyspark.sql import SparkSession
import pyspark.sql.functions as func
import os
import json
import requests


class ET(object):

    def __init__(self):

        super().__init__()
        self.spark = SparkSession.builder.getOrCreate()


#------------------------------------------------  Clean (transform) ----------------------------------------------------------


    def clean(self, currency):

        rawDF = self.spark.read.json("currencies/{}.json".format(currency), multiLine = "true")


        unique_df = rawDF.withColumn("id", func.monotonically_increasing_id())



        new_df = unique_df.select("id", "open", "close", "high", "low", "volume", "time")



        df = new_df.withColumn("open", func.round(new_df["open"], 2))\
            .withColumn("close", func.round(new_df["close"], 2))\
                .withColumn("high", func.round(new_df["high"], 2))\
                    .withColumn("low", func.round(new_df["low"], 2))\
                        .withColumn("volume", func.round(new_df["volume"], 2))



        df = df.select("id", "open", "close", "high", "low", "volume", "time")\
            .withColumn('time', func.regexp_replace(func.col('time'), "([A-Z]).*", ""))



        # df = df.select("id", "open", "close", "high", "low", "volume",\
        #     func.split(func.col("time"),"-").getItem(2).alias("Day"),
        #     func.split(func.col("time"),"-").getItem(1).alias("Month"),
        #     func.split(func.col("time"),"-").getItem(0).alias("Year"))


        if not os.path.exists('CSVs'): os.makedirs('CSVs')


        dataframe = df.toPandas()
        dataframe.to_csv('CSVs\{}.csv'.format(currency), index=False)
    

#------------------------------------------------   extract  ----------------------------------------------------------


    def extract(self, currency, date, limit):

        #example Currency Symbol : BTC
        #example date : 2021-09-07
        #example limit = 30

        if not os.path.exists('currencies'): os.makedirs('currencies')

        try :
            with open("currencies\{}.json".format(currency),'w+') as file:


                url = 'http://api.bitdataset.com/v1/ohlcv/latest/BITFINEX:{}USD?period=D1&from={}&limit={}'\
                    .format(currency, date, limit)
                
                headers = {'apikey' : '0432d620-26dc-4e6d-b6bb-d165de9162c9'}


                response = requests.get(url, headers=headers)


                data = json.loads(response.content)
                newdata = json.dumps(data)


                file.write(str(newdata))
        
        except Exception as ErrorMsg:
            print(ErrorMsg)



if __name__ == "__main__":

    newO = ET()

    #newO.extract("ETH", "2021-09-07", "600")

    newO.clean("ETH")