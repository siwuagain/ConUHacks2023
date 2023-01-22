from pyspark.sql import SparkSession
from pyspark.sql.functions import col, asc, desc, collect_list, count, when, isnull, isnan, size, to_json, from_json, \
    second, from_unixtime, unix_timestamp
from flask_cors import CORS
import sys
import requests
from flask import Flask, request, jsonify, make_response
import numpy as np
from pyspark.sql.types import MapType, StringType, StructType, StructField, IntegerType, DoubleType

app = Flask(__name__)

CORS(app, resources={r"/*": {"origins": "*"}})

ss = SparkSession.builder.master("local[4]").appName("nb_data").getOrCreate();
sc = ss.sparkContext

# Read combined files
df = ss.read.option("multiline", "true").json(["Hackathon_data/AequitasData.json", "Hackathon_data/TSXData.json", "Hackathon_data/AlphaData.json"]).cache()

#Print schema
df.printSchema()

#Print all
df.show()

# # Define custom schema
# schema = StructType([
#             StructField("Direction",StringType(),True),
#             StructField("Exchange",StringType(),True),
#             StructField("MessageType", StringType(), True),
#             StructField("OrderID", StringType(), True),
#             StructField("OrderPrice", DoubleType(), True),
#             StructField("Symbol", StringType(), True),
#             StructField("TimeStamp", StringType(), True),
#             StructField("TimeStampEpoch", StringType(), True),
# ])
#
# dfAequitasWithSchema = ss.read.schema(schema).option("multiline","true").json("Hackathon_data/AequitasData.json").cache()
# dfAequitasWithSchema.printSchema()
# dfAequitasWithSchema.show()
#
# dfAequitas.withColumn("MessageType", to_json(col("MessageType"))).show()
#
# dfAequitasGroupByMessageType = dfAequitas.groupBy(col('MessageType')).count()

df_By_Minute = df.withColumn("Time", from_unixtime(df["TimeStampEpoch"]/1e9,'yyyy-MM-dd HH:mm'))
df_By_Second = df.withColumn("Time", from_unixtime(df["TimeStampEpoch"]/1e9,'yyyy-MM-dd HH:mm:ss'))

#Dataframe for MessageType and Count
df_group_by_messagetype = df.groupBy(col('MessageType')).count()

#Dataframe for Cancelled and Trade
df_group_by_cancelled = df_By_Minute.select("Time", "Exchange", "Symbol", "OrderPrice", "MessageType", "OrderID").filter(df["MessageType"] == "Cancelled").groupBy("Time", "Exchange").count().orderBy('Time')
df_group_by_trade = df_By_Minute.select("Time", "Exchange", "Symbol", "OrderPrice", "MessageType", "OrderID").filter(df["MessageType"] == "Trade").groupBy("Time", "Exchange").count().orderBy('Time')
df_group_by_trade.show()

#API
@app.route('/getByMessageType')
def get_by_message_type():
    msg_count = []
    msg_type = []
    for t in range(len(df_group_by_messagetype.select("count").collect())):
        msg_count.append(df_group_by_messagetype.select("count").collect()[t][0])
    for t in range(len(df_group_by_messagetype.select("MessageType").collect())):
        msg_type.append(df_group_by_messagetype.select("MessageType").collect()[t][0])

    json_data = []
    for t in range(len(msg_count)):
        json_data.append({'MessageType': msg_type[t], 'Count': msg_count[t]})

    return jsonify(json_data)


@app.route('/getCancelledCountByTime')
def get_cancelled_count_by_time():
    msg_count_cancelled = []
    msg_time_cancelled = []
    msg_exchange_cancelled = []
    for t in range(len(df_group_by_cancelled.select("count").collect())):
        msg_count_cancelled.append(df_group_by_cancelled.select("count").collect()[t][0])
    for t in range(len(df_group_by_cancelled.select("Time").collect())):
        msg_time_cancelled.append(df_group_by_cancelled.select("Time").collect()[t][0])
    for t in range(len(df_group_by_cancelled.select("Exchange").collect())):
        msg_exchange_cancelled.append( df_group_by_cancelled.select("Exchange").collect()[t][0])

    msg_count_trade = []
    msg_time_trade = []
    msg_exchange_trade = []
    for t in range(len(df_group_by_cancelled.select("count").collect())):
        msg_count_trade.append(df_group_by_cancelled.select("count").collect()[t][0])
    for t in range(len(df_group_by_cancelled.select("Time").collect())):
        msg_time_trade.append(df_group_by_cancelled.select("Time").collect()[t][0])
    for t in range(len(df_group_by_cancelled.select("Exchange").collect())):
        msg_exchange_trade.append(df_group_by_cancelled.select("Exchange").collect()[t][0])

    TSX_Cancelled = []
    TSX_Trade = []
    Alpha_Cancelled = []
    Alpha_Trade = []
    Aequitas_Cancelled = []
    Aequitas_Trade = []
    for t in range(len(msg_exchange_trade)):
        if msg_exchange_cancelled[t] == "TSX":
            TSX_Cancelled.append({'Time': msg_time_cancelled[t], 'Count': msg_count_cancelled[t]})
        elif msg_exchange_trade[t] == "TSX":
            TSX_Trade.append({'Time': msg_time_trade[t], 'Count': msg_count_trade[t]})
        elif msg_exchange_cancelled[t] == "Alpha":
            Alpha_Cancelled.append({'Time': msg_time_cancelled[t], 'Count': msg_count_cancelled[t]})
        elif msg_exchange_trade[t] == "Alpha":
            Alpha_Trade.append({'Time': msg_time_trade[t], 'Count': msg_count_trade[t]})
        elif msg_exchange_cancelled[t] == "Aequitas":
            Aequitas_Cancelled.append({'Time': msg_time_cancelled[t], 'Count': msg_count_cancelled[t]})
        elif msg_exchange_trade[t] == "Aequitas":
            Aequitas_Trade.append({'Time': msg_time_trade[t], 'Count': msg_count_trade[t]})

    json_data = []
    json_data.append({"Exchange": "TSX", "Trade":TSX_Trade, "Cancelled":TSX_Cancelled})
    json_data.append({"Exchange": "Alpha", "Trade":Alpha_Trade, "Cancelled":Alpha_Cancelled})
    json_data.append({"Exchange": "Aequitas", "Trade":Aequitas_Trade, "Cancelled":Aequitas_Cancelled})

    return jsonify(json_data)