from pyspark.sql import SparkSession
from pyspark.sql.functions import col, asc, desc, collect_list, count, when, isnull, isnan, size, to_json, from_json
import sys
import requests
from flask import Flask, request, jsonify, make_response
from pyspark.sql.types import MapType, StringType, StructType, StructField, IntegerType, DoubleType

app = Flask(__name__)


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

#Datframe for MessageType and Count
dfAequitasGroupByMessageType = df.groupBy(col('MessageType')).count()
dfAequitasGroupByMessageType.printSchema()
dfAequitasGroupByMessageType.show()


#API
@app.route('/getByMessageType')
def send_to_api():
    msg_count = []
    msg_type = []
    for t in range(len(dfAequitasGroupByMessageType.select("count").collect())):
        msg_count.append(dfAequitasGroupByMessageType.select("count").collect()[t][0])
    for t in range(len(dfAequitasGroupByMessageType.select("MessageType").collect())):
        msg_type.append(dfAequitasGroupByMessageType.select("MessageType").collect()[t][0])
    print(msg_type)
    print(msg_count)
    json_data = [dict(zip(msg_type, msg_count))]
    return jsonify(json_data)