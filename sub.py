import json
import random
import time
import math
import threading
from paho.mqtt import client as mqtt_client
from rosettaFilter import RosettaFilter
import sys
from sys import stdin
import pymongo
import requests
from time_to_slot import time_to_timeslot

bloomFilterLength = 256
#
# # # 传入配置项
# # if __name__ == '__main__':
# #     port = 1883
# #     args = sys.argv
# #     # topic base url
# #     print(args)
# #     node_name = args[1]
# #     parentUrl = args[2]
# #     mongo_url = args[3]
# #     broker = "localhost"
# #     client_id = f'sinkWorker{node_name}'
#
# # 如果要用client_track传元数据得用这个
# # std_read = stdin.readline()
# # print(std_read)
# #
# # parse_config = json.loads(std_read)
#
# # 直接跑sub的话就不用上面的
# parse_config = {
#     "topicPrefix": "taxi",
#     "collectionName": "taxi",
#     "payloadType": "json",
#     "timeField": "TIMESTAMP",
#     "fields": {
#         "TAXI_ID":
#             {
#                 "name": "TAXI_ID",
#                 "type": "str",
#             },
#         "TRIP_ID":
#             {
#                 "name": "TRIP_ID",
#                 "type": "str",
#             },
#         "LONGITUDE":
#             {
#                 "name": "LONGITUDE",
#                 "type": "float",
#                 "min": -180,
#                 "max": 180,
#                 "granularity": 0.001
#             },
#         "SPEED":
#             {
#                 "name": "SPEED",
#                 "type": "float",
#                 "min": 0,
#                 "max": 120,
#                 "granularity": 1
#             },
#         "LATITUDE":
#             {
#                 "name": "LATITUDE",
#                 "type": "float",
#                 "min": -180,
#                 "max": 180,
#                 "granularity": 0.001
#             }
#     },
#     "batchWindow": 10,  # 每多少秒flush一下,
#     "ttl": 0
# }
#
# parse_config_key_info = parse_config["fields"]
#
# # 监听设置
# listen_topic_str = parse_config["topicPrefix"] + '/#'
# topic_label = "topic"
#
# aggregateTime = 6
#
# #连接mongodb,创建表
# def get_db(table_name):
#     # mongo_url = mongo_url
#     mongo_client = pymongo.MongoClient(mongo_url)
#     database = mongo_client["mqtt"]
#     collection = database[table_name]
#     # 创建索引，如果不存在
#     index_name = 'custom'
#     index_list_init = [("topic", 1), ("time", 1), ]
#     for collection_key in parse_config["fields"].keys():
#         index_list_init.append((collection_key, 1))
#
#     if "custom" in collection.index_information().keys():
#         print(collection.index_information())
#     else:
#         # print("create mongo index")
#         collection.create_index(index_list_init, name="custom")
#     return collection
# # # 如果有定期删除清理需求会建立ttl索引
# # if "ttl" not in collection.index_information().keys() and "ttl" in parse_config and parse_config["ttl"]!=0:
# #     collection.create_index([("time", 1)],name="ttl", expireAfterSeconds=parse_config["ttl"])
#
# # 数据分片
# def time_to_timeslot(timestamp):
#     # MILLISECOND = 1
#     # SECOND = 1000 * MILLISECOND
#     SECOND = 1
#     MINUTE = 60 * SECOND
#     HOUR = 60 * MINUTE
#     DAY = 24 * HOUR
#     YEAR = 356 * DAY
#     TimeInterVal = 20 * MINUTE
#     MinimumTime = 40 * YEAR
#     return math.floor((timestamp - MinimumTime) / TimeInterVal)
#
# # 实时索引，暂时不用
# def realtime_update_index(stream_topic, timestamp):
#     body = {
#         "tc": stream_topic,
#         "nd": node_name,
#         "ts": time_to_timeslot(timestamp)
#     }
#     # res = requests.post(f'{parentUrl}/statistic/realtime', json=body)
#
# # 记录一个时间槽内新写入消息的摘要信息，用rosetta过滤器
# class AnalyticDataPerTimeSlot:
#     # 初始化各个字段的rosetta过滤器
#     def __init__(self):
#         self.keysInfo = {}
#         # topic是固定有的列，代表资源地址
#         self.keysInfo["topic"] = RosettaFilter(bits=bloomFilterLength,
#                                                split_char="/",
#                                                str_filter_length=4,
#                                                type="str")
#         # 根据配置项里面的字段来创建过滤器
#         for collection_key in parse_config["fields"].keys():
#             value = parse_config["fields"][collection_key]
#             if value["type"] == "str":
#                 self.keysInfo[collection_key] = RosettaFilter(bits=bloomFilterLength,
#                                                              split_char="/",
#                                                              str_filter_length=1,
#                                                              type="str")
#             else:
#                 self.keysInfo[collection_key] = RosettaFilter(bits=bloomFilterLength,
#                                                              value_min=value["min"],
#                                                              value_max=value["max"],
#                                                              granularity=value["granularity"])
#     # 更新rosetta过滤器
#     def update(self, payload, topic):
#         prefix = parse_config["topicPrefix"]
#         topic_tail = topic[len(prefix):]
#         self.keysInfo["topic"].add_str(topic_tail)
#         # 遍历payload的每个字段去插入相应的过滤器
#         for key in self.keysInfo.keys():
#             if key not in payload or key == "topic":
#                 continue
#             value = payload[key]
#
#             analytic_value = self.keysInfo[key]
#             if parse_config["fields"][key]["type"] == "str":
#                 analytic_value.add_str(value)
#             else:
#                 analytic_value.add_int(value)
#
#     def dump_json(self):
#         return {
#             key: val.to_base64() for (key, val) in self.keysInfo.items()
#         }
#
#
# class DataBuffer:
#     # buffer里面有数据和数据的摘要信息
#     buffer_lock = threading.Lock()
#     topic_list = {}
#     analytic_data_per_timeslot = {}
#     flush_count = 0
#     buffer = {}
#
#     def addAnalyzeData(self, payload):
#         timestamp = payload["time"]
#         timeslot_index = time_to_timeslot(timestamp)
#         if timeslot_index not in self.analytic_data_per_timeslot.keys():
#             self.analytic_data_per_timeslot[timeslot_index] = AnalyticDataPerTimeSlot()
#         analytic_data = self.analytic_data_per_timeslot[timeslot_index]
#         stream_topic = payload['topic']
#         # 这里是更新实时索引的，不需要
#         # if stream_topic not in self.topic_list:
#         #     self.topic_list[stream_topic] = 1
#         #     realtime_update_index(stream_topic, timestamp)
#         analytic_data.update(payload, stream_topic)
#
#     def addBufferData(self, payload):
#         timestamp = payload["time"]
#         timeslot_index = time_to_timeslot(timestamp)
#         table_name = parse_config["collectionName"] + timeslot_index
#         if table_name not in self.buffer.keys():
#             self.buffer[table_name] = []
#         self.buffer[table_name].append(payload)
#
#     # 将数据插入到数据缓存和摘要信息里
#     def add_record(self, parse_data):
#         with self.buffer_lock:
#             self.addAnalyzeData(parse_data)
#             self.addBufferData(parse_data)
#
#     # 写数据库
#     def flush_buffer(self):
#         with self.buffer_lock:
#             for i in self.buffer.keys():
#                 get_db(i).insert_many(self.buffer[i])
#                 self.buffer = {}
#
#     def fake_flush_buffer(self):
#         self.buffer = {}
#
#     def flush_analytic_data(self):
#         with self.buffer_lock:
#             data = []
#             for timeslot in self.analytic_data_per_timeslot.keys():
#                 data.append({
#                     "node": node_name,
#                     "shard": parse_config["collectionName"] +str(timeslot),
#                     # "streamGroup": parse_config["topicPrefix"],
#                     # "timeslot": timeslot,
#                     "analyticData": self.analytic_data_per_timeslot[timeslot].dump_json()
#                 })
#             # 没插入过数据就不flush
#             if len(data) != 0:
#                 res = requests.post(f'{parentUrl}/statistic/longTimeStatistic', json=data)
#             self.analytic_data_per_timeslot = {}
#             self.topic_list = {}
#
#     def fake_flush(self):
#         self.analytic_data_per_timeslot = {}
#         self.topic_list = {}
#
# buffer = DataBuffer()
#
#
# # 监听mqtt
# def connect_mqtt() -> mqtt_client:
#     def on_connect(client, userdata, flags, rc):
#         if rc == 0:
#             print("Connected to MQTT Broker!")
#         else:
#             print("Failed to connect, return code %d\n", rc)
#
#     client = mqtt_client.Client(client_id, clean_session=False)
#     client.on_connect = on_connect
#     client.connect(broker, port)
#     return client
#
#
# def subscribe(client: mqtt_client, data_buffer: DataBuffer):
#     def parse_payload(msg):
#         raw_data = msg.payload.decode()
#         payload = json.loads(raw_data)
#         obj = {
#             "topic": msg.topic,
#             "payload": raw_data
#         }
#         # 解析消息
#         if "timeField" in parse_config.keys():
#             obj["time"] = int(payload[parse_config["timeField"]])
#         else:
#             obj["time"] = int(round(time.time()))
#         for key in parse_config_key_info.keys():
#             data_type = parse_config_key_info[key]['type']
#             value = payload[key]
#             if data_type == "float":
#                 obj[key] = float(value)
#             elif data_type == "int":
#                 obj[key] = int(value)
#             elif data_type == "str":
#                 obj[key] = str(value)
#         return obj
#
#     def on_message(client, userdata, msg):
#
#         print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
#         parse_data = parse_payload(msg)
#         data_buffer.add_record(parse_data)
#
#     client.subscribe(listen_topic_str, qos=1)
#     client.on_message = on_message
#
#
# def sub():
#     client = connect_mqtt()
#     subscribe(client, buffer)
#     client.loop_forever()
#
#
# def batch_flush():
#     while True:
#         time.sleep(parse_config["batchWindow"])
#         buffer.fake_flush()
#         # buffer.flush_buffer()
#
#
# def aggregate_flush():
#     while True:
#         time.sleep(aggregateTime)
#         print("start flush aggregate")
#         buffer.flush_analytic_data()
#
# # 多线程
# if __name__ == '__main__':
#     port = 1883
#     args = sys.argv
#     # topic base url
#     print(args)
#     node_name = args[1]
#     parentUrl = args[2]
#     mongo_url = args[3]
#     broker = "localhost"
#     client_id = f'sinkWorker{node_name}'
#
#     t1 = threading.Thread(target=sub)
#     t1.start()
#     t2 = threading.Thread(target=batch_flush)
#     t2.start()
#     t3 = threading.Thread(target=aggregate_flush)
#     t3.start()





# # 传入配置项
# if __name__ == '__main__':
#     port = 1883
#     args = sys.argv
#     # topic base url
#     print(args)
#     node_name = args[1]
#     parentUrl = args[2]
#     mongo_url = args[3]
#     broker = "localhost"
#     client_id = f'sinkWorker{node_name}'

# 如果要用client_track传元数据得用这个
# std_read = stdin.readline()
# print(std_read)
#
# parse_config = json.loads(std_read)

# 直接跑sub的话就不用上面的
parse_config = {
    "topicPrefix": "taxi",
    "collectionName": "taxi",
    "payloadType": "json",
    "timeField": "TIMESTAMP",
    "fields": {
        "TAXI_ID":
            {
                "name": "TAXI_ID",
                "type": "str",
            },
        "TRIP_ID":
            {
                "name": "TRIP_ID",
                "type": "str",
            },
        "LONGITUDE":
            {
                "name": "LONGITUDE",
                "type": "float",
                "min": -180,
                "max": 180,
                "granularity": 0.001
            },
        "SPEED":
            {
                "name": "SPEED",
                "type": "float",
                "min": 0,
                "max": 120,
                "granularity": 1
            },
        "LATITUDE":
            {
                "name": "LATITUDE",
                "type": "float",
                "min": -180,
                "max": 180,
                "granularity": 0.001
            }
    },
    "batchWindow": 10,  # 每多少秒flush一下,
    "ttl": 0
}


##################################
lon_min = -8.7
lon_max = -8.5

lon_interval_num = 4
lon_interval = (lon_max-lon_min )/lon_interval_num

lat_max = 41.22
lat_min = 41.13
lat_interval_num = 3
lat_interval = (lat_max- lat_min) / lat_interval_num
##########################################

parse_config_key_info = parse_config["fields"]

# 监听设置
listen_topic_str = parse_config["topicPrefix"] + '/#'
topic_label = "topic"

aggregateTime = 6


message_per_node_list = [[[] for i in range(lon_interval_num)] for j in range(lat_interval_num)]
# 连接mongodb,创建表
def get_db(node_name,table_name,data):
    # mongo_url = mongo_url

    mongo_url_port=int(node_name[5])*4+int(node_name[7])+28000
    mongo_url_node = mongo_url + ":" + str(mongo_url_port)
    mongo_client = pymongo.MongoClient(mongo_url_node)

    # mongo_client = pymongo.MongoClient(mongo_url)
    database = mongo_client["mqtt"]
    collection = database[table_name]
    # 创建索引，如果不存在
    index_name = 'custom'
    index_list_init = [("topic", 1), ("time", 1), ]
    for collection_key in parse_config["fields"].keys():
        index_list_init.append((collection_key, 1))

    if "custom" in collection.index_information().keys():
        print(collection.index_information())
    else:
        # print("create mongo index")
        collection.create_index(index_list_init, name="custom")
    return collection


# # 如果有定期删除清理需求会建立ttl索引
# if "ttl" not in collection.index_information().keys() and "ttl" in parse_config and parse_config["ttl"]!=0:
#     collection.create_index([("time", 1)],name="ttl", expireAfterSeconds=parse_config["ttl"])






# 记录一个时间槽内新写入消息的摘要信息，用rosetta过滤器
class AnalyticDataPerTimeSlot:
    # 初始化各个字段的rosetta过滤器
    def __init__(self):
        self.keysInfo = {}
        # topic是固定有的列，代表资源地址
        self.keysInfo["topic"] = RosettaFilter(bits=bloomFilterLength,
                                               split_char="/",
                                               str_filter_length=4,
                                               type="str")
        # 根据配置项里面的字段来创建过滤器
        for collection_key in parse_config["fields"].keys():
            value = parse_config["fields"][collection_key]
            if value["type"] == "str":
                self.keysInfo[collection_key] = RosettaFilter(bits=bloomFilterLength,
                                                              split_char="/",
                                                              str_filter_length=1,
                                                              type="str")
            else:
                self.keysInfo[collection_key] = RosettaFilter(bits=bloomFilterLength,
                                                              value_min=value["min"],
                                                              value_max=value["max"],
                                                              granularity=value["granularity"])

    # 更新rosetta过滤器
    def update(self, payload, topic):
        prefix = parse_config["topicPrefix"]
        topic_tail = topic[len(prefix):]
        self.keysInfo["topic"].add_str(topic_tail)
        # 遍历payload的每个字段去插入相应的过滤器
        for key in self.keysInfo.keys():
            if key not in payload or key == "topic":
                continue
            value = payload[key]

            analytic_value = self.keysInfo[key]
            if parse_config["fields"][key]["type"] == "str":
                analytic_value.add_str(value)
            else:
                analytic_value.add_int(value)

    def dump_json(self):
        return {
            key: val.to_base64() for (key, val) in self.keysInfo.items()
        }


class DataBuffer:
    # buffer里面有数据和数据的摘要信息
    buffer_lock = threading.Lock()
    topic_list = {}
    analytic_data_per_timeslot = {}
    message_per_node_list = [[[] for i in range(lon_interval_num)] for j in range(lat_interval_num)]
    node_analytic_data_per_timeslot={}
    flush_count = 0
    buffer = {}

    def addAnalyzeData(self, payload):
        timestamp = payload["time"]
        timeslot_index = time_to_timeslot(timestamp)
        if timeslot_index not in self.analytic_data_per_timeslot.keys():
            self.analytic_data_per_timeslot[timeslot_index] = AnalyticDataPerTimeSlot()
        analytic_data = self.analytic_data_per_timeslot[timeslot_index]
        stream_topic = payload['topic']
        analytic_data.update(payload, stream_topic)
        ######################################################
        lat = payload["LATITUDE"]
        lon = payload["LONGITUDE"]
        lat_slot = int((lat - lat_min) / lat_interval)
        lon_slot = int((lon - lon_min) / lon_interval)
        self.node_analytic_data_per_timeslot[f'{lat_slot}{lon_slot}']=self.analytic_data_per_timeslot
        #############################################################

    def addBufferData(self, payload):
        timestamp = payload["time"]
        timeslot_index = time_to_timeslot(timestamp)
        table_name = parse_config["collectionName"] + str(timeslot_index)
        if table_name not in self.buffer.keys():
            self.buffer[table_name] = []
        self.buffer[table_name].append(payload)

    # 将数据插入到数据缓存和摘要信息里
    def add_record(self, parse_data):
        with self.buffer_lock:
            self.addAnalyzeData(parse_data)
            self.addBufferData(parse_data)

    # 写数据库
    # def flush_buffer(self):
    #     with self.buffer_lock:
    #         for i in self.buffer.keys():
    #             get_db(i).insert_many(self.buffer[i])
    #             self.buffer = {}
    def flush_buffer(self):
        with self.buffer_lock:
            for i in self.buffer.keys():
                # get_db(i, self.buffer.get(i)).insert_many(self.buffer[i])
                # self.buffer = {}


                ###############################################
                for message in self.buffer.get(i):
                    lat = message["LATITUDE"]
                    lon = message["LONGITUDE"]
                    if lat < lat_min or lat > lat_max or lon < lon_min or lon > lon_max:
                        continue
                    lat_slot = int((lat - lat_min) / lat_interval)
                    lon_slot = int((lon - lon_min) / lon_interval)
                    self.message_per_node_list[lat_slot][lon_slot].append(message)
            for i in range(lat_interval_num):
                for j in range(lon_interval_num):
                    node_name=f'node_{i}_{j}'
                    temp_data = {}
                    for msg in self.message_per_node_list[i][j]:
                        timestamp = msg["time"]
                        timeslot_index = time_to_timeslot(timestamp)
                        table_name = parse_config["collectionName"] + str(timeslot_index)
                        if table_name not in temp_data.keys():
                            temp_data[table_name] = []
                        temp_data[table_name].append(msg)
                        print(table_name,'\n\n')
                    for k in temp_data.keys():

                        result =get_db(node_name,k, temp_data.get(i)).insert_many(temp_data.get(i))

                        print(result)
            self.buffer={}
                ####################################################



    def fake_flush_buffer(self):
        self.buffer = {}

    def flush_analytic_data(self):
        with self.buffer_lock:
            for i in self.node_analytic_data_per_timeslot.keys():
                node_name=i
                data = []
                temp_analytic_data_per_timeslot=self.node_analytic_data_per_timeslot.get(i)
                for timeslot in temp_analytic_data_per_timeslot.keys():
                    data.append({
                        "node": node_name,
                        "shard": parse_config["collectionName"] + str(timeslot),
                        # "streamGroup": parse_config["topicPrefix"],
                        # "timeslot": timeslot,
                        "analyticData": temp_analytic_data_per_timeslot[timeslot].dump_json()
                    })
                # 没插入过数据就不flush
                if len(data) != 0:
                    res = requests.post(f'{parentUrl}/statistic/longTimeStatistic', json=data)
                    print(temp_analytic_data_per_timeslot[timeslot])
            self.analytic_data_per_timeslot = {}
            self.topic_list = {}

    def fake_flush(self):
        self.analytic_data_per_timeslot = {}
        self.topic_list = {}


buffer = DataBuffer()


# 监听mqtt
def connect_mqtt() -> mqtt_client:
    def on_connect(self,client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2,client_id, clean_session=False)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def subscribe(client: mqtt_client, data_buffer: DataBuffer):
    def parse_payload(msg):
        raw_data = msg.payload.decode()
        payload = json.loads(raw_data)
        obj = {
            "topic": msg.topic,
            "payload": raw_data
        }
        # 解析消息
        if "timeField" in parse_config.keys():
            obj["time"] = int(payload[parse_config["timeField"]])
        else:
            obj["time"] = int(round(time.time()))
        for key in parse_config_key_info.keys():
            data_type = parse_config_key_info[key]['type']
            value = payload[key]
            if data_type == "float":
                obj[key] = float(value)
            elif data_type == "int":
                obj[key] = int(value)
            elif data_type == "str":
                obj[key] = str(value)
        return obj

    def on_message(client, userdata, msg):

        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
        parse_data = parse_payload(msg)
        data_buffer.add_record(parse_data)

    client.subscribe(listen_topic_str, qos=1)
    client.on_message = on_message


def sub():
    client = connect_mqtt()
    subscribe(client, buffer)
    client.loop_forever()


def batch_flush():
    while True:
        time.sleep(parse_config["batchWindow"])
        # buffer.fake_flush()
        buffer.flush_buffer()


def aggregate_flush():
    while True:
        time.sleep(aggregateTime)
        print("start flush aggregate")
        buffer.flush_analytic_data()


# 多线程
if __name__ == '__main__':
    port = 1883
    # args = sys.argv
    # # topic base url
    # print(args)
    # node_name = "node0_0"
    parentUrl = "http://10.180.132.116:18880"
    # mongo_url = "mongodb://0.0.0.0:28000"
    mongo_url = "mongodb://0.0.0.0"
    broker = "localhost"
    # client_id = f'sinkWorker{node_name}'
    client_id='sinkWorkeredge'

    t1 = threading.Thread(target=sub)
    t1.start()
    t2 = threading.Thread(target=batch_flush)
    t2.start()
    t3 = threading.Thread(target=aggregate_flush)
    t3.start()


