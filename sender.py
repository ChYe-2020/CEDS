# python 3.6
import json
import random
import string
import time
import sys
import pickle
from paho.mqtt import client as mqtt_client
import requests
from time_to_slot import time_to_timeslot


args = sys.argv

time_slot = 60
sleep_time = 6

# node_name = 'node_0_0'
broker = "localhost"
# broker = "192.168.1.12"
port = 1883

topic = "taxi"
client_id='edge'
# #读取所有数据
with open(f'./files/processed_data.dump', 'rb') as f:
    message_list = pickle.load(f)
# # 对lon lat 做划分，并把数据分到每个区块的边缘节点上
lon_min = -8.7
lon_max = -8.5

lon_interval_num = 4
lon_interval = (lon_max-lon_min )/lon_interval_num

lat_max = 41.22
lat_min = 41.13
lat_interval_num = 3
lat_interval = (lat_max- lat_min) / lat_interval_num

# 实时记录
parentUrl = "http://10.180.132.116:18880"


def realtime_update_index(taxi_id,node_name, timestamp,endState):
    body = {
        "vehID": taxi_id,
        "node": node_name,
        "timeslot": time_to_timeslot(timestamp),
        "startTime":timestamp,
        "endState":endState
    }
    res = requests.post(f'{parentUrl}/statistic/realtime', json=body)



def connect_mqtt():
    def on_connect(self,client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    client = mqtt_client.Client(mqtt_client.CallbackAPIVersion.VERSION2,client_id)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


isIN= {}
#isIN=[]

# 模拟发消息
def publish(client):
    # 从前往后遍历，index是当前遍历的位置
    index = 0
    time_start = message_list[0]["TIMESTAMP"]

    # pre_node_name = "null"

    while index < len(message_list):
        msg = message_list[index]



        now_time = msg["TIMESTAMP"]
        # 每一个time_slot的周期sleep一下，防止一次发太多接收端卡了
        if now_time - time_start > time_slot:
            time_start = time_slot + time_start
            time.sleep(sleep_time)
            print(f'now_time:{time_start}, first_message_time:{now_time}')
            continue

        topic_name = f'{topic}/{msg["TAXI_ID"]}'
        # heart beat, but not used in test bench
        # client.publish("online", json.dumps([rand_topic]), qos=1, retain=False)
        # client.loop()
        print(msg)

        ######################################
        lat = msg["LATITUDE"]
        lon = msg["LONGITUDE"]
        if lat < lat_min or lat > lat_max or lon < lon_min or lon > lon_max:
            index+=1
            continue
        lat_slot = int((lat - lat_min) / lat_interval)
        lon_slot = int((lon - lon_min) / lon_interval)
        node_name = f'node_{lat_slot}_{lon_slot}'

        if msg["TAXI_ID"] not in isIN.keys():
            isIN[msg["TAXI_ID"]]=node_name
            realtime_update_index(msg["TAXI_ID"], node_name, msg["TIMESTAMP"], "no")
        else:
            if node_name!=isIN[msg["TAXI_ID"]]:
                realtime_update_index(msg["TAXI_ID"], isIN[msg["TAXI_ID"]], msg["TIMESTAMP"], "leave")
                realtime_update_index(msg["TAXI_ID"], node_name, msg["TIMESTAMP"], "no")
                isIN[msg["TAXI_ID"]] = node_name
        # 发消息，qos=1和loop是为了避免消息丢失和重复消费
        client.publish(topic_name, json.dumps(msg), qos=1, retain=False)
        client.loop()
        index += 1



def run():
    client = connect_mqtt()
    publish(client)


if __name__ == '__main__':
    run()

