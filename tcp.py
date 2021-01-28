from pymodbus.client.sync import ModbusTcpClient as ModbusClient 
from pymodbus.constants import Defaults
from abc import ABCMeta, abstractmethod
import csv
from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadDecoder
from pymodbus.payload import BinaryPayloadBuilder
from pymodbus.compat import iteritems
import paho.mqtt.client as mqtt
from collections import OrderedDict
import time
import datetime
import json

#寫檔案
def write_station_bkfile(station:str,data,timestamp):
    # station name, data
    data.append(timestamp)
    filename = "/home/moxa/guanying/csv/" + datetime.datetime.now().strftime("%Y%m%d") +"_" + station + ".csv"
    with open(filename,"a",newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(data)

def write_mqtt_file(data):
    # station name, data
    filename = "/home/moxa/guanying/mqttcsv/" + datetime.datetime.now().strftime("%Y%m%d") +"_" +"mqtt" + ".csv"
    with open(filename,"a",newline="") as csvfile:
        writer = csv.writer(csvfile,delimiter='@')
        writer.writerow(data)

#讀各站寄存器/秒 * N站
def read_station_registers(client,addr,count):
    client.connect()
    if client.is_socket_open():
        try:
            result = client.read_holding_registers(address=addr,count=count,unit=1)
            if result.__dict__.get("registers"):
                data = result.registers
            else:
                data = None
        except:
            print("except")
            client.close()
            data = None
        finally:
            return data
    else:
        data = None
    return data
    # 34站資料


def mqttpub(mqtt_client,topic,values,t,qs=0,rt=True):
    try:
        mqtt_client.connect("3.34.97.49",1883,60)
        # print("PUBLISH MQTT")
        payload={"time":t,"value":values}
        mqtt_client.publish(topic,json.dumps(payload),qs,retain=rt)
    except:
        print("error")
        data = [topic,values,t]
        write_mqtt_file(data)
    # mqtt_client.disconnect()


def execute(client,addr,count,station,timestamp,station_value,min_start):
    st_data = read_station_registers(client,addr,count) # 每秒資料
    if st_data and (time.time()-min_start <60):
        write_station_bkfile(station,st_data,timestamp)
        return st_data
    elif (not st_data) and (time.time()-min_start <60):
        if time.time()-min_start <60:
            return station_value
        else:
            return None
    else:
        return None


def main(client,mqtt_client):
    seconds = 0
    seconds100 = 0
    end = 0
    st1_value = 0
    st2_value = 0
    st3_value = 0
    st4_value = 0
    st5_value = 0
    st6_value = 0
    st7_value = 0
    st8_value = 0
    st9_value = 0
    st10_value = 0
    st11_value = 0
    st12_value = 0
    st13_value = 0
    st14_value = 0
    st15_value = 0
    st16_value = 0
    st17_value = 0
    st18_value = 0
    st19_value = 0
    while True:
        start = time.time()
        min_start = time.time()
        while seconds <60:
            # if end:
            #     start = time.time()
            stamp = datetime.datetime.now().timestamp()
            # 傳入60內最後一筆非NONE的st_value 
            st1_value = execute(client,0,20,"M101",stamp,st1_value,min_start)
            st2_value = execute(client,0,20,"M102",stamp,st2_value,min_start)
            # st1_value = execute(client,0,20,"M101_M115",stamp,st1_value,min_start)
            # st2_value = execute(client,0,20,"M102_M116",stamp,st2_value,min_start)
            st3_value = execute(client,0,20,"M103",stamp,st3_value,min_start)
            # st3_value = execute(client,0,20,"M103_M117",stamp,st3_value,min_start)
            st4_value = execute(client,0,18,"M104",stamp,st4_value,min_start)
            st5_value = execute(client,20,24,"M105_M118",stamp,st5_value,min_start)
            st6_value = execute(client,0,18,"M106",stamp,st6_value,min_start)
            st7_value = execute(client,20,24,"M107_M119",stamp,st7_value,min_start)
            st8_value = execute(client,0,20,"M108",stamp,st8_value,min_start)
            # st8_value = execute(client,0,20,"M108_M114",stamp,st8_value,min_start)
            st9_value = execute(client,0,18,"M109",stamp,st9_value,min_start)
            st10_value = execute(client,0,18,"M110",stamp,st10_value,min_start)
            st11_value = execute(client,0,18,"M111",stamp,st11_value,min_start)
            st12_value = execute(client,0,20,"M112",stamp,st12_value,min_start)
            st13_value = execute(client,100,21,"M113_M120",stamp,st13_value,min_start)
            st14_value = execute(client,0,18,"M114",stamp,st14_value,min_start)
            st15_value = execute(client,0,18,"M115",stamp,st15_value,min_start)
            st16_value = execute(client,0,18,"M116",stamp,st16_value,min_start)
            st17_value = execute(client,50,49,"vfd_info49",stamp,st17_value,min_start)
            st18_value = execute(client,0,18,"M117",stamp,st18_value,min_start)
            st19_value = execute(client,130,6,"water_sensor",stamp,st19_value,min_start)
            
            if time.time()-min_start <60:
                seconds+=1
                seconds100+=1
                flag_over1min = False
                if seconds100 % 100 == 0:
                    if seconds100==5000:
                        end = time.time()
                        if 1-(end-start)-0.009 >0:
                            time.sleep(1-(end-start)-0.009)
                        else:
                            print("over time")
                        seconds100 = 0
                        start = time.time()
                    else:
                        # print("seconds100 % 100 ==0")
                        end = time.time()
                        if 1-(end-start)-0.008 >0:
                            time.sleep(1-(end-start)-0.008)
                        else:
                            print("over time")
                        start = time.time()
                else:
                    # print("sleep 1 second")
                    end = time.time()
                    if 1-(end-start)-0.007 >0:
                        time.sleep(1-(end-start)-0.007)
                    else:
                        print("over time")
                    start = time.time()
            else:
                flag_over1min = True
                seconds = 60
                start = time.time()
        if not flag_over1min:
            # "guanying/24registers"
            values24 ={"M105_M118":st5_value,"M107_M119":st7_value}
            mqttpub(mqtt_client=mqtt_client,topic="guanying/24registers",values=values24,t=stamp,qs=0,rt=True)

            # "guanying/20registers"
            values20 ={"M101":st1_value,"M102":st2_value,"M103":st3_value,"M108":st8_value,"M112":st12_value}
            mqttpub(mqtt_client=mqtt_client,topic="guanying/20registers",values=values20,t=stamp,qs=0,rt=True)
            
            # "guanying/21registers"
            values21 ={"M113_M120":st13_value}
            mqttpub(mqtt_client=mqtt_client,topic="guanying/21registers",values=values21,t=stamp,qs=0,rt=True)

            # "guanying/18registers"
            values18 ={"M104":st4_value,"M106":st6_value,"M109":st9_value,"M110":st10_value,"M111":st11_value,
            "M114":st14_value,"M115":st15_value,"M116":st16_value,"M117":st18_value}
            mqttpub(mqtt_client=mqtt_client,topic="guanying/18registers",values=values18,t=stamp,qs=0,rt=True)

            # "guanying/7motors"
            values49 = {"VFD_info49":st17_value}
            mqttpub(mqtt_client=mqtt_client,topic="guanying/7motors",values=values49,t=stamp,qs=0,rt=True)

            # "guanying/water_sensor"
            values_water = {"water_sensor":st19_value}
            mqttpub(mqtt_client=mqtt_client,topic="guanying/water_sensor",values=values_water,t=stamp,qs=0,rt=True)

        seconds=0
        st1_value = 0
        st2_value = 0
        st3_value = 0
        st4_value = 0
        st5_value = 0
        st6_value = 0
        st7_value = 0
        st8_value = 0
        st9_value = 0
        st10_value = 0
        st11_value = 0
        st12_value = 0
        st13_value = 0
        st14_value = 0
        st15_value = 0
        st16_value = 0
        st17_value = 0
        st18_value = 0
        st19_value = 0


if __name__ == "__main__":
    client = ModbusClient('localhost',port=502)
    print(client.connect())
    mqtt_client = mqtt.Client()
    # mqtt_client.connect("3.34.97.49",1883,60)
    main(client,mqtt_client)
