import pandas as pd
from sqlalchemy import create_engine
import pymysql
import os
import datetime
import glob
import numpy as np
from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadDecoder

def get_time_to_interval(dataframe):
    stamp = dataframe["timestamp_to_interval"].astype("int32")
    dt_str_series = pd.to_datetime(stamp,unit='s')
    return dt_str_series

def decode_to_32float(_registers):
    decoder = BinaryPayloadDecoder.fromRegisters(list(_registers), byteorder=Endian.Big, wordorder=Endian.Little)
    float_num = decoder.decode_32bit_float()
    return float_num

def decode_to_bits(_register):
    decoder = BinaryPayloadDecoder.fromRegisters([_register], byteorder=Endian.Big, wordorder=Endian.Little)
    return decoder.decode_bits()

def df_for_register20(filename,cols,m1,m2,s_id,meter_name,meter_s_id,engine):
    data = pd.read_csv(filename, names=cols, header=None)
    df = data.copy()
    df_sql = df.drop(["low_word","high_word"],axis=1)
    df_sql["time_to_interval"] = get_time_to_interval(df)
    df_sql["flag"] = [decode_to_bits(i)[1] for i in df_sql["bits_values"]]
    df_sql["unit_id"] = [m1 if i else m2 for i in df_sql["flag"]]
    df_sql["station_id"] = s_id
    df_sql["lm_time"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df_sql = df_sql.drop(["flag","bits_values"],axis=1)
    df_sql.to_sql('tb_eqp_sec_data_meter', con = engine, if_exists = 'append', chunksize = 1000,index= False)
    df_sql_wh_meter = df_sql.copy()
    df_sql_wh_meter=df_sql_wh_meter[["time_to_interval", "timestamp_to_interval","station_id","lm_time"]]
    x = df[["low_word","high_word"]].values
    decode_to_float = lambda t: decode_to_32float(t)
    float_nums = np.array([decode_to_float(xi) for xi in x])
    df_sql_wh_meter["kwh"] = float_nums
    df_sql_wh_meter["station_id"] = meter_s_id
    df_sql_wh_meter["wh_meter_name"] = meter_name
    df_sql_wh_meter.to_sql('tb_watt_hour_meter_list', con = engine, if_exists = 'append', chunksize = 1000,index= False)


def df_for_register18(filename,cols,m1,m2,s_id,engine):
    data = pd.read_csv(filename, names=cols, header=None)
    df = data.copy()
    df_sql = df.drop(["bits_values"],axis=1)
    df_sql["time_to_interval"] = get_time_to_interval(df)
    df_sql["flag"] = [decode_to_bits(i)[1] for i in df["bits_values"]]
    df_sql["unit_id"] = [m1 if i else m2 for i in df_sql["flag"]]
    df_sql["station_id"] = s_id
    df_sql["lm_time"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df_sql = df_sql.drop(["flag"],axis=1)
    df_sql.to_sql('tb_eqp_sec_data_meter', con = engine, if_exists = 'append', chunksize = 1000,index= False)


def df_for_register24(filename,cols,m1,m2,s_id,sensor_id,sensor_s_id,meter_name,meter_s_id,engine):
    #TODO:temperature
    data = pd.read_csv(filename, names=cols, header=None)
    df = data.copy()
    df_sql = df.drop(["low_word","high_word","temp_low_word","temp_high_word","bits_values2","DO"],axis=1)
    df_sql["time_to_interval"] = get_time_to_interval(df)
    df_sql["flag"] = [decode_to_bits(i)[1] for i in df_sql["bits_values"]]
    df_sql["unit_id"] = [m1 if i else m2 for i in df_sql["flag"]]
    df_sql["station_id"] = s_id
    df_sql["lm_time"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df_sql = df_sql.drop(["flag","bits_values"],axis=1)
    df_sql.to_sql('tb_eqp_sec_data_meter', con = engine, if_exists = 'append', chunksize = 1000,index= False)
    
    df_sql_wh_meter = df_sql.copy()
    df_sql_wh_meter=df_sql_wh_meter[["time_to_interval", "timestamp_to_interval","lm_time"]]
    x = df[["low_word","high_word"]].values
    decode_to_float = lambda t: decode_to_32float(t)
    float_nums = np.array([decode_to_float(xi) for xi in x])
    print(float_nums)
    df_sql_wh_meter["kwh"] = float_nums
    df_sql_wh_meter["station_id"] = meter_s_id
    df_sql_wh_meter["wh_meter_name"] = meter_name
    df_sql_wh_meter.to_sql('tb_watt_hour_meter_list', con = engine, if_exists = 'append', chunksize = 1000,index= False)

    df_sql_do = df_sql_wh_meter[["time_to_interval", "timestamp_to_interval","lm_time"]]
    df_sql_do["unit_id"] = sensor_id
    df_sql_do["station_id"] = sensor_s_id
    df_sql_do["DO"] = data["DO"]
    x = df[["temp_low_word","temp_high_word"]].values
    decode_to_float = lambda t: decode_to_32float(t)
    float_nums = np.array([decode_to_float(xi) for xi in x])
    df_sql_do["temperature"] = float_nums
    df_sql_do.to_sql('tb_eqp_sec_data_sensor', con = engine, if_exists = 'append', chunksize = 1000,index= False)

def insert_7_motor_sql(motor_val,unit_id,s_id,timestr,lm_time,cur):
    col = ["time_to_interval", "unit_id", "station_id", "lm_time", "hz_set", "hz", "amp","dc_bus_volt", "volt", "kw", "vfd_temp","timestamp_to_interval"]
    col_str = ",".join(col)
    sql_cmd = f"insert into tb_eqp_min_data_meter ({col_str}) values ('{timestr}','{unit_id}','{s_id}','{lm_time}',{motor_val})"
    cur.execute(sql_cmd)

def insert_sensor_sql(col,timestr,timestamp,unit_id,s_id,value,lm_time,cur):
    col_str = ",".join(col)
    sql_cmd = f"insert into tb_eqp_sec_data_sensor ({col_str}) values ('{timestr}','{timestamp}','{unit_id}','{s_id}','{lm_time}','{value}')"
    cur.execute(sql_cmd)

# create sqlalchemy engine
engine = create_engine("mysql+pymysql://{user}:{pw}@webdb.c93tkjt9wmzg.ap-northeast-2.rds.amazonaws.com/{db}"
                       .format(user="webtest1",
                               pw="Stanforda@0978966891",
                               db="web_worksheet_beta"))

conn = pymysql.connect(host='webdb.c93tkjt9wmzg.ap-northeast-2.rds.amazonaws.com', user='webtest1', password='Stanforda@0978966891',
        db='web_worksheet_beta', port=3306, autocommit=True, charset='utf8')
cur = conn.cursor()

yesterday =(datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")
sec_folder_path = "/home/ubuntu/moxa_backup/" + "20210118" + "/second/" + "20210118" + "/"
# mqtt_folder_path = "/home/ubuntu/moxa_backup/" + yesterday + "/mqtt/" + yesterday + "/"
# lm_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# 21
csv_registers_20_cols = ["hz","hz_set", "amp", "volt", "dc_bus_volt", "vfd_status", "fault_code", "kw", 
                         "torque_current", "vfd_temp", "pf_factor", "fault_hz", "fault_current", 
                         "fault_bus_volt","fault_status", "dc_bus_ripple_v", "bits_values","vfd_frequence",
                         "low_word","high_word","timestamp_to_interval"] 

csv_registers_18_cols = ["hz","hz_set", "amp", "volt", "dc_bus_volt", "vfd_status", "fault_code", "kw", 
                         "torque_current", "vfd_temp", "pf_factor", "fault_hz", "fault_current", 
                         "fault_bus_volt","fault_status", "dc_bus_ripple_v", "bits_values","vfd_frequence",
                         "timestamp_to_interval"]

csv_registers_24_cols = ["hz","hz_set", "amp", "volt", "dc_bus_volt", "vfd_status", "fault_code", "kw", 
                         "torque_current", "vfd_temp", "pf_factor", "fault_hz", "fault_current", 
                         "fault_bus_volt","fault_status", "dc_bus_ripple_v", "bits_values","DO",
                         "vfd_frequence", "low_word","high_word","bits_values2",
                         "temp_low_word","temp_high_word","timestamp_to_interval"]

if os.path.exists(sec_folder_path):
    dirPathPattern =  sec_folder_path + "*.csv"
    csv_files = glob.glob(dirPathPattern)
    for file in csv_files:
        ############ "guanying/20registers"  ############
        if file[-8:-4] == "M101":
            df_for_register20(file,csv_registers_20_cols,"M101","M115",1,"ROI_1_WH",1,engine)
        elif file[-8:-4] == "M102":
            df_for_register20(file,csv_registers_20_cols,"M102","M116",2,"ROI_2_WH",2,engine)
        elif file[-8:-4] == "M103":
            df_for_register20(file,csv_registers_20_cols,"M103","M117",3,"ROI_3_WH",3,engine)
        elif file[-8:-4] == "M108":
            df_for_register20(file,csv_registers_20_cols,"M108","M114",6,"ROI_6_WH",6,engine)
        elif file[-8:-4] == "M112":
            df_for_register20(file,csv_registers_20_cols,"M112","M112",7,"ROI_7_WH",7,engine)
        elif file[-13:-4] == "M113_M120":
            df_for_register20(file,csv_registers_20_cols,"M113","M113",8,"ROI_8_WH",8,engine)
        elif file[-8:-4] == "M104":
            df_for_register18(file,csv_registers_18_cols,"M104","M104",4,engine)
        elif file[-8:-4] == "M106":
            df_for_register18(file,csv_registers_18_cols,"M106","M106",5,engine)
        elif file[-8:-4] == "M109":
            df_for_register18(file,csv_registers_18_cols,"M109","M109",7,engine)
        elif file[-8:-4] == "M110":
            df_for_register18(file,csv_registers_18_cols,"M110","1010",7,engine)
        elif file[-8:-4] == "M111":
            df_for_register18(file,csv_registers_18_cols,"M111","1111",7,engine)
        elif file[-8:-4] == "M114":
            df_for_register18(file,csv_registers_18_cols,"M114","1414",6,engine)
        elif file[-8:-4] == "M115":
            df_for_register18(file,csv_registers_18_cols,"M115","1515",1,engine)
        elif file[-8:-4] == "M116":
            df_for_register18(file,csv_registers_18_cols,"M116","1616",2,engine)
        elif file[-8:-4] == "M117":
            df_for_register18(file,csv_registers_18_cols,"M117","1717",3,engine)
        elif file[-13:-4] == "M105_M118":
            df_for_register24(file,csv_registers_24_cols,"M105","M118",4,"S104",4,"ROI_5_WH",5,engine)
        elif file[-13:-4] == "M107_M119":
            df_for_register24(file,csv_registers_24_cols,"M107","M119",4,"S106",5,"ROI_4_WH",4,engine)
        elif file[-14:-4] == "vfd_info49":
            with open(file,"r") as f:
                lines = f.readlines()
            lm_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for line in lines:
                #TODO *100 *10
                values = line.strip().split(",")
                motor1_val = values[:7]
                motor1_val= [eval(motor1_val[i])*100 if i < 2 else eval(motor1_val[i])*10 for i in range(len(motor1_val))]
                motor1_val.append(int(eval(values[49])))
                motor1_val = [str(i) for i in motor1_val]
                motor2_val = values[7:14]
                motor2_val= [eval(motor2_val[i])*100 if i < 2 else eval(motor2_val[i])*10 for i in range(len(motor2_val))]
                motor2_val.append(int(eval(values[49])))
                motor2_val = [str(i) for i in motor2_val]
                motor3_val = values[14:21]
                motor3_val= [eval(motor3_val[i])*100 if i < 2 else eval(motor3_val[i])*10 for i in range(len(motor3_val))]
                motor3_val.append(int(eval(values[49])))     
                motor3_val = [str(i) for i in motor3_val]
                motor4_val = values[21:28]
                motor4_val= [eval(motor4_val[i])*100 if i < 2 else eval(motor4_val[i])*10 for i in range(len(motor4_val))]
                motor4_val.append(int(eval(values[49])))
                motor4_val = [str(i) for i in motor4_val]
                motor5_val = values[28:35]
                motor5_val= [eval(motor5_val[i])*100 if i < 2 else eval(motor5_val[i])*10 for i in range(len(motor5_val))]
                motor5_val.append(int(eval(values[49])))
                motor5_val = [str(i) for i in motor5_val]
                motor6_val = values[35:42]
                motor6_val= [eval(motor6_val[i])*100 if i < 2 else eval(motor6_val[i])*10 for i in range(len(motor6_val))]
                motor6_val.append(int(eval(values[49])))
                motor6_val = [str(i) for i in motor6_val]
                motor7_val = values[42:49]
                motor7_val= [eval(motor7_val[i])*100 if i < 2 else eval(motor7_val[i])*10 for i in range(len(motor7_val))]
                motor7_val.append(int(eval(values[49])))
                motor7_val = [str(i) for i in motor7_val]
                motor1 = "','".join(motor1_val)
                motor1 = "'" + motor1 + "'"
                motor2 = "','".join(motor2_val)
                motor2 = "'" + motor2 + "'"
                motor3 = "','".join(motor3_val)
                motor3 = "'" + motor3 + "'"
                motor4 = "','".join(motor4_val)
                motor4 = "'" + motor4 + "'"
                motor5 = "','".join(motor5_val)
                motor5 = "'" + motor5 + "'"
                motor6 = "','".join(motor6_val)
                motor6 = "'" + motor6 + "'"
                motor7 = "','".join(motor7_val)
                motor7 = "'" + motor7 + "'"
                timestamp = int(eval(values[49]))
                timestr = datetime.datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")
                insert_7_motor_sql(motor1,"M121","P",timestr,lm_time,cur)
                insert_7_motor_sql(motor2,"M122","P",timestr,lm_time,cur)
                insert_7_motor_sql(motor3,"M123","P",timestr,lm_time,cur)
                insert_7_motor_sql(motor4,"M124","P",timestr,lm_time,cur)
                insert_7_motor_sql(motor2,"M125","3D",timestr,lm_time,cur)
                insert_7_motor_sql(motor3,"M126","3D",timestr,lm_time,cur)
                insert_7_motor_sql(motor4,"M127","3D",timestr,lm_time,cur)
        elif file[-16:-4] == "water_sensor":
            with open(file,"r") as f:
                lines = f.readlines()
            lm_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for line in lines:
                values = line.strip().split(",")
                level1 = int(eval(values[0])) * 10
                level2 = int(eval(values[1])) * 10
                flow1 = int(eval(values[2]))  * 10
                flow2 = int(eval(values[3])) * 10
                threshold = int(eval(values[4])) * 10
                level3 = int(eval(values[5])) * 10

                timestamp = int(eval(values[6]))
                timestr = datetime.datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d")
                _col = ["time_to_interval","timestamp_to_interval","unit_id","station_id","lm_time"]
                col1 = _col.copy()
                col2 = _col.copy()
                col3 = _col.copy()
                col4 = _col.copy()
                col5 = _col.copy()
                col6 = _col.copy()
                col1.append("liquid_level")
                col2.append("liquid_level")
                col3.append("flow")
                col4.append("flow")
                col5.append("threshold")
                col6.append("liquid_level")
                insert_sensor_sql(col1,timestr,timestamp,"S116","P",level1,lm_time,cur)
                insert_sensor_sql(col2,timestr,timestamp,"S117","1",level2,lm_time,cur)
                insert_sensor_sql(col3,timestr,timestamp,"S118","D",flow1,lm_time,cur)
                insert_sensor_sql(col4,timestr,timestamp,"S119","D",flow2,lm_time,cur)
                insert_sensor_sql(col5,timestr,timestamp,"S120","R",threshold,lm_time,cur)
                insert_sensor_sql(col6,timestr,timestamp,"S121","3D",level3,lm_time,cur)

