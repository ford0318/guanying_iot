"""THIS IS MQTT CLIENT FOR SUBSCRIBE , WHILE IT RECEIVED DATA, INSERT RECIEVED DATA INTO MYSQL DATABASE IMMEDIATELY."""

import paho.mqtt.client as mqtt
import pymysql
import json
import datetime
from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadDecoder

#使用with语句是， pymysql.connect返回的是数据库游标。(具体的内容查看源代码)
# conn = pymysql.connect(host='4.tcp.ngrok.io', user='ford0318', password='fordlove0713',
#         db='mqtt_db', port=18366, autocommit=True, charset='utf8')
conn = pymysql.connect(host='webdb.c93tkjt9wmzg.ap-northeast-2.rds.amazonaws.com', user='webtest1', password='Stanforda@0978966891',
        db='web_worksheet_beta', port=3306, autocommit=True, charset='utf8')
cur = conn.cursor()

def decode_to_bits(_register):
    decoder = BinaryPayloadDecoder.fromRegisters([_register], byteorder=Endian.Big, wordorder=Endian.Little)
    return decoder.decode_bits() 

def decode_to_32bit_float(_registers):
    decoder = BinaryPayloadDecoder.fromRegisters(_registers, byteorder=Endian.Big, wordorder=Endian.Little)
    return decoder.decode_32bit_float() 

def write_back_sql(filename,sql_string):
    """ WRITE THE SQL COMMAND STRING IN SPECIFIED FILE NAME
    """
    filename = filename + ".txt"
    with open(filename,"a") as f:
        f.writelines(sql_string+"\n")

def on_connect(client,userdata,flags,rc):
    print("Connected with result code"+str(rc))
    client.subscribe([("guanying/18registers",0),("guanying/20registers",0),("guanying/21registers",0),("guanying/24registers",0),
    ("guanying/7motors",0),("guanying/water_sensor",0)])

def on_message(client,userdata,msg):
    # print(msg.topic+" "+msg.payload.decode("utf-8"))
    raw_string = msg.payload.decode("utf-8")
    data = json.loads(raw_string)
    # print("stamp:",data["time"])
    dt_stamp = int(data["time"]) + 60*60*8
    dt_str = datetime.datetime.fromtimestamp(dt_stamp).strftime("%Y-%m-%d %H:%M:%S")
    lm_time = datetime.datetime.fromtimestamp(int(datetime.datetime.now().timestamp()+8*60*60)).strftime("%Y-%m-%d %H:%M:%S")
 
    #print(lm_time)
    
    if msg.topic == "guanying/20registers":
        eqp_id_M101_M115 = data["value"]["M101"]
        eqp_id_M102_M116 = data["value"]["M102"]
        eqp_id_M103_M117 = data["value"]["M103"]
        eqp_id_M108_M114 = data["value"]["M108"]
        eqp_id_M112 = data["value"]["M112"]
        lm_time = datetime.datetime.fromtimestamp(int(datetime.datetime.now().timestamp()+ 8*60*60)).strftime("%Y-%m-%d %H:%M:%S")
        if eqp_id_M101_M115 == None or  eqp_id_M101_M115 == "None" :
            pass
        else:
            # CHECK THE 3RD BIT IN 17TH WORD
            # decoder = BinaryPayloadDecoder.fromRegisters([eqp_id_M101_M115[16]], byteorder=Endian.Big, wordorder=Endian.Little)
            # bits_values = decoder.decode_bits() 
            POS = decode_to_bits(eqp_id_M101_M115[16])[2]
            # val = eqp_id_M101_M115[16]
            # POS = val & (1<<2)
            # POS = POS >>2

            # CHECK THE SECOND MOTOR IS RUNNING OR NOT. IF TRUE, EXECUTE THE FIRST BLOCK, ELSE EXECUTE SECOND.
            if POS:
                insert_sql_1 = f"""insert into tb_eqp_min_data_meter (time_to_interval, timestamp_to_interval, unit_id, station_id,
                hz, hz_set, amp, volt, dc_bus_volt, vfd_status, fault_code, kw, torque_current, vfd_temp, pf_factor, fault_hz,
                fault_current, fault_bus_volt,fault_status, dc_bus_ripple_v, vfd_frequence, lm_time
                ) values ('{dt_str}',{dt_stamp},'M115','1','{eqp_id_M101_M115[0]}','{eqp_id_M101_M115[1]}','{eqp_id_M101_M115[2]}',
                '{eqp_id_M101_M115[3]}','{eqp_id_M101_M115[4]}','{eqp_id_M101_M115[5]}','{eqp_id_M101_M115[6]}','{eqp_id_M101_M115[7]}',
                '{eqp_id_M101_M115[8]}','{eqp_id_M101_M115[9]}','{eqp_id_M101_M115[10]}','{eqp_id_M101_M115[11]}',
                '{eqp_id_M101_M115[12]}','{eqp_id_M101_M115[13]}','{eqp_id_M101_M115[14]}','{eqp_id_M101_M115[15]}',
                '{eqp_id_M101_M115[17]}','{lm_time}')"""
            else:
                insert_sql_1 = f"""insert into tb_eqp_min_data_meter (time_to_interval, timestamp_to_interval, unit_id, station_id,
                hz, hz_set, amp, volt, dc_bus_volt, vfd_status, fault_code, kw, torque_current, vfd_temp, pf_factor, fault_hz,
                fault_current, fault_bus_volt,fault_status, dc_bus_ripple_v, vfd_frequence, lm_time
                ) values ('{dt_str}',{dt_stamp},'M101','1','{eqp_id_M101_M115[0]}','{eqp_id_M101_M115[1]}','{eqp_id_M101_M115[2]}',
                '{eqp_id_M101_M115[3]}','{eqp_id_M101_M115[4]}','{eqp_id_M101_M115[5]}','{eqp_id_M101_M115[6]}','{eqp_id_M101_M115[7]}',
                '{eqp_id_M101_M115[8]}','{eqp_id_M101_M115[9]}','{eqp_id_M101_M115[10]}','{eqp_id_M101_M115[11]}',
                '{eqp_id_M101_M115[12]}','{eqp_id_M101_M115[13]}','{eqp_id_M101_M115[14]}','{eqp_id_M101_M115[15]}',
                '{eqp_id_M101_M115[17]}','{lm_time}')"""
            try:
            #write_back_sql("M101_M115_data_meter_sql",insert_sql_1)
                cur.execute(insert_sql_1)
            except Exception as e:
                print(e)
                write_back_sql("M101_M115_data_meter_sql",insert_sql_1)
            else:
                print("M101/115 插入数据成功.......")
            # Watt Hour meter 
            # Watt Hour meter
            #　V = (-1)^S * 2^(E-127) * (1+M)
            #   SEEE EEEE   EMMM MMMM   MMMM MMMM   MMMM MMMM
            # hi = int(eqp_id_M101_M115[19])
            # lo = int(eqp_id_M101_M115[18])
            # intSign = hi // 32768
            # intSignRest = hi % 32768
            # intExponent = intSignRest // 128
            # intExponentRest = intSignRest % 128
            # faDigit = (intExponentRest * 65536 + lo) / 8388608
            # wh_meter = ((-1)**intSign)*(2**(intExponent - 127)) * (faDigit + 1)
            # ROI 1 WH

            # GET THE 19 20 WORD AND CONVERT TO 32BIT FLOAT
            # decoder_float = BinaryPayloadDecoder.fromRegisters(eqp_id_M101_M115[18:20], byteorder=Endian.Big, wordorder=Endian.Little)
            # wh_meter = decoder_float.decode_32bit_float()
            wh_meter = decode_to_32bit_float(eqp_id_M101_M115[18:20])
            insert_sql_1_2 = f"""insert into tb_watt_hour_meter_list (time_to_interval, timestamp_to_interval, station_id,
            wh_meter_name, kwh,lm_time) values ('{dt_str}', {dt_stamp},'1','ROI_1_WH','{wh_meter}','{lm_time}')"""
            try:
                cur.execute(insert_sql_1_2)
            except:
                write_back_sql("ROI_1_WH_sql",insert_sql_1_2)
            else:
                print("ROI_1_WH 插入数据成功.......")

        if eqp_id_M102_M116 == None or  eqp_id_M102_M116 == "None" :
            pass
        else:
            # GET THE 3RD BIT IN 17TH WORD
            POS = decode_to_bits(eqp_id_M102_M116[16])[2]

            # CHECK THE SECOND MOTOR IS RUNNING OR NOT. IF TRUE, EXECUTE THE FIRST BLOCK, ELSE EXECUTE SECOND.
            if POS:
                insert_sql_2 = f"""insert into tb_eqp_min_data_meter (time_to_interval, timestamp_to_interval, unit_id, station_id,
                hz, hz_set, amp, volt, dc_bus_volt, vfd_status, fault_code, kw, torque_current, vfd_temp, pf_factor, fault_hz,
                fault_current, fault_bus_volt, fault_status, dc_bus_ripple_v, vfd_frequence, lm_time
                ) values ('{dt_str}',{dt_stamp},'M116','2','{eqp_id_M102_M116[0]}','{eqp_id_M102_M116[1]}','{eqp_id_M102_M116[2]}',
                '{eqp_id_M102_M116[3]}','{eqp_id_M102_M116[4]}','{eqp_id_M102_M116[5]}','{eqp_id_M102_M116[6]}','{eqp_id_M102_M116[7]}',
                '{eqp_id_M102_M116[8]}','{eqp_id_M102_M116[9]}','{eqp_id_M102_M116[10]}','{eqp_id_M102_M116[11]}',
                '{eqp_id_M102_M116[12]}','{eqp_id_M102_M116[13]}','{eqp_id_M102_M116[14]}','{eqp_id_M102_M116[15]}',
                '{eqp_id_M102_M116[17]}','{lm_time}')"""
            else:
                insert_sql_2 = f"""insert into tb_eqp_min_data_meter (time_to_interval, timestamp_to_interval, unit_id, station_id,
                hz, hz_set, amp, volt, dc_bus_volt, vfd_status, fault_code, kw, torque_current, vfd_temp, pf_factor, fault_hz,
                fault_current, fault_bus_volt, fault_status, dc_bus_ripple_v, vfd_frequence, lm_time
                ) values ('{dt_str}',{dt_stamp},'M102','2','{eqp_id_M102_M116[0]}','{eqp_id_M102_M116[1]}','{eqp_id_M102_M116[2]}',
                '{eqp_id_M102_M116[3]}','{eqp_id_M102_M116[4]}','{eqp_id_M102_M116[5]}','{eqp_id_M102_M116[6]}','{eqp_id_M102_M116[7]}',
                '{eqp_id_M102_M116[8]}','{eqp_id_M102_M116[9]}','{eqp_id_M102_M116[10]}','{eqp_id_M102_M116[11]}',
                '{eqp_id_M102_M116[12]}','{eqp_id_M102_M116[13]}','{eqp_id_M102_M116[14]}','{eqp_id_M102_M116[15]}',
                '{eqp_id_M102_M116[17]}','{lm_time}')"""
            try:
                cur.execute(insert_sql_2)
            except:
                write_back_sql("M102_M116_data_meter_sql",insert_sql_2)
            else:
                print("M102/116 插入数据成功.......")

            # GET THE 19 20 WORD AND CONVERT TO 32BIT FLOAT
            wh_meter = decode_to_32bit_float(eqp_id_M102_M116[18:20])
            insert_sql_2_2 = f"""insert into tb_watt_hour_meter_list (time_to_interval, timestamp_to_interval, station_id,
            wh_meter_name, kwh,lm_time) values ('{dt_str}', {dt_stamp},'2','ROI_2_WH','{wh_meter}','{lm_time}')"""
            try:
                cur.execute(insert_sql_2_2)
            except:
                write_back_sql("ROI_2_WH_sql",insert_sql_2_2)
            else:
                print("ROI_2_WH 插入数据成功.......")

        if eqp_id_M103_M117 == None or  eqp_id_M103_M117 == "None" :
            pass
        else:
            # GET THE 3RD BIT IN 17TH WORD
            POS = decode_to_bits(eqp_id_M103_M117[16])[2]

            # CHECK THE SECOND MOTOR IS RUNNING OR NOT. IF TRUE, EXECUTE THE FIRST BLOCK, ELSE EXECUTE SECOND.
            if POS:
                insert_sql_3 = f"""insert into tb_eqp_min_data_meter (time_to_interval, timestamp_to_interval, unit_id, station_id,
                hz, hz_set, amp, volt, dc_bus_volt, vfd_status, fault_code, kw, torque_current, vfd_temp, pf_factor, fault_hz,
                fault_current, fault_bus_volt, fault_status, dc_bus_ripple_v, vfd_frequence, lm_time
                ) values ('{dt_str}',{dt_stamp},'M117','3','{eqp_id_M103_M117[0]}','{eqp_id_M103_M117[1]}','{eqp_id_M103_M117[2]}',
                '{eqp_id_M103_M117[3]}','{eqp_id_M103_M117[4]}','{eqp_id_M103_M117[5]}','{eqp_id_M103_M117[6]}','{eqp_id_M103_M117[7]}',
                '{eqp_id_M103_M117[8]}','{eqp_id_M103_M117[9]}','{eqp_id_M103_M117[10]}','{eqp_id_M103_M117[11]}',
                '{eqp_id_M103_M117[12]}','{eqp_id_M103_M117[13]}','{eqp_id_M103_M117[14]}','{eqp_id_M103_M117[15]}',
                '{eqp_id_M103_M117[17]}','{lm_time}')"""
            else:
                insert_sql_3 = f"""insert into tb_eqp_min_data_meter (time_to_interval, timestamp_to_interval, unit_id, station_id,
                hz, hz_set, amp, volt, dc_bus_volt, vfd_status, fault_code, kw, torque_current, vfd_temp, pf_factor, fault_hz,
                fault_current, fault_bus_volt, fault_status, dc_bus_ripple_v, vfd_frequence, lm_time
                ) values ('{dt_str}',{dt_stamp},'M103','3','{eqp_id_M103_M117[0]}','{eqp_id_M103_M117[1]}','{eqp_id_M103_M117[2]}',
                '{eqp_id_M103_M117[3]}','{eqp_id_M103_M117[4]}','{eqp_id_M103_M117[5]}','{eqp_id_M103_M117[6]}','{eqp_id_M103_M117[7]}',
                '{eqp_id_M103_M117[8]}','{eqp_id_M103_M117[9]}','{eqp_id_M103_M117[10]}','{eqp_id_M103_M117[11]}',
                '{eqp_id_M103_M117[12]}','{eqp_id_M103_M117[13]}','{eqp_id_M103_M117[14]}','{eqp_id_M103_M117[15]}',
                '{eqp_id_M103_M117[17]}','{lm_time}')"""
            try:
                cur.execute(insert_sql_3)
            except:
                write_back_sql("M103_M117_data_meter_sql",insert_sql_3)
            else:
                print("M103/117 插入数据成功.......")

            # GET THE 19 20 WORD AND CONVERT TO 32BIT FLOAT
            wh_meter = decode_to_32bit_float(eqp_id_M103_M117[18:20])
            insert_sql_3_2 = f"""insert into tb_watt_hour_meter_list (time_to_interval, timestamp_to_interval, station_id,
            wh_meter_name, kwh,lm_time) values ('{dt_str}', {dt_stamp},'3','ROI_3_WH','{wh_meter}','{lm_time}')"""
            try:
                cur.execute(insert_sql_3_2)
            except:
                write_back_sql("ROI_3_WH",insert_sql_3_2)
            else:
                print("ROI_3_WH 插入数据成功.......")

        if eqp_id_M108_M114 == None or  eqp_id_M108_M114 == "None" :
            pass
        else:
            # GET THE 3RD BIT IN 17TH WORD
            POS = decode_to_bits(eqp_id_M108_M114[16])[2]

            # CHECK THE SECOND MOTOR IS RUNNING OR NOT. IF TRUE, EXECUTE THE FIRST BLOCK, ELSE EXECUTE SECOND.
            if POS:
                insert_sql_8 = f"""insert into tb_eqp_min_data_meter (time_to_interval, timestamp_to_interval, unit_id, station_id,
                hz, hz_set, amp, volt, dc_bus_volt, vfd_status, fault_code, kw, torque_current, vfd_temp, pf_factor, fault_hz,
                fault_current,fault_bus_volt, fault_status, dc_bus_ripple_v, vfd_frequence, lm_time
                ) values ('{dt_str}',{dt_stamp},'M114','6','{eqp_id_M108_M114[0]}','{eqp_id_M108_M114[1]}','{eqp_id_M108_M114[2]}',
                '{eqp_id_M108_M114[3]}','{eqp_id_M108_M114[4]}','{eqp_id_M108_M114[5]}','{eqp_id_M108_M114[6]}','{eqp_id_M108_M114[7]}',
                '{eqp_id_M108_M114[8]}','{eqp_id_M108_M114[9]}','{eqp_id_M108_M114[10]}','{eqp_id_M108_M114[11]}',
                '{eqp_id_M108_M114[12]}','{eqp_id_M108_M114[13]}','{eqp_id_M108_M114[14]}','{eqp_id_M108_M114[15]}',
                '{eqp_id_M108_M114[17]}','{lm_time}')"""
            else:
                insert_sql_8 = f"""insert into tb_eqp_min_data_meter (time_to_interval, timestamp_to_interval, unit_id, station_id,
                hz, hz_set, amp, volt, dc_bus_volt, vfd_status, fault_code, kw, torque_current, vfd_temp, pf_factor, fault_hz, 
                fault_current, fault_bus_volt, fault_status, dc_bus_ripple_v, vfd_frequence, lm_time
                ) values ('{dt_str}',{dt_stamp},'M108','6','{eqp_id_M108_M114[0]}','{eqp_id_M108_M114[1]}','{eqp_id_M108_M114[2]}',
                '{eqp_id_M108_M114[3]}','{eqp_id_M108_M114[4]}','{eqp_id_M108_M114[5]}','{eqp_id_M108_M114[6]}','{eqp_id_M108_M114[7]}',
                '{eqp_id_M108_M114[8]}','{eqp_id_M108_M114[9]}','{eqp_id_M108_M114[10]}','{eqp_id_M108_M114[11]}',
                '{eqp_id_M108_M114[12]}','{eqp_id_M108_M114[13]}','{eqp_id_M108_M114[14]}','{eqp_id_M108_M114[15]}',
                '{eqp_id_M108_M114[17]}','{lm_time}')"""
            try:
                cur.execute(insert_sql_8)
            except:
                write_back_sql("M108_M114_data_meter_sql",insert_sql_8)
            else:
                print("M108/114 插入数据成功.......")

            # GET THE 19 20 WORD AND CONVERT TO 32BIT FLOAT
            wh_meter = decode_to_32bit_float(eqp_id_M108_M114[18:20])
            insert_sql_8_2 = f"""insert into tb_watt_hour_meter_list (time_to_interval, timestamp_to_interval, station_id,
            wh_meter_name, kwh,lm_time) values ('{dt_str}', {dt_stamp},'6','ROI_6_WH','{wh_meter}','{lm_time}')"""
            try:
                cur.execute(insert_sql_8_2)
            except:
                write_back_sql("ROI_6_WH_sql",insert_sql_8_2)
            else:
                print("ROI_6_WH 插入数据成功.......")

            

        if eqp_id_M112 == None or  eqp_id_M112 == "None" :
            pass
        else:
            # GET THE 3RD BIT IN 17TH WORD
            POS = decode_to_bits(eqp_id_M112[16])[2]

            insert_sql_12 = f"""insert into tb_eqp_min_data_meter (time_to_interval, timestamp_to_interval, unit_id, station_id,
            hz, hz_set, amp, volt, dc_bus_volt, vfd_status, fault_code, kw, torque_current, vfd_temp, pf_factor, fault_hz,
            fault_current, fault_bus_volt, fault_status, dc_bus_ripple_v, vfd_frequence, lm_time
            ) values ('{dt_str}',{dt_stamp},'M112','7','{eqp_id_M112[0]}','{eqp_id_M112[1]}','{eqp_id_M112[2]}','{eqp_id_M112[3]}',
            '{eqp_id_M112[4]}','{eqp_id_M112[5]}','{eqp_id_M112[6]}','{eqp_id_M112[7]}','{eqp_id_M112[8]}','{eqp_id_M112[9]}',
            '{eqp_id_M112[10]}','{eqp_id_M112[11]}','{eqp_id_M112[12]}','{eqp_id_M112[13]}','{eqp_id_M112[14]}','{eqp_id_M112[15]}',
            '{eqp_id_M112[17]}','{lm_time}')"""
            try:
                cur.execute(insert_sql_12)
            except:
                write_back_sql("M112_data_meter_sql",insert_sql_12)
            else:
                print("M112 插入数据成功.......")

            # GET THE 19 20 WORD AND CONVERT TO 32BIT FLOAT
            wh_meter = decode_to_32bit_float(eqp_id_M112[18:20])
            insert_sql_12_2 = f"""insert into tb_watt_hour_meter_list (time_to_interval, timestamp_to_interval, station_id,
            wh_meter_name, kwh,lm_time) values ('{dt_str}', {dt_stamp},'7','ROI_7_WH','{wh_meter}','{lm_time}')"""
            try:
                cur.execute(insert_sql_12_2)
            except:
                write_back_sql("ROI_7_WH_sql",insert_sql_12_2)
            else:
                print("ROI_7_WH 插入数据成功.......")
            

    elif msg.topic=="guanying/21registers":
        lm_time = datetime.datetime.fromtimestamp(int(datetime.datetime.now().timestamp()+8*60*60)).strftime("%Y-%m-%d %H:%M:%S")
        eqp_id_M113_M120 = data["value"]["M113_M120"]
        if eqp_id_M113_M120 == None or  eqp_id_M113_M120 == "None" :
            pass
        else:
            # GET THE 3RD BIT IN 17TH WORD
            POS = decode_to_bits(eqp_id_M113_M120[16])[2]
            

            # 汙泥抽送站圖控馬達 鼓風機按鈕 1 & 2 (bit)
            decoder_button = BinaryPayloadDecoder.fromRegisters([eqp_id_M113_M120[20]], byteorder=Endian.Big, wordorder=Endian.Little)
            on_off_s = [decoder_button.decode_bits() for _ in range(2)]

            #TODO:位元處理確認
            # CHECK THE SECOND MOTOR IS RUNNING OR NOT. IF TRUE, EXECUTE THE FIRST BLOCK, ELSE EXECUTE SECOND.
            if POS:
                insert_sql_13 = f"""insert into tb_eqp_min_data_meter (time_to_interval, timestamp_to_interval, unit_id, station_id,
                hz, hz_set, amp, volt, dc_bus_volt, vfd_status, fault_code, kw, torque_current, vfd_temp, pf_factor, fault_hz,
                fault_current, fault_bus_volt, fault_status, dc_bus_ripple_v, vfd_frequence, lm_time
                ) values ('{dt_str}',{dt_stamp},'M120','8','{eqp_id_M113_M120[0]}','{eqp_id_M113_M120[1]}','{eqp_id_M113_M120[2]}',
                '{eqp_id_M113_M120[3]}','{eqp_id_M113_M120[4]}', '{eqp_id_M113_M120[5]}','{eqp_id_M113_M120[6]}','{eqp_id_M113_M120[7]}',
                '{eqp_id_M113_M120[8]}','{eqp_id_M113_M120[9]}','{eqp_id_M113_M120[10]}','{eqp_id_M113_M120[11]}', 
                '{eqp_id_M113_M120[12]}','{eqp_id_M113_M120[13]}','{eqp_id_M113_M120[14]}','{eqp_id_M113_M120[15]}',
                '{eqp_id_M113_M120[17]}','{lm_time}')"""
            else:
                insert_sql_13 = f"""insert into tb_eqp_min_data_meter (time_to_interval, timestamp_to_interval, unit_id, station_id,
                hz, hz_set, amp,volt, dc_bus_volt, vfd_status, fault_code, kw, torque_current, vfd_temp, pf_factor, fault_hz,
                fault_current, fault_bus_volt, fault_status, dc_bus_ripple_v, vfd_frequence, lm_time
                ) values ('{dt_str}',{dt_stamp},'M113','8','{eqp_id_M113_M120[0]}','{eqp_id_M113_M120[1]}','{eqp_id_M113_M120[2]}',
                '{eqp_id_M113_M120[3]}','{eqp_id_M113_M120[4]}', '{eqp_id_M113_M120[5]}','{eqp_id_M113_M120[6]}','{eqp_id_M113_M120[7]}',
                '{eqp_id_M113_M120[8]}','{eqp_id_M113_M120[9]}','{eqp_id_M113_M120[10]}','{eqp_id_M113_M120[11]}',
                '{eqp_id_M113_M120[12]}','{eqp_id_M113_M120[13]}','{eqp_id_M113_M120[14]}','{eqp_id_M113_M120[15]}',
                '{eqp_id_M113_M120[17]}','{lm_time}')"""
            try:
                cur.execute(insert_sql_13)
            except:
                write_back_sql("M113_M120_data_meter_sql",insert_sql_13)
            else:
                print("M113/120 插入数据成功.......")

            # GET THE 19 20 WORD AND CONVERT TO 32BIT FLOAT
            wh_meter = decode_to_32bit_float(eqp_id_M113_M120[18:20])
            insert_sql_13_2 = f"""insert into tb_watt_hour_meter_list (time_to_interval, timestamp_to_interval, station_id,
            wh_meter_name, kwh,lm_time) values ('{dt_str}', {dt_stamp},'8','ROI_8_WH','{wh_meter}','{lm_time}')"""
            try:
                cur.execute(insert_sql_13_2)
            except:
                write_back_sql("ROI_8_WH_sql",insert_sql_13_2)
            else:
                print("M113 插入数据成功.......")
    
    elif msg.topic == "guanying/18registers":    
        eqp_id_M104 = data["value"]["M104"]
        eqp_id_M106 = data["value"]["M106"]
        eqp_id_M109 = data["value"]["M109"]
        eqp_id_M110 = data["value"]["M110"]
        eqp_id_M111 = data["value"]["M111"]
        eqp_id_M114 = data["value"]["M114"]
        eqp_id_M115 = data["value"]["M115"]
        eqp_id_M116 = data["value"]["M116"]
        eqp_id_M117 = data["value"]["M117"]
        lm_time = datetime.datetime.fromtimestamp(int(datetime.datetime.now().timestamp()+8*60*60)).strftime("%Y-%m-%d %H:%M:%S")

        if eqp_id_M104 == None or  eqp_id_M104 == "None" :
            pass
        else:
            insert_sql_4 = f"""insert into tb_eqp_min_data_meter (time_to_interval, timestamp_to_interval, unit_id, station_id,
            hz, hz_set, amp, volt, dc_bus_volt, vfd_status, fault_code, kw, torque_current, vfd_temp, pf_factor, fault_hz,
            fault_current, fault_bus_volt, fault_status, dc_bus_ripple_v, vfd_frequence, lm_time
            ) values ('{dt_str}',{dt_stamp},'M104','4','{eqp_id_M104[0]}','{eqp_id_M104[1]}','{eqp_id_M104[2]}','{eqp_id_M104[3]}',
            '{eqp_id_M104[4]}','{eqp_id_M104[5]}','{eqp_id_M104[6]}','{eqp_id_M104[7]}','{eqp_id_M104[8]}','{eqp_id_M104[9]}',
            '{eqp_id_M104[10]}','{eqp_id_M104[11]}','{eqp_id_M104[12]}','{eqp_id_M104[13]}','{eqp_id_M104[14]}','{eqp_id_M104[15]}',
            '{eqp_id_M104[17]}','{lm_time}')"""
            try:
                cur.execute(insert_sql_4)
            except:
                write_back_sql("M104_data_meter_sql",insert_sql_4)
            else:
                print("M104 插入数据成功.......")

        if eqp_id_M106 == None or  eqp_id_M106 == "None" :
            pass
        else:
            insert_sql_6 = f"""insert into tb_eqp_min_data_meter (time_to_interval, timestamp_to_interval, unit_id, station_id,
            hz, hz_set, amp, volt, dc_bus_volt, vfd_status, fault_code, kw, torque_current, vfd_temp, pf_factor, fault_hz,
            fault_current, fault_bus_volt, fault_status, dc_bus_ripple_v, vfd_frequence, lm_time
            ) values ('{dt_str}',{dt_stamp},'M106','5','{eqp_id_M106[0]}','{eqp_id_M106[1]}','{eqp_id_M106[2]}','{eqp_id_M106[3]}',
            '{eqp_id_M106[4]}','{eqp_id_M106[5]}','{eqp_id_M106[6]}','{eqp_id_M106[7]}','{eqp_id_M106[8]}','{eqp_id_M106[9]}',
            '{eqp_id_M106[10]}','{eqp_id_M106[11]}','{eqp_id_M106[12]}','{eqp_id_M106[13]}','{eqp_id_M106[14]}','{eqp_id_M106[15]}',
            '{eqp_id_M106[17]}','{lm_time}')"""
            try:
                cur.execute(insert_sql_6)
            except:
                write_back_sql("M106_data_meter_sql",insert_sql_6)
            else:
                print("M106 插入数据成功.......")

        if eqp_id_M109 == None or  eqp_id_M109 == "None" :
            pass
        else:
            insert_sql_9 = f"""insert into tb_eqp_min_data_meter (time_to_interval, timestamp_to_interval, unit_id, station_id,
            hz, hz_set, amp, volt, dc_bus_volt, vfd_status, fault_code, kw, torque_current, vfd_temp, pf_factor, fault_hz,
            fault_current, fault_bus_volt, fault_status, dc_bus_ripple_v, vfd_frequence, lm_time
            ) values ('{dt_str}',{dt_stamp},'M109','7','{eqp_id_M109[0]}','{eqp_id_M109[1]}','{eqp_id_M109[2]}','{eqp_id_M109[3]}',
            '{eqp_id_M109[4]}','{eqp_id_M109[5]}','{eqp_id_M109[6]}','{eqp_id_M109[7]}','{eqp_id_M109[8]}','{eqp_id_M109[9]}',
            '{eqp_id_M109[10]}','{eqp_id_M109[11]}','{eqp_id_M109[12]}','{eqp_id_M109[13]}','{eqp_id_M109[14]}','{eqp_id_M109[15]}',
            '{eqp_id_M109[17]}','{lm_time}')"""
            try:
                cur.execute(insert_sql_9)
            except:
                write_back_sql("M109_data_meter_sql",insert_sql_9)
            else:
                print("M109 插入数据成功.......")

        if eqp_id_M110 == None or  eqp_id_M110 == "None" :
            pass
        else:
            insert_sql_10 = f"""insert into tb_eqp_min_data_meter (time_to_interval, timestamp_to_interval, unit_id, station_id,
            hz,hz_set,amp,volt, dc_bus_volt, vfd_status, fault_code, kw, torque_current, vfd_temp, pf_factor, fault_hz,
            fault_current, fault_bus_volt, fault_status, dc_bus_ripple_v, vfd_frequence , lm_time
            ) values ('{dt_str}',{dt_stamp},'M110','7','{eqp_id_M110[0]}','{eqp_id_M110[1]}','{eqp_id_M110[2]}','{eqp_id_M110[3]}',
            '{eqp_id_M110[4]}','{eqp_id_M110[5]}','{eqp_id_M110[6]}','{eqp_id_M110[7]}','{eqp_id_M110[8]}','{eqp_id_M110[9]}',
            '{eqp_id_M110[10]}','{eqp_id_M110[11]}','{eqp_id_M110[12]}','{eqp_id_M110[13]}','{eqp_id_M110[14]}','{eqp_id_M110[15]}',
            '{eqp_id_M110[17]}','{lm_time}')"""
            try:
                cur.execute(insert_sql_10)
            except:
                write_back_sql("M110_data_meter_sql",insert_sql_10)
            else:
                print("M110 插入数据成功.......")

        if eqp_id_M111 == None or  eqp_id_M111 == "None" :
            pass
        else:
            insert_sql_11 = f"""insert into tb_eqp_min_data_meter (time_to_interval, timestamp_to_interval, unit_id, station_id,
            hz, hz_set, amp, volt, dc_bus_volt, vfd_status, fault_code, kw, torque_current, vfd_temp, pf_factor, fault_hz,
            fault_current, fault_bus_volt, fault_status, dc_bus_ripple_v, vfd_frequence, lm_time
            ) values ('{dt_str}',{dt_stamp},'M111','7','{eqp_id_M111[0]}','{eqp_id_M111[1]}','{eqp_id_M111[2]}','{eqp_id_M111[3]}',
            '{eqp_id_M111[4]}','{eqp_id_M111[5]}','{eqp_id_M111[6]}','{eqp_id_M111[7]}','{eqp_id_M111[8]}','{eqp_id_M111[9]}',
            '{eqp_id_M111[10]}','{eqp_id_M111[11]}','{eqp_id_M111[12]}','{eqp_id_M111[13]}','{eqp_id_M111[14]}','{eqp_id_M111[15]}',
            '{eqp_id_M111[17]}','{lm_time}')"""
            try:
                cur.execute(insert_sql_11)
            except:
                write_back_sql("M111_data_meter_sql",insert_sql_11)
            else:
                print("M111 插入数据成功.......")

        if eqp_id_M114 == None or  eqp_id_M114 == "None" :
            pass
        else:
            insert_sql_14 = f"""insert into tb_eqp_min_data_meter (time_to_interval, timestamp_to_interval, unit_id, station_id,
            hz, hz_set, amp, volt, dc_bus_volt, vfd_status, fault_code, kw, torque_current, vfd_temp, pf_factor, fault_hz,
            fault_current,fault_bus_volt, fault_status, dc_bus_ripple_v, vfd_frequence, lm_time
            ) values ('{dt_str}',{dt_stamp},'M114','6','{eqp_id_M114[0]}','{eqp_id_M114[1]}','{eqp_id_M114[2]}','{eqp_id_M114[3]}',
            '{eqp_id_M114[4]}','{eqp_id_M114[5]}','{eqp_id_M114[6]}','{eqp_id_M114[7]}','{eqp_id_M114[8]}','{eqp_id_M114[9]}',
            '{eqp_id_M114[10]}','{eqp_id_M114[11]}','{eqp_id_M114[12]}','{eqp_id_M114[13]}','{eqp_id_M114[14]}','{eqp_id_M114[15]}',
            '{eqp_id_M114[17]}','{lm_time}')"""
            try:
                cur.execute(insert_sql_14)
            except:
                write_back_sql("M114_data_meter_sql",insert_sql_14)
            else:
                print("M114 插入数据成功.......")

        if eqp_id_M115 == None or  eqp_id_M115 == "None" :
            pass
        else:
            insert_sql_15 = f"""insert into tb_eqp_min_data_meter (time_to_interval, timestamp_to_interval, unit_id, station_id,
            hz, hz_set, amp,volt, dc_bus_volt, vfd_status, fault_code, kw, torque_current, vfd_temp, pf_factor, fault_hz,
            fault_current,fault_bus_volt, fault_status, dc_bus_ripple_v, vfd_frequence, lm_time
            ) values ('{dt_str}',{dt_stamp},'M115','1','{eqp_id_M115[0]}','{eqp_id_M115[1]}','{eqp_id_M115[2]}','{eqp_id_M115[3]}',
            '{eqp_id_M115[4]}','{eqp_id_M115[5]}','{eqp_id_M115[6]}','{eqp_id_M115[7]}','{eqp_id_M115[8]}','{eqp_id_M115[9]}',
            '{eqp_id_M115[10]}','{eqp_id_M115[11]}','{eqp_id_M115[12]}','{eqp_id_M115[13]}','{eqp_id_M115[14]}','{eqp_id_M115[15]}',
            '{eqp_id_M115[17]}','{lm_time}')"""
            try:
                cur.execute(insert_sql_15)
            except:
                write_back_sql("M115_data_meter_sql",insert_sql_15)
            else:
                print("M115 插入数据成功.......")

        if eqp_id_M116 == None or  eqp_id_M116 == "None" :
            pass
        else:        
            insert_sql_16 = f"""insert into tb_eqp_min_data_meter (time_to_interval, timestamp_to_interval, unit_id, station_id,
            hz, hz_set, amp, volt, dc_bus_volt, vfd_status, fault_code, kw, torque_current, vfd_temp, pf_factor, fault_hz,
            fault_current,fault_bus_volt, fault_status, dc_bus_ripple_v, vfd_frequence, lm_time
            ) values ('{dt_str}',{dt_stamp},'M116','2','{eqp_id_M116[0]}','{eqp_id_M116[1]}','{eqp_id_M116[2]}','{eqp_id_M116[3]}',
            '{eqp_id_M116[4]}','{eqp_id_M116[5]}','{eqp_id_M116[6]}','{eqp_id_M116[7]}','{eqp_id_M116[8]}','{eqp_id_M116[9]}',
            '{eqp_id_M116[10]}','{eqp_id_M116[11]}','{eqp_id_M116[12]}','{eqp_id_M116[13]}','{eqp_id_M116[14]}','{eqp_id_M116[15]}',
            '{eqp_id_M116[17]}','{lm_time}')"""
            try:
                cur.execute(insert_sql_16)
            except:
                write_back_sql("M116_date_meter_sql",insert_sql_16)
            else:
                print("M116 插入数据成功.......")

        if eqp_id_M117 == None or  eqp_id_M117 == "None" :
            pass
        else:  
            insert_sql_18 = f"""insert into tb_eqp_min_data_meter (time_to_interval, timestamp_to_interval, unit_id, station_id,
            hz, hz_set, amp, volt, dc_bus_volt, vfd_status, fault_code, kw, torque_current, vfd_temp, pf_factor, fault_hz,
            fault_current, fault_bus_volt, fault_status, dc_bus_ripple_v,vfd_frequence, lm_time
            ) values ('{dt_str}',{dt_stamp},'M117','3','{eqp_id_M117[0]}','{eqp_id_M117[1]}','{eqp_id_M117[2]}','{eqp_id_M117[3]}',
            '{eqp_id_M117[4]}','{eqp_id_M117[5]}','{eqp_id_M117[6]}','{eqp_id_M117[7]}','{eqp_id_M117[8]}','{eqp_id_M117[9]}',
            '{eqp_id_M117[10]}','{eqp_id_M117[11]}','{eqp_id_M117[12]}','{eqp_id_M117[13]}','{eqp_id_M117[14]}','{eqp_id_M117[15]}',
            '{eqp_id_M117[17]}','{lm_time}')"""
            try:
                cur.execute(insert_sql_18)
            except:
                write_back_sql("M117_data_meter_sql",insert_sql_18)
            else:
                print("M117 插入数据成功.......")

    elif msg.topic =="guanying/24registers":
        # 18溶氧 ,19 VFD frequence, 20 21 lo hi wh meter,22 [motor 1,2] button,23 24 溶氧
        eqp_id_M105_M118 = data["value"]["M105_M118"]
        eqp_id_M107_M119 = data["value"]["M107_M119"]
        lm_time = datetime.datetime.fromtimestamp(int(datetime.datetime.now().timestamp()+8*60*60)).strftime("%Y-%m-%d %H:%M:%S")

        if eqp_id_M105_M118 == None or  eqp_id_M105_M118 == "None" :
            pass
        else:
            # GET 17TH WORD AND GET ITS 3RD BIT
            POS = decode_to_bits(eqp_id_M105_M118[16])[2]

            # CHECK THE SECOND MOTOR IS RUNNING OR NOT. IF TRUE, EXECUTE THE FIRST BLOCK, ELSE EXECUTE SECOND.
            if POS:
                insert_sql_5 = f"""insert into tb_eqp_min_data_meter (time_to_interval, timestamp_to_interval, unit_id, station_id,
                hz, hz_set, amp, volt, dc_bus_volt, vfd_status, fault_code, kw,torque_current, vfd_temp, pf_factor, fault_hz,
                fault_current,fault_bus_volt, fault_status, dc_bus_ripple_v, vfd_frequence, lm_time
                ) values ('{dt_str}',{dt_stamp},'M118','4','{eqp_id_M105_M118[0]}','{eqp_id_M105_M118[1]}','{eqp_id_M105_M118[2]}',
                '{eqp_id_M105_M118[3]}','{eqp_id_M105_M118[4]}','{eqp_id_M105_M118[5]}','{eqp_id_M105_M118[6]}','{eqp_id_M105_M118[7]}',
                '{eqp_id_M105_M118[8]}','{eqp_id_M105_M118[9]}','{eqp_id_M105_M118[10]}','{eqp_id_M105_M118[11]}','{eqp_id_M105_M118[12]}',
                '{eqp_id_M105_M118[13]}','{eqp_id_M105_M118[14]}','{eqp_id_M105_M118[15]}','{eqp_id_M105_M118[18]}','{lm_time}')"""
            else:
                insert_sql_5 = f"""insert into tb_eqp_min_data_meter (time_to_interval, timestamp_to_interval, unit_id, station_id,
                hz, hz_set, amp, volt, dc_bus_volt, vfd_status, fault_code, kw,torque_current, vfd_temp, pf_factor, fault_hz,
                fault_current,fault_bus_volt, fault_status, dc_bus_ripple_v, vfd_frequence, lm_time
                ) values ('{dt_str}',{dt_stamp},'M105','4','{eqp_id_M105_M118[0]}','{eqp_id_M105_M118[1]}','{eqp_id_M105_M118[2]}',
                '{eqp_id_M105_M118[3]}','{eqp_id_M105_M118[4]}','{eqp_id_M105_M118[5]}','{eqp_id_M105_M118[6]}','{eqp_id_M105_M118[7]}',
                '{eqp_id_M105_M118[8]}','{eqp_id_M105_M118[9]}','{eqp_id_M105_M118[10]}','{eqp_id_M105_M118[11]}','{eqp_id_M105_M118[12]}',
                '{eqp_id_M105_M118[13]}','{eqp_id_M105_M118[14]}','{eqp_id_M105_M118[15]}','{eqp_id_M105_M118[18]}','{lm_time}')"""
            try:
                cur.execute(insert_sql_5)
            except:
                write_back_sql("M105_M118_data_meter_sql",insert_sql_5)
            else:
                print("M105/118 插入数据成功.......")

            # GET 23RD 24TH WORD AND THE CONVERT TO A 32BIT FLOAT NUMBER
            temp = decode_to_32bit_float(eqp_id_M105_M118[22:24])

            insert_sql_5_2 = f"""insert into tb_eqp_min_data_sensor (time_to_interval, timestamp_to_interval, unit_id, station_id,
            DO, temperature, lm_time
            ) values ('{dt_str}', {dt_stamp},'S104','4','{eqp_id_M105_M118[17]}','{temp}','{lm_time}')"""
            try:
                cur.execute(insert_sql_5_2)
            except:
                write_back_sql("S104_sensor_sql",insert_sql_5_2)
            else:
                print("S104 插入数据成功.......")
            
            # GET 20TH 21ST WORD AND THE CONVERT TO A 32BIT FLOAT NUMBER
            wh_meter = decode_to_32bit_float(eqp_id_M105_M118[19:21])

            insert_sql_5_3 = f"""insert into tb_watt_hour_meter_list (time_to_interval, timestamp_to_interval, station_id,
            wh_meter_name, kwh,lm_time) values ('{dt_str}', {dt_stamp},'5','ROI_5_WH','{wh_meter}','{lm_time}')"""
            cur.execute(insert_sql_5_3)

            # 曝氣池圖控馬達 鼓風機按鈕 1 & 2 (bit)
            decoder_button = BinaryPayloadDecoder.fromRegisters([eqp_id_M105_M118[21]], byteorder=Endian.Big, wordorder=Endian.Little)
            on_off_s = [decoder_button.decode_bits() for _ in range(2)]



        if eqp_id_M107_M119 == None or  eqp_id_M107_M119 == "None" :
            pass
        else:
            # GET 17TH WORD AND GET ITS 3RD BIT
            POS = decode_to_bits(eqp_id_M107_M119[16])[2]

            # CHECK THE SECOND MOTOR IS RUNNING OR NOT. IF TRUE, EXECUTE THE FIRST BLOCK, ELSE EXECUTE SECOND.
            if POS:
                insert_sql_7 = f"""insert into tb_eqp_min_data_meter (time_to_interval, timestamp_to_interval, unit_id, station_id,
                hz, hz_set, amp, volt, dc_bus_volt, vfd_status, fault_code, kw, torque_current, vfd_temp, pf_factor, fault_hz,
                fault_current, fault_bus_volt, fault_status, dc_bus_ripple_v,vfd_frequence, lm_time
                ) values ('{dt_str}', {dt_stamp},'M119','1','{eqp_id_M107_M119[0]}','{eqp_id_M107_M119[1]}','{eqp_id_M107_M119[2]}',
                '{eqp_id_M107_M119[3]}', '{eqp_id_M107_M119[4]}','{eqp_id_M107_M119[5]}','{eqp_id_M107_M119[6]}','{eqp_id_M107_M119[7]}',
                '{eqp_id_M107_M119[8]}', '{eqp_id_M107_M119[9]}','{eqp_id_M107_M119[10]}','{eqp_id_M107_M119[11]}', '{eqp_id_M107_M119[12]}',
                '{eqp_id_M107_M119[13]}', '{eqp_id_M107_M119[14]}','{eqp_id_M107_M119[15]}','{eqp_id_M107_M119[18]}', '{lm_time}')"""
            else:
                insert_sql_7 = f"""insert into tb_eqp_min_data_meter (time_to_interval, timestamp_to_interval, unit_id, station_id,
                hz, hz_set, amp, volt, dc_bus_volt, vfd_status, fault_code, kw, torque_current, vfd_temp, pf_factor, fault_hz,
                fault_current, fault_bus_volt, fault_status, dc_bus_ripple_v,vfd_frequence, lm_time
                ) values ('{dt_str}', {dt_stamp},'M107','1','{eqp_id_M107_M119[0]}','{eqp_id_M107_M119[1]}','{eqp_id_M107_M119[2]}',
                '{eqp_id_M107_M119[3]}', '{eqp_id_M107_M119[4]}','{eqp_id_M107_M119[5]}','{eqp_id_M107_M119[6]}','{eqp_id_M107_M119[7]}',
                '{eqp_id_M107_M119[8]}', '{eqp_id_M107_M119[9]}','{eqp_id_M107_M119[10]}','{eqp_id_M107_M119[11]}', '{eqp_id_M107_M119[12]}',
                '{eqp_id_M107_M119[13]}', '{eqp_id_M107_M119[14]}','{eqp_id_M107_M119[15]}','{eqp_id_M107_M119[18]}', '{lm_time}')"""
            try:
                cur.execute(insert_sql_7)
            except:
                write_back_sql("M107_M119_data_meter_sql",insert_sql_7)
            else:
                print("M107119 插入数据成功.......")

            # GET 23RD 24TH WORD AND CONVERT TO A 32BIT FLOAT NUMBER
            temp = decode_to_32bit_float(eqp_id_M107_M119[22:24])
            insert_sql_7_2 = f"""insert into tb_eqp_min_data_sensor (time_to_interval, timestamp_to_interval, unit_id, station_id,
            DO, temperature, lm_time
            ) values ('{dt_str}', {dt_stamp},'S106','5','{eqp_id_M107_M119[17]}','{temp}','{lm_time}')"""
            try:
                cur.execute(insert_sql_7_2)
            except:
                write_back_sql("S106_sensor_sql",insert_sql_7_2)
            else:
                print("S106 插入数据成功.......")

            # GET 20TH 21RD WORD AND CONVERT TO A 32BIT FLOAT NUMBER
            wh_meter = decode_to_32bit_float(eqp_id_M107_M119[19:21])
            insert_sql_7_3 = f"""insert into tb_watt_hour_meter_list (time_to_interval, timestamp_to_interval, station_id,
            wh_meter_name, kwh,lm_time) values ('{dt_str}', {dt_stamp},'4','ROI_4_WH','{wh_meter}','{lm_time}')"""
            try:
                cur.execute(insert_sql_7_3)
            except:
                write_back_sql("ROI_4_WH_sql",insert_sql_7_3)
            else: 
                print("ROI_4 插入数据成功.......")
            # 曝氣池圖控馬達 鼓風機按鈕 1 & 2 (bit)
            decoder_button = BinaryPayloadDecoder.fromRegisters([eqp_id_M107_M119[21]], byteorder=Endian.Big, wordorder=Endian.Little)
            on_off_s = [decoder_button.decode_bits() for _ in range(2)]



    elif msg.topic =="guanying/7motors":
        eqp_id_VFD_info49 = data["value"]["VFD_info49"]
        # 寫入DB時間
        lm_time = datetime.datetime.fromtimestamp(int(datetime.datetime.now().timestamp()+8*60*60)).strftime("%Y-%m-%d %H:%M:%S")
        
        insert_sql_17_1 = f"""insert into tb_eqp_min_data_meter (time_to_interval, timestamp_to_interval, unit_id, station_id,
        hz_set, hz, amp, dc_bus_volt, volt, kw, vfd_temp, lm_time
        ) values ( '{dt_str}', {dt_stamp},'M121','P','{eqp_id_VFD_info49[0]*100}','{eqp_id_VFD_info49[1]*100}',
        '{eqp_id_VFD_info49[2]*10}','{eqp_id_VFD_info49[3]*10}','{eqp_id_VFD_info49[4]*10}','{eqp_id_VFD_info49[5]*10}',
        '{eqp_id_VFD_info49[6]*10}','{lm_time}')"""
        try:
            cur.execute(insert_sql_17_1)
        except:
            write_back_sql("M121_data_meter_sql",insert_sql_17_1)
        else:
            print("M121 插入数据成功.......")
        

        insert_sql_17_2 = f"""insert into tb_eqp_min_data_meter (time_to_interval, timestamp_to_interval, unit_id, station_id,
        hz_set, hz, amp, dc_bus_volt, volt, kw, vfd_temp, lm_time
        ) values ( '{dt_str}', {dt_stamp},'M122','P','{eqp_id_VFD_info49[7]*100}','{eqp_id_VFD_info49[8]*100}',
        '{eqp_id_VFD_info49[9]*10}','{eqp_id_VFD_info49[10]*10}','{eqp_id_VFD_info49[11]*10}','{eqp_id_VFD_info49[12]*10}',
        '{eqp_id_VFD_info49[13]*10}','{lm_time}')"""
        try:
            cur.execute(insert_sql_17_2)
        except:
            write_back_sql("M122_data_meter_sql",insert_sql_17_2)
        else:
            print("M122 插入数据成功.......")


        insert_sql_17_3 = f"""insert into tb_eqp_min_data_meter (time_to_interval, timestamp_to_interval, unit_id, station_id,
        hz_set, hz, amp, dc_bus_volt, volt, kw, vfd_temp, lm_time
        ) values ( '{dt_str}', {dt_stamp},'M123','P','{eqp_id_VFD_info49[14]*100}','{eqp_id_VFD_info49[15]*100}',
        '{eqp_id_VFD_info49[16]*10}','{eqp_id_VFD_info49[17]*10}','{eqp_id_VFD_info49[18]*10}','{eqp_id_VFD_info49[19]*10}',
        '{eqp_id_VFD_info49[20]*10}','{lm_time}')"""
        try:
            cur.execute(insert_sql_17_3)
        except:
            write_back_sql("M123_data_meter_sql",insert_sql_17_3)
        else:
            print("M123 插入数据成功.......")

        insert_sql_17_4 = f"""insert into tb_eqp_min_data_meter (time_to_interval, timestamp_to_interval, unit_id, station_id,
        hz_set, hz, amp, dc_bus_volt, volt, kw, vfd_temp, lm_time
        ) values ( '{dt_str}', {dt_stamp},'M124','P','{eqp_id_VFD_info49[21]*100}','{eqp_id_VFD_info49[22]*100}',
        '{eqp_id_VFD_info49[23]*10}','{eqp_id_VFD_info49[24]*10}','{eqp_id_VFD_info49[25]*10}','{eqp_id_VFD_info49[26]*10}',
        '{eqp_id_VFD_info49[27]*10}','{lm_time}')"""
        try:
            cur.execute(insert_sql_17_4)
        except:
            write_back_sql("M124_data_meter_sql",insert_sql_17_3)
        else:
            print("M124 插入数据成功.......")            


        insert_sql_17_5 = f"""insert into tb_eqp_min_data_meter (time_to_interval, timestamp_to_interval, unit_id, station_id,
        hz_set, hz, amp, dc_bus_volt, volt, kw, vfd_temp, lm_time
        ) values ( '{dt_str}', {dt_stamp},'M125','3D','{eqp_id_VFD_info49[28]*100}','{eqp_id_VFD_info49[29]*100}',
        '{eqp_id_VFD_info49[30]*10}','{eqp_id_VFD_info49[31]*10}','{eqp_id_VFD_info49[32]*10}','{eqp_id_VFD_info49[33]*10}',
        '{eqp_id_VFD_info49[34]*10}','{lm_time}')"""
        try:
            cur.execute(insert_sql_17_5)
        except:
            write_back_sql("M125_data_meter_sql",insert_sql_17_5)
        else:
            print("M125 插入数据成功.......")   

        insert_sql_17_6 = f"""insert into tb_eqp_min_data_meter (time_to_interval, timestamp_to_interval, unit_id, station_id,
        hz_set, hz, amp, dc_bus_volt, volt, kw, vfd_temp, lm_time
        ) values ( '{dt_str}', {dt_stamp},'M126','3D','{eqp_id_VFD_info49[35]*100}','{eqp_id_VFD_info49[36]*100}',
        '{eqp_id_VFD_info49[37]*10}','{eqp_id_VFD_info49[38]*10}','{eqp_id_VFD_info49[39]*10}','{eqp_id_VFD_info49[40]*10}',
        '{eqp_id_VFD_info49[41]*10}','{lm_time}')"""
        try:
            cur.execute(insert_sql_17_6)
        except:
            write_back_sql("M126_data_meter_sql",insert_sql_17_6)
        else:
            print("M126 插入数据成功.......")              
        

        insert_sql_17_7 = f"""insert into tb_eqp_min_data_meter (time_to_interval, timestamp_to_interval, unit_id, station_id,
        hz_set, hz, amp, dc_bus_volt, volt, kw, vfd_temp, lm_time
        ) values ( '{dt_str}', {dt_stamp},'M127','3D','{eqp_id_VFD_info49[42]*100}','{eqp_id_VFD_info49[43]*100}',
        '{eqp_id_VFD_info49[44]*10}','{eqp_id_VFD_info49[45]*10}','{eqp_id_VFD_info49[46]*10}','{eqp_id_VFD_info49[47]*10}',
        '{eqp_id_VFD_info49[48]*10}','{lm_time}')"""
        try:
            cur.execute(insert_sql_17_7)
        except:
            write_back_sql("M127_data_meter_sql",insert_sql_17_7)
        else:
            print("M127 插入数据成功.......")              


    elif msg.topic =="guanying/water_sensor":
        eqp_id_water_sensor = data["value"]["water_sensor"]
        lm_time = datetime.datetime.fromtimestamp(int(datetime.datetime.now().timestamp()+8*60*60)).strftime("%Y-%m-%d %H:%M:%S")
        insert_sql_18_1 = f"""insert into tb_eqp_min_data_sensor (time_to_interval, timestamp_to_interval, unit_id, station_id,
        liquid_level, lm_time
        ) values (  '{dt_str}', {dt_stamp},'S116','P','{eqp_id_water_sensor[0]*10}','{lm_time}')"""
        try:
            cur.execute(insert_sql_18_1)
        except:
            write_back_sql("S116_data_meter_sql",insert_sql_18_1)
        else:
            print("S116 插入数据成功.......")  

        insert_sql_18_2 = f"""insert into tb_eqp_min_data_sensor (time_to_interval, timestamp_to_interval, unit_id, station_id,
        liquid_level, lm_time
        ) values (  '{dt_str}', {dt_stamp},'S117','1','{eqp_id_water_sensor[1]*10}','{lm_time}')"""
        try:
            cur.execute(insert_sql_18_2)
        except:
            write_back_sql("S117_data_meter_sql",insert_sql_18_2)
        else:
            print("S117 插入数据成功.......")

        insert_sql_18_3 = f"""insert into tb_eqp_min_data_sensor (time_to_interval, timestamp_to_interval, unit_id, station_id, flow,
        lm_time
        ) values (  '{dt_str}', {dt_stamp},'S118','D','{eqp_id_water_sensor[2]*10}','{lm_time}')"""
        try:
            cur.execute(insert_sql_18_3)
        except:
            write_back_sql("S118_data_meter_sql",insert_sql_18_3)
        else:
            print("S118 插入数据成功.......")              

        insert_sql_18_4 = f"""insert into tb_eqp_min_data_sensor (time_to_interval, timestamp_to_interval, unit_id, station_id, flow,
        lm_time
        ) values (  '{dt_str}', {dt_stamp},'S119','D','{eqp_id_water_sensor[3]*10}','{lm_time}')"""
        try:
            cur.execute(insert_sql_18_4)
        except:
            write_back_sql("S119_data_meter_sql",insert_sql_18_4)
        else:
            print("S119 插入数据成功.......")              

        insert_sql_18_5 = f"""insert into tb_eqp_min_data_sensor (time_to_interval, timestamp_to_interval, unit_id, station_id,
        threshold, lm_time
        ) values (  '{dt_str}', {dt_stamp},'S120','R','{eqp_id_water_sensor[4]*10}','{lm_time}')"""
        try:
            cur.execute(insert_sql_18_5)
        except:
            write_back_sql("S120_data_meter_sql",insert_sql_18_5)
        else:
            print("S120 插入数据成功.......")

        insert_sql_18_6 = f"""insert into tb_eqp_min_data_sensor (time_to_interval, timestamp_to_interval, unit_id, station_id,
        liquid_level, lm_time
        ) values (  '{dt_str}', {dt_stamp},'S121','3D','{eqp_id_water_sensor[5]*10}','{lm_time}')"""
        try:
            cur.execute(insert_sql_18_6)
        except:
            write_back_sql("S121_data_meter_sql",insert_sql_18_6)
        else:
            print("S121 插入数据成功.......")

def on_log(client, userdata, level, buf):
    print("log: ",buf)


mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.on_log = on_log
mqtt_client.connect("3.34.97.49",1883,60)
mqtt_client.loop_forever()
