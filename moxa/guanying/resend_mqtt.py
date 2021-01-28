
import paho.mqtt.client as mqtt
import json
import os
import datetime
mqtt_client = mqtt.Client()
mqtt_client.connect("3.34.97.49",1883,60)

date = datetime.datetime.now().strftime("%Y%m%d")
filepath ="/home/moxa/guanying/mqttcsv/{date}/{date}_mqtt.csv".format(date=date)
print(filepath)
if os.path.isfile(filepath):
    print("file exists!")
    with open(filepath,"r") as f:
        lines = f.readlines()
        for line in lines:
            datas = line.strip().split("@")
            topic = datas[0]
            values = eval(datas[1])
            t = eval(datas[2])
            payload = {"time":t,"value":values}
            try:
                mqtt_client.publish(topic,json.dumps(payload),0,True)
            except:
                filename = "{date}.log".format(date=date)
                with open(filename,"a") as f:
                    payloads = json.dumps(payload)
                
                    content = "[{date}][{filepath}]: mqtt publish failed!\n[{date}][Content]: {payloads}".format(date=date,
                                                                                                                 filepath=filepath,
                                                                                                                 payloads=payloads)
                    f.write(content)
