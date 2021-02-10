from serial.tools import list_ports
from serial import Serial
from datetime import datetime
from logging import basicConfig, info, error, DEBUG
from enum import IntEnum
from socket import socket, AF_INET, SOCK_STREAM
from json import loads
import paho.mqtt.client as mqtt


esp8266 = list()
mqtt_queue = "hostqueue"
Success = 5

class Enviroment(IntEnum):
    Kafka = 9092
    MQTT = 1883
    ZooKeeper = 2181
    Postgres = 5432



def check():
    check_bits = 0
    ports = list_ports.comports()
    info("[*] Scanning usb ports.")
    for port, desc, hwid in sorted(ports):
        print("  |-> {}: {} [{}]".format(port, desc, hwid))
        if 'CH340' in desc:
            check_bits += 1
            info(f"[+] Esp8266 Connected to {port} port.")
            esp8266.append(port)
            break
    if not check_bits:
        error('[!] Esp8266 Not Found.Please check your usb connection.')
        # exit(1)
    info("Big data environment is being controlled.")
    for i in Enviroment:
        sock = socket(AF_INET, SOCK_STREAM)
        check_bits += 1
        if not sock.connect_ex(("localhost", i.value)):
            info(f"[+] {i.name} [OK]")            
        else:
            error(f"[!] {i.name} unable to connect.")
        sock.close()
    if check_bits != Success:
        return True


def pushMqtt(**kwargs):
    client.publish(mqtt_queue, "%s, DHT11, %s,%s" % (kwargs['date'], kwargs['temperature'], kwargs['humidity']))
    

if __name__ == "__main__":
    try:
        basicConfig(format='%(asctime)s %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=DEBUG)
        if check():
            exit(1)
        client = mqtt.Client()
        client.connect("localhost", 1883, 60)
        ser = Serial(esp8266[0], 9600)
        while True:
            esp_data = loads(ser.readline().decode('utf-8'))
            info(''.join(["Temperature -> ",str(esp_data['temperature'])," Humidity -> ",str(esp_data['humidity'])]))
            pushMqtt(date=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), temperature=esp_data['temperature'],
                     humidity=esp_data['humidity'])                 
    except KeyboardInterrupt:
        print('Exitting...')
    except Exception as f:
        print(f.args)