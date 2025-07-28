import paho.mqtt.client as mqtt

def on_connect(client, userdata, flags, reasonCode, properties=None):
    if reasonCode == 0:
        print("✅ 已连接 MQTT Broker！")
        # 订阅多个主题
        topics = [
            ("PWR/ERCOTLMP", 0),
            ("PWR/ERCOTLMP15", 0),
            ("TEST/PWR/ERCOTLMP15", 0),
            ("BTC/CW/CMD", 0)
        ]
        client.subscribe(topics)
        print("📡 正在订阅：", [t[0] for t in topics])
    else:
        print(f"❌ 连接失败，返回码: {reasonCode}")

def on_message(client, userdata, msg):
    print(f"📨 主题: {msg.topic} | 消息: {msg.payload.decode()}")

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="test_subscriber", protocol=mqtt.MQTTv5)
client.username_pw_set("mqttusr3", "uu56890CCE#218")
client.on_connect = on_connect
client.on_message = on_message

client.connect("10.10.112.130", 1883, 60)
client.loop_forever()