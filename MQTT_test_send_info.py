import paho.mqtt.client as mqtt

client = mqtt.Client(client_id="publisher_test", protocol=mqtt.MQTTv5)
client.username_pw_set("mqttusr3", "uu56890CCE#218")
client.connect("10.10.112.130", 1883, 60)

client.publish("TEST/PWR/ERCOTLMP15", payload="test message from Alex Liu", qos=1, retain=True)
print("✅ 已发布测试消息")
client.disconnect()