import time
from opcua import Client, ua

class SubHandler:
    def datachange_notification(self, node, val, data):
        print(f"📈 DataChange from {node}: {val}")

if __name__ == "__main__":
    endpoint_url = "opc.tcp://srv2:9409/DvOpcUaServer"
    CLIENT_CERT = "/home/erfan/OPC-CERT/client_cert_opcua.der"
    CLIENT_KEY = "/home/erfan/OPC-CERT/client_key.pem"

    client = Client(endpoint_url)
    client.set_security_string(
        "Basic256Sha256,SignAndEncrypt,"
        + CLIENT_CERT + ","
        + CLIENT_KEY
    )

    client.application_uri = "urn:oth"
    client.application_name = "oth"

    try:
        client.connect()
        print("✅ Secure OPC UA session established.")

        # مسیر نود PV (متغیر واقعی)
        node_path = [
            "0:Objects",
            "2:DA",
            "2:MODULES",
            "2:BOILER",
            "2:BOIL_COMB_BURN",
            "2:AI",
            "2:AI1",
            "2:ALARM_HYS",
            "2:CV"
        ]
        target_node = client.get_root_node().get_child(node_path)
        print("🎯 Target node:", target_node)

        # خواندن مقدار اولیه
        try:
            val = target_node.get_value()
            print("📊 Current value:", val)
        except Exception as e:
            print("⚠️ Could not read value:", e)

        # ایجاد subscription
        handler = SubHandler()
        sub = client.create_subscription(500, handler)
        handle = sub.subscribe_data_change(target_node)

        print("👂 Listening for changes... (Ctrl+C to stop)")
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n🛑 Stopped by user.")
    except Exception as e:
        print("❌ Runtime error:", e)
    finally:
        try:
            client.disconnect()
            print("🔒 Disconnected from server.")
        except Exception as e:
            print("⚠️ disconnect warning:", e)
