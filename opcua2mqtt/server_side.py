from opcua import Client

endpoint = "opc.tcp://10.4.0.6:9409/DvOpcUaServer"
client = Client(endpoint)

print(f"🔍 Checking endpoints of {endpoint} ...")

endpoints = client.connect_and_get_server_endpoints()
for i, ep in enumerate(endpoints, start=1):
    print(f"\nEndpoint {i}:")
    print("  URL:             ", ep.EndpointUrl)
    print("  SecurityPolicy:  ", ep.SecurityPolicyUri)
    print("  SecurityMode:    ", ep.SecurityMode)
    if any(proto in ep.EndpointUrl.lower() for proto in ("mqtt", "amqp", "opc.udp")):
        print("  ✅ Possible PubSub transport detected!")
    else:
        print("  ⚠️ Regular client endpoint (session-based).")

app_uri = None
try:
    app_uri = ep.Server.ApplicationUri
except:
    pass
if app_uri:
    print("\nApplicationUri:", app_uri)
    if any(keyword in app_uri.lower() for keyword in ("pubsub", "publisher", "mqtt")):
        print("  ✅ Application URI hints PubSub support.")
