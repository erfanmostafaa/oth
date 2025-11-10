import os
import json
import time
import signal
import logging
import threading
import itertools
from datetime import datetime, timezone
from typing import Dict, Tuple, List

from opcua import Client, ua
import paho.mqtt.client as mqtt

# =======================
#   env config
# =======================
OPCUA_ENDPOINT        = os.getenv("OPCUA_ENDPOINT", "opc.tcp://127.0.0.1:4840")
OPCUA_SECURITY        = os.getenv("OPCUA_SECURITY", "NONE").strip()
OPCUA_USER            = os.getenv("OPCUA_USER", "").strip()
OPCUA_PASS            = os.getenv("OPCUA_PASS", "").strip()

SITE                  = os.getenv("SITE", "SITE1")
UNIT                  = os.getenv("UNIT", "UNIT1")
PLC                   = os.getenv("PLC", "PLC1")

MQTT_HOST             = os.getenv("MQTT_HOST", "emqx")
MQTT_PORT             = int(os.getenv("MQTT_PORT", "1883"))
MQTT_QOS              = int(os.getenv("MQTT_QOS", "1"))

TOPIC_BASE            = os.getenv("TOPIC_BASE", f"plant/{SITE}/{UNIT}/{PLC}/cv")
PUBSUB_TOPIC          = os.getenv("PUBSUB_TOPIC", f"opcua/json/{SITE}-{UNIT}-{PLC}")

PUBLISH_INTERVAL_MS   = int(os.getenv("PUBLISH_INTERVAL_MS", "250"))
LOG_LEVEL             = os.getenv("LOG_LEVEL", "INFO").upper()

# هر چند ثانیه یه بار همه CV ها رو دوباره بفرست
PERIODIC_REPUBLISH_SEC = int(os.getenv("PERIODIC_REPUBLISH_SEC", "5"))

# برای اینکه دلتاوی بی‌نهایت درخت نداشته باشه
MAX_BROWSE_DEPTH       = int(os.getenv("MAX_BROWSE_DEPTH", "15"))

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s: %(message)s"
)
log = logging.getLogger("opcua2mqtt")

stop = False


def iso_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def valid_qos(q: int) -> int:
    return q if q in (0, 1, 2) else 0


class PubBuffer:
    """یه بافر کوچیک برای تجمیع تغییرها"""
    def __init__(self):
        self._lock = threading.Lock()
        self._seq = itertools.count(1)
        self._pending: Dict[str, object] = {}

    def add(self, tagname: str, value):
        with self._lock:
            self._pending[tagname] = value

    def drain(self) -> Dict[str, object]:
        with self._lock:
            d = dict(self._pending)
            self._pending.clear()
            return d

    def next(self) -> int:
        return next(self._seq)


# =======================
#   MQTT
# =======================
def mqtt_connect():
    """زودتر از همه وصل شو به MQTT"""
    qos = valid_qos(MQTT_QOS)
    client_id = f"opcua2mqtt-{int(time.time())}"

    c = mqtt.Client(
        client_id=client_id,
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        protocol=mqtt.MQTTv5,
    )

    # LWT
    c.will_set(f"{PUBSUB_TOPIC}/$status", payload="offline", qos=qos, retain=True)

    def on_connect(client, userdata, flags, reason_code, properties):
        log.info("MQTT connected rc=%s", reason_code)
        client.publish(f"{PUBSUB_TOPIC}/$status", "online", qos=qos, retain=True)

    def on_disconnect(client, userdata, reason_code, properties):
        log.warning("MQTT disconnected rc=%s", reason_code)

    c.on_connect = on_connect
    c.on_disconnect = on_disconnect

    delay = 1
    while not stop:
        try:
            log.info("Connecting MQTT to %s:%s", MQTT_HOST, MQTT_PORT)
            c.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
            c.loop_start()
            return c
        except Exception as e:
            log.error("MQTT connect failed: %s", e)
            time.sleep(delay)
            delay = min(delay * 2, 30)


def mqtt_publish(c, topic: str, payload: dict, retain=False, qos=None):
    if qos is None:
        qos = valid_qos(MQTT_QOS)
    data = json.dumps(payload, ensure_ascii=False)
    info = c.publish(topic, data, qos=qos, retain=retain)
    if info.rc != mqtt.MQTT_ERR_SUCCESS:
        log.warning("Publish rc=%s topic=%s", info.rc, topic)


# =======================
#   OPC UA
# =======================
def _apply_security(client: Client):
    sec = OPCUA_SECURITY.upper()
    if sec == "" or sec == "NONE":
        log.warning("OPCUA_SECURITY=NONE -> no security")
        return
    # مثال: Basic256Sha256,SignAndEncrypt,./certs/client.pem,./certs/client-key.pem
    parts = [p.strip() for p in OPCUA_SECURITY.split(",")]
    if len(parts) < 4:
        raise RuntimeError(f"OPCUA_SECURITY malformed: {OPCUA_SECURITY}")
    client.set_security_string(OPCUA_SECURITY)


def build_opcua_client() -> Client:
    log.info("Connecting OPC UA to %s", OPCUA_ENDPOINT)
    client = Client(OPCUA_ENDPOINT)
    client.application_uri = "urn:opcua2mqtt"
    client.application_name = "opcua2mqtt"
    _apply_security(client)
    if OPCUA_USER and OPCUA_PASS:
        client.set_user(OPCUA_USER)
        client.set_password(OPCUA_PASS)
    return client


def browse_children(node, client: Client):
    out = []
    refs = node.get_references(
        ua.ObjectIds.HierarchicalReferences,
        direction=ua.BrowseDirection.Forward
    )
    for ref in refs:
        try:
            child = client.get_node(ref.NodeId)
            dn = child.get_display_name().Text
            out.append((dn, child))
        except Exception as e:
            log.warning("browse child err: %s", e)
    return out


def crawl_all_for_cv(client: Client) -> Dict[str, Dict]:
    """
    از Objects شروع می‌کنیم و هر Variable که اسمش CV یا ...CV باشه رو جمع می‌کنیم.
    عمق گشتن هم با MAX_BROWSE_DEPTH محدود شده.
    """
    objects = client.get_objects_node()
    result: Dict[str, Dict] = {}
    stack: List[Tuple[List[str], object, int]] = [([], objects, 0)]

    while stack:
        path, node, depth = stack.pop()
        if depth > MAX_BROWSE_DEPTH:
            continue

        for (name, child) in browse_children(node, client):
            new_path = path + [name]
            try:
                node_class = child.get_node_class()
            except Exception:
                node_class = None

            name_lower = (name or "").lower()
            if node_class == ua.NodeClass.Variable and (name_lower == "cv" or name_lower.endswith(".cv")):
                tagname = ".".join(new_path)
                nodeid_str = child.nodeid.to_string()
                result[tagname] = {
                    "node": child,
                    "nodeid_str": nodeid_str,
                }
                log.info("FOUND CV TAG: %s -> %s", tagname, nodeid_str)

            # ادامه‌ی گشتن
            if node_class in (
                ua.NodeClass.Object,
                ua.NodeClass.Unspecified,
                ua.NodeClass.VariableType,
                None
            ):
                stack.append((new_path, child, depth + 1))

    return result


def periodic_republish(mqttc, cv_map: Dict[str, Dict], interval_sec: int = 5):
    """هر چند ثانیه همه رو دوباره بفرست"""
    global stop
    while not stop:
        for tagname, info in cv_map.items():
            node = info["node"]
            nodeid_str = info["nodeid_str"]
            try:
                v = node.get_value()
            except Exception:
                continue
            payload = {
                "tag": tagname,
                "nodeid": nodeid_str,
                "value": v,
                "ts": iso_now(),
            }
            mqtt_publish(mqttc, f"{TOPIC_BASE}/{tagname}", payload, retain=False)
        time.sleep(interval_sec)


# =======================
#   main run
# =======================
def run_once():
    global stop

    # 1) اول MQTT
    mqttc = mqtt_connect()

    # 2) بعد OPC UA
    client = build_opcua_client()
    client.connect()
    log.info("✅ OPC UA session active")

    # 3) گشتن دنبال CV
    cv_map = crawl_all_for_cv(client)
    if not cv_map:
        log.warning("No CV tags found under Objects")

    # 4) انتشار اولیه (retained)
    for tagname, info in cv_map.items():
        node = info["node"]
        nodeid_str = info["nodeid_str"]
        try:
            v = node.get_value()
        except Exception as e:
            log.error("Initial read failed for %s: %s", tagname, e)
            continue
        payload = {
            "tag": tagname,
            "nodeid": nodeid_str,
            "value": v,
            "ts": iso_now(),
        }
        mqtt_publish(mqttc, f"{TOPIC_BASE}/init/{tagname}", payload, retain=True)

    # نقشه‌ی برعکس برای دیتاچنج
    node_to_meta: Dict[str, Dict[str, str]] = {}
    for tagname, info in cv_map.items():
        nodeid_str = info["nodeid_str"]
        node_to_meta[nodeid_str] = {"tagname": tagname, "nodeid": nodeid_str}

    # ترد ارسال دوره‌ای
    repub_thread = threading.Thread(
        target=periodic_republish,
        args=(mqttc, cv_map, PERIODIC_REPUBLISH_SEC),
        daemon=True,
    )
    repub_thread.start()

    agg = PubBuffer()

    class Handler:
        def datachange_notification(self_inner, node, val, data):
            nodekey = node.nodeid.to_string()
            meta = node_to_meta.get(nodekey, {"tagname": nodekey, "nodeid": nodekey})
            tagname = meta["tagname"]
            nodeid_str = meta["nodeid"]
            log.info("DATA CHANGE %s -> %s", tagname, val)
            payload = {
                "tag": tagname,
                "nodeid": nodeid_str,
                "value": val,
                "ts": iso_now(),
            }
            mqtt_publish(mqttc, f"{TOPIC_BASE}/{tagname}", payload, retain=False)
            agg.add(tagname, val)

    # سابسکرایب
    if cv_map:
        sub = client.create_subscription(1000, Handler())
        sub.subscribe_data_change([info["node"] for info in cv_map.values()])
        log.info("Subscribed to %d CV tags", len(cv_map))
    else:
        sub = None

    # ترد تجمیع OPC UA PubSub
    def pub_loop():
        while not stop:
            time.sleep(PUBLISH_INTERVAL_MS / 1000.0)
            payloads = agg.drain()
            if not payloads:
                continue
            msg = {
                "MessageId": str(agg.next()),
                "PublisherId": f"{SITE}-{UNIT}-{PLC}",
                "Messages": [
                    {
                        "DataSetWriterId": 1,
                        "SequenceNumber": agg.next(),
                        "MetaDataVersion": {"MajorVersion": 1, "MinorVersion": 0},
                        "Timestamp": iso_now(),
                        "Payload": payloads,
                    }
                ],
            }
            mqtt_publish(mqttc, PUBSUB_TOPIC, msg, retain=False)

    t = threading.Thread(target=pub_loop, daemon=True)
    t.start()

    try:
        while not stop:
            time.sleep(0.5)
    finally:
        try:
            if sub:
                sub.delete()
        except Exception:
            pass
        try:
            client.disconnect()
        except Exception:
            pass
        try:
            mqtt_publish(
                mqttc,
                f"{PUBSUB_TOPIC}/$status",
                {"status": "offline", "ts": iso_now()},
                retain=True,
            )
            mqttc.loop_stop()
            mqttc.disconnect()
        except Exception:
            pass
        log.info("Clean shutdown.")


def main():
    global stop

    def _sig(*_):
        global stop
        stop = True

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    backoff = 1
    while not stop:
        try:
            run_once()
            break
        except Exception as e:
            log.error("main loop error: %s", e)
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)


if __name__ == "__main__":
    main()
