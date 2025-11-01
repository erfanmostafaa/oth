import os
import json
import time
import signal
import logging
import threading
import itertools
from datetime import datetime, timezone
from typing import Dict, List, Tuple

from opcua import Client, ua
import paho.mqtt.client as mqtt


# ==============================
#        CONFIG (env)
# ==============================

OPCUA_ENDPOINT       = os.getenv("OPCUA_ENDPOINT", "opc.tcp://127.0.0.1:4840")
OPCUA_SECURITY       = os.getenv("OPCUA_SECURITY", "").strip()  # "NONE" or "policy,mode,client_cert,key[,server_cert]"
OPCUA_USER           = os.getenv("OPCUA_USER", "").strip()
OPCUA_PASS           = os.getenv("OPCUA_PASS", "").strip()

SITE                 = os.getenv("SITE", "SITE1")
UNIT                 = os.getenv("UNIT", "UNIT1")
PLC                  = os.getenv("PLC",  "PLC1")

MQTT_HOST            = os.getenv("MQTT_HOST", "emqx")
MQTT_PORT            = int(os.getenv("MQTT_PORT", "1883"))
MQTT_QOS             = int(os.getenv("MQTT_QOS", "1"))

TOPIC_BASE           = os.getenv("TOPIC_BASE", f"plant/{SITE}/{UNIT}/{PLC}/cv")
PUBSUB_TOPIC         = os.getenv("PUBSUB_TOPIC", f"opcua/json/{SITE}-{UNIT}-{PLC}")

PUBLISH_INTERVAL_MS  = int(os.getenv("PUBLISH_INTERVAL_MS", "250"))
LOG_LEVEL            = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s: %(message)s"
)
log = logging.getLogger("opcua2mqtt")

stop = False


# ==============================
#        HELPERS
# ==============================

def iso_now() -> str:
    """
    زمان UTC بصورت ISO8601 (برای لاگ و MQTT)
    """
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def valid_qos(q: int) -> int:
    """
    مطمئن می‌شیم qos یکی از 0/1/2 هست
    """
    return q if q in (0, 1, 2) else 0


class PubBuffer:
    """
    بافر موقت تغییرات تا ما بتونیم هر چند میلی‌ثانیه
    یک پیام group روی PUBSUB_TOPIC بفرستیم
    """
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


# ==============================
#        MQTT
# ==============================

def mqtt_connect():
    """
    وصل می‌شیم به MQTT و loop_start می‌کنیم.
    روی تاپیک status هم online/offline می‌زنیم.
    """
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
        client.publish(
            f"{PUBSUB_TOPIC}/$status",
            "online",
            qos=qos,
            retain=True
        )

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
    """
    پابلیش JSON روی یه تاپیک خاص
    """
    if qos is None:
        qos = valid_qos(MQTT_QOS)

    try:
        data = json.dumps(payload, ensure_ascii=False)
    except Exception as e:
        # fallback درصورت مشکل سریال‌سازی
        log.warning("JSON encode failed (%s); forcing str(value)", e)
        payload = {**payload, "value": str(payload.get("value", ""))}
        data = json.dumps(payload, ensure_ascii=False)

    info = c.publish(topic, data, qos=qos, retain=retain)
    if info.rc != mqtt.MQTT_ERR_SUCCESS:
        log.warning("Publish rc=%s topic=%s", info.rc, topic)


# ==============================
#        OPC UA CLIENT + SECURITY
# ==============================

def _apply_security(client: Client):
    """
    اگر OPCUA_SECURITY=NONE یا خالی → بدون امنیت
    اگر نه → فرمت:
      Basic256Sha256,SignAndEncrypt,/certs/client_cert_opcua.pem,/certs/client_key.pem[,/certs/server.der]
    رو ست می‌کنیم روی client.
    """
    sec = OPCUA_SECURITY.strip()

    if sec == "" or sec.upper() == "NONE":
        log.warning("OPCUA_SECURITY set to NONE -> connecting WITHOUT security!")
        return

    parts = [p.strip() for p in sec.split(",")]
    if len(parts) < 4:
        raise RuntimeError(
            f"OPCUA_SECURITY malformed: '{sec}' "
            "(need: policy,mode,client_cert,client_key[,server_cert])"
        )

    policy = parts[0]
    mode = parts[1]
    client_cert_path = parts[2]
    client_key_path = parts[3]
    server_cert_path = parts[4] if len(parts) > 4 else None

    # چک فایل‌ها
    for fpath in [client_cert_path, client_key_path, server_cert_path]:
        if not fpath:
            continue
        if not os.path.exists(fpath):
            raise FileNotFoundError(f"Security file not found: {fpath}")

    log.info("OPC UA security policy=%s mode=%s", policy, mode)
    log.info(" client cert: %s", client_cert_path)
    log.info(" private key: %s", client_key_path)
    if server_cert_path:
        log.info(" server cert: %s", server_cert_path)

    # python-opcua انتظار همین فرمت رو دارد
    client.set_security_string(sec)


def build_opcua_client() -> Client:
    """
    ساخت کلاینت OPC UA با security و (در صورت وجود) user/pass
    """
    log.info("Connecting OPC UA secure to %s", OPCUA_ENDPOINT)

    client = Client(OPCUA_ENDPOINT)

    # برای لاگ‌ها داخل DeltaV
    client.application_uri = "urn:opcua2mqtt"
    client.application_name = "opcua2mqtt"

    _apply_security(client)

    if OPCUA_USER and OPCUA_PASS:
        client.set_user(OPCUA_USER)
        client.set_password(OPCUA_PASS)

    return client


# ==============================
#        BROWSE HELPERS
# ==============================

def browse_children(node, client: Client) -> List[Tuple[str, ua.NodeId, object]]:
    """
    همه‌ی بچه‌های node رو به صورت (DisplayName, NodeId, NodeObj) برمی‌گردونه.
    """
    out: List[Tuple[str, ua.NodeId, object]] = []

    refs = node.get_references(
        ua.ObjectIds.HierarchicalReferences,
        direction=ua.BrowseDirection.Forward
    )

    for ref in refs:
        try:
            child = client.get_node(ref.NodeId)
            dn = child.get_display_name().Text  # string انسانی
            out.append((dn, ref.NodeId, child))
        except Exception as e:
            log.warning("browse_children error: %s", e)

    return out


def find_child_by_name(parent_node, client: Client, wanted_name: str):
    """
    زیر یک نود خاص دنبال بچه‌ای با همون DisplayName (case-insensitive)
    """
    wanted_lower = wanted_name.lower()
    for (disp, _nid, obj) in browse_children(parent_node, client):
        if disp and disp.lower() == wanted_lower:
            return obj
    return None


def get_boiler_root(client: Client):
    """
    طبق ساختاری که از UAExpert دیدیم:
    Root → Objects → DA → MODULES → BOILER

    یعنی ما فقط BOILER رو مانیتور می‌کنیم (کل بویلر و زیرشاخه‌هاش).
    """

    # root معمولا i=84
    root = client.get_root_node()

    objects_node = find_child_by_name(root, client, "Objects")
    if objects_node is None:
        raise RuntimeError("Couldn't find 'Objects' under Root")

    da_node = find_child_by_name(objects_node, client, "DA")
    if da_node is None:
        raise RuntimeError("Couldn't find 'DA' under Objects")

    modules_node = find_child_by_name(da_node, client, "MODULES")
    if modules_node is None:
        raise RuntimeError("Couldn't find 'MODULES' under DA")

    boiler_node = find_child_by_name(modules_node, client, "BOILER")
    if boiler_node is None:
        raise RuntimeError("Couldn't find 'BOILER' under MODULES")

    log.info("BOILER root nodeid = %s", boiler_node.nodeid.to_string())
    return boiler_node


def crawl_for_cv_nodes(client: Client, start_node) -> Dict[str, Dict]:
    """
    از start_node (BOILER) شروع می‌کنه و کل زیرشاخه‌ها رو DFS می‌گرده.
    هر نود Variable که اسمش "CV" باشه یا آخرش ".CV" باشه رو جمع می‌کنه.

    خروجی:
    {
      "AIC427.BAD_MASK.CV": {
          "node": <opcua node>,
          "nodeid_str": "ns=2;s=0:AIC427/BAD_MASK.CV"
      },
      ...
    }

    tagname = مسیر پوشه‌ها با '.' بهم چسبیده.
    مثلا ["AIC427","BAD_MASK.CV"] → "AIC427.BAD_MASK.CV"
    """

    result: Dict[str, Dict] = {}
    stack: List[Tuple[List[str], object]] = [([], start_node)]

    while stack:
        path_names, node = stack.pop()

        kids = browse_children(node, client)

        for (dispname, _nodeid, childnode) in kids:
            if not dispname:
                continue

            new_path = path_names + [dispname]

            # نوع نود
            try:
                node_class = childnode.get_node_class()
            except Exception:
                node_class = None

            dn_lower = dispname.lower()

            # اگر Variable و اسمش CV یا ...CV
            if node_class == ua.NodeClass.Variable:
                if dn_lower == "cv" or dn_lower.endswith(".cv"):
                    tagname = ".".join(new_path)  # مثلا "AIC427.BAD_MASK.CV"
                    nodeid_str = childnode.nodeid.to_string()  # مثلا ns=2;s=0:...

                    result[tagname] = {
                        "node": childnode,
                        "nodeid_str": nodeid_str,
                    }

                    log.info("FOUND CV TAG: %s -> %s", tagname, nodeid_str)

            # ادامه‌ی DFS: برای Objectها و چیزایی که زیرشاخه دارن
            if node_class in (
                ua.NodeClass.Object,
                ua.NodeClass.Unspecified,
                ua.NodeClass.VariableType,
                None
            ):
                stack.append((new_path, childnode))

    return result


# ==============================
#        RUNTIME LOOP
# ==============================

def run_once():
    """
    1. وصل به OPC UA
    2. وصل به MQTT
    3. پیدا کردن BOILER
    4. کشف همه‌ی CV ها زیر BOILER
    5. خواندن اولیه و publish retained
    6. subscribe روی همه‌ی CV ها برای data change
    7. پابلیشر گروهی برای PUBSUB_TOPIC
    """

    global stop

    # --- OPC UA session ---
    client = build_opcua_client()
    client.connect()
    log.info("✅ OPC UA session active")

    # --- MQTT session ---
    mqttc = mqtt_connect()

    # --- برو به BOILER ---
    boiler_node = get_boiler_root(client)

    # --- همه‌ی CV ها را کشف کن ---
    cv_map = crawl_for_cv_nodes(client, boiler_node)
    if not cv_map:
        log.warning("No CV tags found under BOILER")

    # --- خواندن اولیه و publish retained روی تاپیک /init/... ---
    for tagname, info in cv_map.items():
        nodeobj = info["node"]
        nodeid_str = info["nodeid_str"]

        try:
            v = nodeobj.get_value()
        except Exception as e:
            log.error("Initial read failed for %s: %s", tagname, e)
            continue

        log.info("INIT READ %s = %s", tagname, v)

        snapshot_topic = f"{TOPIC_BASE}/init/{tagname}"
        snapshot_payload = {
            "tag": tagname,
            "nodeid": nodeid_str,
            "value": v,
            "ts": iso_now(),
        }
        mqtt_publish(mqttc, snapshot_topic, snapshot_payload, retain=True)

    # --- بافر تجمیع برای ارسال دوره‌ای ---
    agg = PubBuffer()

    # map برعکس: nodeid_str -> tag metadata
    node_to_meta: Dict[str, Dict[str, str]] = {}
    for tagname, info in cv_map.items():
        nodeid_str = info["nodeid_str"]
        node_to_meta[nodeid_str] = {
            "tagname": tagname,
            "nodeid": nodeid_str,
        }

    # --- handler برای subscribe ---
    class Handler:
        def datachange_notification(self_inner, node, val, data):
            # nodeid به استرینگ
            nodekey = node.nodeid.to_string()
            meta = node_to_meta.get(nodekey)

            if meta:
                tagname = meta["tagname"]
                nodeid_str = meta["nodeid"]
            else:
                # fallback عجیب (نباید زیاد رخ بده)
                tagname = nodekey
                nodeid_str = nodekey

            log.info("DATA CHANGE %s -> %s", tagname, val)

            # پابلیش per-tag
            per_tag_topic = f"{TOPIC_BASE}/{tagname}"
            per_tag_payload = {
                "tag": tagname,
                "nodeid": nodeid_str,
                "value": val,
                "ts": iso_now(),
            }
            mqtt_publish(mqttc, per_tag_topic, per_tag_payload, retain=False)

            # برای پیام تجمیعی
            agg.add(tagname, val)

        def event_notification(self_inner, event):
            # الان eventها رو لازم نداریم
            pass

    # --- subscribe روی همه‌ی CVها ---
    sub = client.create_subscription(1000, Handler())
    sub.subscribe_data_change([info["node"] for info in cv_map.values()])
    log.info("Subscribed to %d CV tags", len(cv_map))

    # --- پابلیشر گروهی در ترد جدا ---
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

    # --- لوپ اصلی برای زنده نگه داشتن ---
    try:
        while not stop:
            time.sleep(0.5)
    finally:
        # shutdown تمیز
        try:
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


# ==============================
#        MAIN
# ==============================

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
