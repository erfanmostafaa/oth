import os
import json
import time
import signal
import logging
import threading
import itertools
import re
from datetime import datetime, timezone, date
from typing import Dict, Tuple, List, Any

from opcua import Client, ua
import paho.mqtt.client as mqtt

# =======================
#   ENV CONFIG
# =======================

OPCUA_ENDPOINT = os.getenv("OPCUA_ENDPOINT", "opc.tcp://127.0.0.1:4840")

# مثال: NONE یا Basic256,Sign,/app/certs/client-cert.pem,/app/certs/client-key.pem
OPCUA_SECURITY = os.getenv("OPCUA_SECURITY", "NONE").strip()
OPCUA_USER = os.getenv("OPCUA_USER", "").strip()
OPCUA_PASS = os.getenv("OPCUA_PASS", "").strip()

SITE = os.getenv("SITE", "SITE1")
UNIT = os.getenv("UNIT", "UNIT1")
PLC = os.getenv("PLC", "PLC1")

MQTT_HOST = os.getenv("MQTT_HOST", "emqx")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_QOS = int(os.getenv("MQTT_QOS", "0"))

TOPIC_BASE = os.getenv("TOPIC_BASE", f"plant/{SITE}/{UNIT}/{PLC}/cv")
PUBSUB_TOPIC = os.getenv("PUBSUB_TOPIC", f"opcua/json/{SITE}-{UNIT}-{PLC}")

# فاصله ارسال بسته PubSub (بر اساس تغییرات جمع‌شده)
PUBLISH_INTERVAL_MS = int(os.getenv("PUBLISH_INTERVAL_MS", "1000"))

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# عمق گشتن درخت فقط در شروع
MAX_BROWSE_DEPTH = int(os.getenv("MAX_BROWSE_DEPTH", "20"))
BROWSE_DELAY_MS = int(os.getenv("BROWSE_DELAY_MS", "50"))

# مسیر شروع زیر Objects
OPCUA_START_PATH = os.getenv("OPCUA_START_PATH", "DA.MODULES.BOILER").strip()

# رجکس برای انتخاب تگ‌ها (مثلاً (^|[./])CV$)
TAG_MATCH_REGEX = os.getenv("TAG_MATCH_REGEX", r"(^|[./])CV$")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("opcua2mqtt")

try:
    TAG_RE = re.compile(TAG_MATCH_REGEX, re.IGNORECASE)
except re.error as e:
    log.warning(
        "Invalid TAG_MATCH_REGEX=%r (%s) – falling back to simple .CV matching",
        TAG_MATCH_REGEX,
        e,
    )
    TAG_RE = None

stop = False


def iso_now() -> str:
    """زمان فعلی به صورت ISO8601 در UTC بدون microsecond."""
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def valid_qos(q: int) -> int:
    return q if q in (0, 1, 2) else 0


# =======================
#   JSON SAFE CONVERSION
# =======================

def to_jsonable(obj: Any) -> Any:
    """هر نوع OPC UA / Python را به نوع قابل JSON تبدیل می‌کند."""
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj

    if isinstance(obj, (datetime, date)):
        if obj.tzinfo is None:
            obj = obj.replace(tzinfo=timezone.utc)
        return obj.replace(microsecond=0).isoformat().replace("+00:00", "Z")

    if isinstance(obj, (bytes, bytearray)):
        try:
            return obj.decode("utf-8")
        except Exception:
            return obj.hex()

    if isinstance(obj, (list, tuple)):
        return [to_jsonable(x) for x in obj]

    if isinstance(obj, dict):
        return {str(k): to_jsonable(v) for k, v in obj.items()}

    try:
        if isinstance(obj, ua.Variant):
            return to_jsonable(obj.Value)
        if isinstance(obj, ua.NodeId):
            return obj.to_string()
        if isinstance(obj, ua.StatusCode):
            return int(obj)
    except Exception:
        pass

    return str(obj)


class PubBuffer:
    """بافر ساده برای تجمیع تغییرات قبل از ارسال پیام PubSub روی MQTT."""

    def __init__(self):
        self._lock = threading.Lock()
        self._seq = itertools.count(1)
        self._pending: Dict[str, Any] = {}

    def add(self, tagname: str, value: Any):
        with self._lock:
            self._pending[tagname] = value

    def drain(self) -> Dict[str, Any]:
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
    qos = valid_qos(MQTT_QOS)
    client_id = f"opcua2mqtt-{int(time.time())}"

    c = mqtt.Client(
        client_id=client_id,
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        protocol=mqtt.MQTTv5,
    )

    # Last Will
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
    try:
        safe_payload = to_jsonable(payload)
        data = json.dumps(safe_payload, ensure_ascii=False)
    except Exception as e:
        log.error("JSON encode failed for topic=%s payload=%r error=%s", topic, payload, e)
        return

    try:
        info = c.publish(topic, data, qos=qos, retain=retain)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            log.warning("Publish FAILED rc=%s topic=%s", info.rc, topic)
        else:
            log.debug("Publish OK topic=%s retain=%s", topic, retain)
    except Exception as e:
        log.error("MQTT publish exception topic=%s error=%s", topic, e)


# =======================
#   OPC UA
# =======================

def _apply_security(client: Client):
    sec = OPCUA_SECURITY.upper()
    if not sec or sec == "NONE":
        log.warning("OPCUA_SECURITY=NONE -> no security")
        return
    log.info("Using OPC UA security string: %s", OPCUA_SECURITY)
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
    """یک بار children را می‌خواند؛ delay برای فشار نیاوردن به DeltaV."""
    out = []
    try:
        refs = node.get_references(
            ua.ObjectIds.HierarchicalReferences,
            direction=ua.BrowseDirection.Forward,
        )
    except Exception as e:
        log.warning("browse get_references err: %s", e)
        return out

    for ref in refs:
        try:
            child = client.get_node(ref.NodeId)
            dn = child.get_display_name().Text
            out.append((dn, child))
        except Exception as e:
            log.warning("browse child err: %s", e)
        if BROWSE_DELAY_MS > 0:
            time.sleep(BROWSE_DELAY_MS / 1000.0)
    return out


def resolve_path_from_objects(client: Client, path_str: str):
    """مثلاً DA.MODULES.BOILER را از زیر Objects پیدا می‌کند."""
    parts = [p.strip() for p in path_str.split(".") if p.strip()]
    node = client.get_objects_node()
    current_path = ["Objects"]

    for part in parts:
        children = browse_children(node, client)
        found = None
        for (name, child) in children:
            if name == part:
                found = child
                break
        if not found:
            log.error(
                "Path component '%s' not found under %s",
                part,
                ".".join(current_path),
            )
            return None
        node = found
        current_path.append(part)

    log.info(
        "Resolved OPCUA_START_PATH=%s to node %s",
        path_str,
        ".".join(current_path),
    )
    return node, current_path


def _is_cv_tag(path: List[str], name: str) -> bool:
    """بررسی می‌کند این نود یک CV است یا نه."""
    logical_path = path[1:] if path and path[0] == "Objects" else path
    full = "/".join(logical_path + [name])
    if TAG_RE is not None:
        return TAG_RE.search(full) is not None
    return name.lower().endswith(".cv")


def _crawl_from_node(client: Client, root_node, root_path: List[str]) -> Dict[str, Dict]:
    """
    فقط یک بار در startup اجرا می‌شود.
    هیچ Loop دوره‌ای برای browse بعد از این وجود ندارد.
    """
    result: Dict[str, Dict] = {}
    stack: List[Tuple[List[str], Any, int]] = [(root_path, root_node, 0)]

    while stack:
        path, node, depth = stack.pop()
        if depth > MAX_BROWSE_DEPTH:
            continue

        for (name, child) in browse_children(node, client):
            if not name:
                continue

            new_path = path + [name]

            try:
                node_class = child.get_node_class()
            except Exception:
                node_class = None

            if node_class == ua.NodeClass.Variable and _is_cv_tag(path, name):
                tagname = ".".join(new_path)
                nodeid_str = child.nodeid.to_string()
                result[tagname] = {
                    "node": child,
                    "nodeid_str": nodeid_str,
                }
                log.info("FOUND CV TAG: %s -> %s", tagname, nodeid_str)

            # ادامه گشتن فقط در startup
            if node_class in (
                ua.NodeClass.Object,
                ua.NodeClass.Unspecified,
                ua.NodeClass.VariableType,
                None,
            ):
                stack.append((new_path, child, depth + 1))

    return result


def crawl_all_for_cv(client: Client) -> Dict[str, Dict]:
    """فقط یک بار در شروع اجرا می‌شود؛ هیچ تکرار دوره‌ای ندارد."""
    if not OPCUA_START_PATH:
        log.error("OPCUA_START_PATH is empty – nothing to browse.")
        return {}

    resolved = resolve_path_from_objects(client, OPCUA_START_PATH)
    if not resolved:
        log.error("Could not resolve OPCUA_START_PATH '%s'", OPCUA_START_PATH)
        return {}

    root_node, root_path = resolved
    log.info(
        "Browsing CVs under: %s (max depth %d)",
        ".".join(root_path),
        MAX_BROWSE_DEPTH,
    )
    cv_map = _crawl_from_node(client, root_node, root_path)
    log.info("Finished browse. CV tags found: %d", len(cv_map))
    return cv_map


# =======================
#   MAIN RUN
# =======================

def run_once():
    global stop

    # 1) MQTT
    mqttc = mqtt_connect()

    # پیام تست
    test_topic = "test/opcua2mqtt/hello"
    test_payload = {"msg": "hello from opcua2mqtt", "ts": iso_now()}
    mqtt_publish(mqttc, test_topic, test_payload, retain=False)
    log.info("TEST MQTT MESSAGE sent on topic %s", test_topic)

    # 2) OPC UA
    client = build_opcua_client()
    client.connect()
    log.info("✅ OPC UA session active")

    # 3) فقط یک بار Browse برای پیدا کردن CVها
    cv_map = crawl_all_for_cv(client)
    total = len(cv_map)
    log.info("Total CV tags found under %s: %d", OPCUA_START_PATH, total)
    if total == 0:
        log.warning("No CV tags found under configured path")

    # 4) Initial snapshot (retained) – یک بار
    log.info("Publishing %d retained init snapshots to MQTT...", total)
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
        topic = f"{TOPIC_BASE}/init/{tagname}"
        mqtt_publish(mqttc, topic, payload, retain=True)
    log.info("Initial publish done.")

    # map برای datachange
    node_to_meta: Dict[str, Dict[str, str]] = {}
    for tagname, info in cv_map.items():
        nodeid_str = info["nodeid_str"]
        node_to_meta[nodeid_str] = {"tagname": tagname, "nodeid": nodeid_str}

    agg = PubBuffer()

    class Handler:
        def datachange_notification(self_inner, node, val, data):
            nodekey = node.nodeid.to_string()
            meta = node_to_meta.get(
                nodekey, {"tagname": nodekey, "nodeid": nodekey}
            )
            tagname = meta["tagname"]
            nodeid_str = meta["nodeid"]
            log.info("DATA CHANGE %s -> %s", tagname, val)
            payload = {
                "tag": tagname,
                "nodeid": nodeid_str,
                "value": val,
                "ts": iso_now(),
            }
            topic = f"{TOPIC_BASE}/{tagname}"
            mqtt_publish(mqttc, topic, payload, retain=False)
            agg.add(tagname, val)

    # 5) Subscription فقط روی CVها – بدون هیچ browse/read loop بعدی
    if cv_map:
        sub = client.create_subscription(1000, Handler())
        sub.subscribe_data_change([info["node"] for info in cv_map.values()])
        log.info("Subscribed to %d CV tags", len(cv_map))
    else:
        sub = None

    # 6) حلقه‌ی PubSub aggregate روی تاپیک OPC UA PubSub
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
                        "Payload": to_jsonable(payloads),
                    }
                ],
            }
            mqtt_publish(mqttc, PUBSUB_TOPIC, msg, retain=False)

    t = threading.Thread(target=pub_loop, daemon=True)
    t.start()

    try:
        # دیگر هیچ browse/read دوره‌ای نداریم؛ فقط منتظر eventها هستیم
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
