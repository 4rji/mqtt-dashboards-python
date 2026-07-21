#!/usr/bin/env python3
"""
Single terminal dashboard split across THREE brokers, stacked top-to-bottom:

  TOP    ── HiveMQ Cloud (TLS 8883) ── Digi IX20 gateway + SmartSense sensor
  MIDDLE ── EMQX Cloud   (TLS 8883) ── Edge router + Wi-Fi AP + log feed
  BOTTOM ── Local Mosquitto (1883, no TLS) ── Power/PDU monitor + log feed

Each broker runs its own publisher + subscriber threads (6 MQTT clients total).
Every panel is fed by a real subscription, so it shows what actually
round-tripped through each broker. The EMQX and Local log panels subscribe to
wildcards, so they also capture the plaintext messages your `mqttclient` bash
scripts publish (prueba/router and Power/TEMP respectively).

Requirements:
    pip install paho-mqtt rich --break-system-packages
    # emqxsl-ca.crt next to this script (optional; falls back to system CA)

Usage:
    python3 tri_dashboard.py
    python3 tri_dashboard.py --interval 4 --emqx-ca ./emqxsl-ca.crt --local-host 10.10.65.67
"""

import argparse
import json
import os
import random
import ssl
import threading
import time
from collections import deque
from dataclasses import dataclass, asdict
from datetime import datetime

import paho.mqtt.client as mqtt
from rich.live import Live
from rich.table import Table
from rich.console import Console
from rich.panel import Panel
from rich.layout import Layout
from rich.text import Text
from rich.align import Align

# --------------------------------------------------------------------------- #
# Broker config
# --------------------------------------------------------------------------- #
HIVEMQ = dict(host="10.10.65.67",
              port=1883, user="digimqtt", password="digimqtt", tls=False, ca=None)
EMQX = dict(host="10.10.65.67",
            port=1883, user="digimqtt", password="digimqtt", tls=False, ca=None)
LOCAL = dict(host="10.10.65.67",
             port=1883, user="digimqtt", password="digimqtt", tls=False, ca=None)

GATEWAY_TOPIC = "digi/gateway/IX20-01/telemetry"
SENSOR_TOPIC = "digi/sensor/SS-TEMP-07/telemetry"
ROUTER_TOPIC = "prueba/router/telemetry"
AP_TOPIC = "prueba/ap/telemetry"
LOGS_TOPIC = "prueba/logs"
BASH_EMQX_TOPIC = "prueba/router"        # emqx bash script plaintext
POWER_TOPIC = "Power/TEMP/telemetry"     # local power monitor JSON
BASH_LOCAL_TOPIC = "Power/TEMP"          # local bash script plaintext

SPARK = "▁▂▃▄▅▆▇█"

lock = threading.Lock()
shared = {
    "hivemq": {"pub": False, "sub": False, "sent": 0, "recv": 0, "err": None,
               "devices": {GATEWAY_TOPIC: {"payload": None, "hist": deque(maxlen=30)},
                           SENSOR_TOPIC: {"payload": None, "hist": deque(maxlen=30)}}},
    "emqx": {"pub": False, "sub": False, "sent": 0, "recv": 0, "err": None,
             "devices": {ROUTER_TOPIC: {"payload": None, "hist": deque(maxlen=30)},
                         AP_TOPIC: {"payload": None, "hist": deque(maxlen=30)}},
             "logs": deque(maxlen=10)},
    "local": {"pub": False, "sub": False, "sent": 0, "recv": 0, "err": None,
              "devices": {POWER_TOPIC: {"payload": None, "hist": deque(maxlen=30)}},
              "logs": deque(maxlen=10)},
}


# --------------------------------------------------------------------------- #
# Device models
# --------------------------------------------------------------------------- #
def drift(prev, lo, hi, step, nd=1):
    if prev is None:
        return round(random.uniform(lo, hi), nd)
    return round(min(hi, max(lo, prev + random.uniform(-step, step))), nd)


@dataclass
class Gateway:
    device_id: str = "IX20-01"; imei: str = "352093089812345"; firmware: str = "23.9.74.0"
    network: str = "LTE"; operator: str = "Verizon"; band: str = "B13"
    rsrp_dbm: float = 0; rsrq_db: float = 0; sinr_db: float = 0
    cpu_temp_c: float = 0; cpu_util_pct: float = 0; mem_util_pct: float = 0
    wan_rx_mbps: float = 0; wan_tx_mbps: float = 0; uptime_s: int = 0; ts: str = ""

    @classmethod
    def step(cls, p=None):
        return cls(rsrp_dbm=drift(p.rsrp_dbm if p else None, -115, -75, 4),
                   rsrq_db=drift(p.rsrq_db if p else None, -16, -6, 1.5),
                   sinr_db=drift(p.sinr_db if p else None, -2, 25, 3),
                   cpu_temp_c=drift(p.cpu_temp_c if p else None, 42, 78, 2.5),
                   cpu_util_pct=drift(p.cpu_util_pct if p else None, 3, 95, 10),
                   mem_util_pct=drift(p.mem_util_pct if p else None, 30, 88, 4),
                   wan_rx_mbps=drift(p.wan_rx_mbps if p else None, 0, 150, 25),
                   wan_tx_mbps=drift(p.wan_tx_mbps if p else None, 0, 50, 10),
                   uptime_s=(p.uptime_s + 4) if p else random.randint(0, 900000),
                   ts=datetime.now().isoformat(timespec="seconds"))


@dataclass
class Sensor:
    sensor_id: str = "SS-TEMP-07"; probe: str = "cold-storage"; firmware: str = "1.8.2"
    temperature_c: float = 0; humidity_pct: float = 0; battery_pct: float = 0
    battery_v: float = 0; rssi_dbm: float = 0; door_open: bool = False; ts: str = ""

    @classmethod
    def step(cls, p=None):
        door = random.random() < (0.15 if p and p.door_open else 0.05)
        s = 4 if door else 1.2
        return cls(temperature_c=drift(p.temperature_c if p else None, -22, 6, s),
                   humidity_pct=drift(p.humidity_pct if p else None, 25, 80, 3),
                   battery_pct=round(p.battery_pct - 0.02, 2) if p else round(random.uniform(60, 100), 2),
                   battery_v=drift(p.battery_v if p else None, 2.9, 3.3, 0.02, 2),
                   rssi_dbm=drift(p.rssi_dbm if p else None, -95, -55, 4),
                   door_open=door, ts=datetime.now().isoformat(timespec="seconds"))


@dataclass
class Router:
    device_id: str = "EDGE-RTR-01"; model: str = "IX20"; firmware: str = "23.9.74.0"
    wan_rx_mbps: float = 0; wan_tx_mbps: float = 0; lan_clients: int = 0
    cpu_util_pct: float = 0; cpu_temp_c: float = 0; mem_util_pct: float = 0
    wan_latency_ms: float = 0; packet_loss_pct: float = 0; uptime_s: int = 0; ts: str = ""

    @classmethod
    def step(cls, p=None):
        return cls(wan_rx_mbps=drift(p.wan_rx_mbps if p else None, 0, 940, 90),
                   wan_tx_mbps=drift(p.wan_tx_mbps if p else None, 0, 300, 40),
                   lan_clients=int(drift(p.lan_clients if p else None, 4, 60, 3, 0)),
                   cpu_util_pct=drift(p.cpu_util_pct if p else None, 2, 90, 12),
                   cpu_temp_c=drift(p.cpu_temp_c if p else None, 40, 75, 2),
                   mem_util_pct=drift(p.mem_util_pct if p else None, 25, 85, 4),
                   wan_latency_ms=drift(p.wan_latency_ms if p else None, 8, 120, 15),
                   packet_loss_pct=drift(p.packet_loss_pct if p else None, 0, 4, 0.6),
                   uptime_s=(p.uptime_s + 4) if p else random.randint(0, 1200000),
                   ts=datetime.now().isoformat(timespec="seconds"))


@dataclass
class AccessPoint:
    ap_id: str = "WIFI-AP-03"; band: str = "5GHz"; channel: int = 44
    tx_power_dbm: float = 0; stations: int = 0; noise_floor_dbm: float = 0
    retries_pct: float = 0; throughput_mbps: float = 0; uptime_s: int = 0; ts: str = ""

    @classmethod
    def step(cls, p=None):
        return cls(channel=random.choice([36, 40, 44, 48, 149, 157]) if not p else p.channel,
                   tx_power_dbm=drift(p.tx_power_dbm if p else None, 12, 23, 1),
                   stations=int(drift(p.stations if p else None, 0, 40, 3, 0)),
                   noise_floor_dbm=drift(p.noise_floor_dbm if p else None, -98, -80, 2),
                   retries_pct=drift(p.retries_pct if p else None, 0, 18, 3),
                   throughput_mbps=drift(p.throughput_mbps if p else None, 0, 650, 70),
                   uptime_s=(p.uptime_s + 4) if p else random.randint(0, 800000),
                   ts=datetime.now().isoformat(timespec="seconds"))


@dataclass
class PowerMonitor:
    """Rack PDU / power+temperature monitor published to the local broker."""
    device_id: str = "PDU-RACK-A1"; firmware: str = "2.4.1"; phase: str = "L1"
    voltage_v: float = 0; current_a: float = 0; power_w: float = 0
    power_factor: float = 0; frequency_hz: float = 0
    energy_kwh: float = 0; temperature_c: float = 0; load_pct: float = 0; ts: str = ""

    @classmethod
    def step(cls, p=None):
        v = drift(p.voltage_v if p else None, 226, 234, 1)
        a = drift(p.current_a if p else None, 1.0, 14.0, 1.2)
        pf = drift(p.power_factor if p else None, 0.85, 0.99, 0.02, 2)
        w = round(v * a * pf, 1)
        energy = round((p.energy_kwh + w / 900000.0), 4) if p else round(random.uniform(50, 500), 4)
        return cls(voltage_v=v, current_a=a, power_factor=pf, power_w=w,
                   frequency_hz=drift(p.frequency_hz if p else None, 49.9, 50.1, 0.03, 2),
                   energy_kwh=energy,
                   temperature_c=drift(p.temperature_c if p else None, 28, 55, 1.5),
                   load_pct=round(min(a / 16.0 * 100, 100), 1),
                   ts=datetime.now().isoformat(timespec="seconds"))


LOG_TEMPLATES = [
    ("INFO", "dhcpd: DHCPACK 192.168.1.{} lease 12h"),
    ("INFO", "wan: link up 1000Mbps full-duplex"),
    ("INFO", "vpn: IPsec tunnel established peer 10.8.0.{}"),
    ("WARN", "firewall: DROP in TCP 22 from 45.83.{}.{}"),
    ("WARN", "wifi: high retry rate on ch44 ({}%)"),
    ("ERR", "wan: latency spike {}ms, failover armed"),
    ("INFO", "ntp: clock synced, offset {}ms"),
]
POWER_LOG_TEMPLATES = [
    ("INFO", "pdu: outlet {} on, {}W"),
    ("WARN", "pdu: phase load {}% near threshold"),
    ("ERR", "pdu: inlet temp {}C over limit"),
    ("INFO", "meter: energy checkpoint {}kWh"),
]


def make_log_line(templates):
    sev, tmpl = random.choice(templates)
    args = [random.randint(1, 254) for _ in range(tmpl.count("{}"))]
    return {"sev": sev, "msg": tmpl.format(*args), "ts": datetime.now().strftime("%H:%M:%S")}


# --------------------------------------------------------------------------- #
# MQTT
# --------------------------------------------------------------------------- #
def make_client(cid, cfg):
    c = mqtt.Client(client_id=cid, callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    c.username_pw_set(cfg["user"], cfg["password"])
    if cfg["tls"]:
        if cfg["ca"] and os.path.exists(cfg["ca"]):
            c.tls_set(ca_certs=cfg["ca"], tls_version=ssl.PROTOCOL_TLS_CLIENT)
        else:
            c.tls_set(tls_version=ssl.PROTOCOL_TLS_CLIENT)   # system CA bundle
    return c   # no tls_set at all -> plaintext (local 1883)


def _set(b, k, v):
    with lock: shared[b][k] = v

def _err(b, m):
    with lock: shared[b]["err"] = m


def hivemq_pub(cfg, interval, stop):
    c = make_client(f"hm-pub-{random.randint(1000,9999)}", cfg)
    c.on_connect = lambda *a: _set("hivemq", "pub", str(a[3]) == "Success")
    c.on_disconnect = lambda *a: _set("hivemq", "pub", False)
    try: c.connect(cfg["host"], cfg["port"], 30)
    except Exception as e: _err("hivemq", f"pub: {e}"); return
    c.loop_start(); gw = sn = None
    try:
        while not stop.is_set():
            gw, sn = Gateway.step(gw), Sensor.step(sn)
            c.publish(GATEWAY_TOPIC, json.dumps(asdict(gw)), qos=1)
            c.publish(SENSOR_TOPIC, json.dumps(asdict(sn)), qos=1)
            with lock: shared["hivemq"]["sent"] += 2
            stop.wait(interval)
    finally: c.loop_stop(); c.disconnect()


def emqx_pub(cfg, interval, stop):
    c = make_client(f"eq-pub-{random.randint(1000,9999)}", cfg)
    c.on_connect = lambda *a: _set("emqx", "pub", str(a[3]) == "Success")
    c.on_disconnect = lambda *a: _set("emqx", "pub", False)
    try: c.connect(cfg["host"], cfg["port"], 30)
    except Exception as e: _err("emqx", f"pub: {e}"); return
    c.loop_start(); rt = ap = None
    try:
        while not stop.is_set():
            rt, ap = Router.step(rt), AccessPoint.step(ap)
            c.publish(ROUTER_TOPIC, json.dumps(asdict(rt)), qos=1)
            c.publish(AP_TOPIC, json.dumps(asdict(ap)), qos=1)
            c.publish(LOGS_TOPIC, json.dumps(make_log_line(LOG_TEMPLATES)), qos=0)
            with lock: shared["emqx"]["sent"] += 3
            stop.wait(interval)
    finally: c.loop_stop(); c.disconnect()


def local_pub(cfg, interval, stop):
    c = make_client(f"lo-pub-{random.randint(1000,9999)}", cfg)
    c.on_connect = lambda *a: _set("local", "pub", str(a[3]) == "Success")
    c.on_disconnect = lambda *a: _set("local", "pub", False)
    try: c.connect(cfg["host"], cfg["port"], 30)
    except Exception as e: _err("local", f"pub: {e}"); return
    c.loop_start(); pm = None
    try:
        while not stop.is_set():
            pm = PowerMonitor.step(pm)
            c.publish(POWER_TOPIC, json.dumps(asdict(pm)), qos=1)
            if random.random() < 0.6:
                c.publish(f"{BASH_LOCAL_TOPIC}/logs", json.dumps(make_log_line(POWER_LOG_TEMPLATES)), qos=0)
            with lock: shared["local"]["sent"] += 1
            stop.wait(interval)
    finally: c.loop_stop(); c.disconnect()


def hivemq_sub(cfg, stop):
    c = make_client(f"hm-sub-{random.randint(1000,9999)}", cfg)
    def on_connect(cl, u, f, rc, p=None):
        _set("hivemq", "sub", str(rc) == "Success")
        cl.subscribe([(GATEWAY_TOPIC, 1), (SENSOR_TOPIC, 1)])
    def on_msg(cl, u, m):
        try: data = json.loads(m.payload.decode())
        except Exception: return
        with lock:
            dev = shared["hivemq"]["devices"].get(m.topic)
            if not dev: return
            dev["payload"] = data
            t = data.get("cpu_temp_c", data.get("temperature_c"))
            if t is not None: dev["hist"].append(t)
            shared["hivemq"]["recv"] += 1
    c.on_connect = on_connect
    c.on_disconnect = lambda *a: _set("hivemq", "sub", False)
    c.on_message = on_msg
    try: c.connect(cfg["host"], cfg["port"], 30)
    except Exception as e: _err("hivemq", f"sub: {e}"); return
    c.loop_start(); stop.wait(); c.loop_stop(); c.disconnect()


def emqx_sub(cfg, stop):
    c = make_client(f"eq-sub-{random.randint(1000,9999)}", cfg)
    def on_connect(cl, u, f, rc, p=None):
        _set("emqx", "sub", str(rc) == "Success")
        cl.subscribe("prueba/#", 1)
    def on_msg(cl, u, m):
        raw = m.payload.decode(errors="replace")
        with lock:
            if m.topic in (ROUTER_TOPIC, AP_TOPIC):
                try: shared["emqx"]["devices"][m.topic]["payload"] = json.loads(raw)
                except Exception: return
                if m.topic == ROUTER_TOPIC:
                    try: shared["emqx"]["devices"][m.topic]["hist"].append(json.loads(raw)["cpu_temp_c"])
                    except Exception: pass
            elif m.topic == LOGS_TOPIC:
                try: shared["emqx"]["logs"].append(json.loads(raw))
                except Exception: shared["emqx"]["logs"].append({"sev": "INFO", "msg": raw, "ts": datetime.now().strftime("%H:%M:%S")})
            elif m.topic == BASH_EMQX_TOPIC:
                shared["emqx"]["logs"].append({"sev": "EXT", "msg": raw, "ts": datetime.now().strftime("%H:%M:%S")})
            else: return
            shared["emqx"]["recv"] += 1
    c.on_connect = on_connect
    c.on_disconnect = lambda *a: _set("emqx", "sub", False)
    c.on_message = on_msg
    try: c.connect(cfg["host"], cfg["port"], 30)
    except Exception as e: _err("emqx", f"sub: {e}"); return
    c.loop_start(); stop.wait(); c.loop_stop(); c.disconnect()


def local_sub(cfg, stop):
    c = make_client(f"lo-sub-{random.randint(1000,9999)}", cfg)
    def on_connect(cl, u, f, rc, p=None):
        _set("local", "sub", str(rc) == "Success")
        cl.subscribe("Power/#", 1)   # telemetry, logs, and the bash script plaintext
    def on_msg(cl, u, m):
        raw = m.payload.decode(errors="replace")
        with lock:
            if m.topic == POWER_TOPIC:
                try:
                    d = json.loads(raw)
                    shared["local"]["devices"][POWER_TOPIC]["payload"] = d
                    shared["local"]["devices"][POWER_TOPIC]["hist"].append(d["temperature_c"])
                except Exception: return
            elif m.topic == f"{BASH_LOCAL_TOPIC}/logs":
                try: shared["local"]["logs"].append(json.loads(raw))
                except Exception: shared["local"]["logs"].append({"sev": "INFO", "msg": raw, "ts": datetime.now().strftime("%H:%M:%S")})
            elif m.topic == BASH_LOCAL_TOPIC:
                shared["local"]["logs"].append({"sev": "EXT", "msg": raw, "ts": datetime.now().strftime("%H:%M:%S")})
            else: return
            shared["local"]["recv"] += 1
    c.on_connect = on_connect
    c.on_disconnect = lambda *a: _set("local", "sub", False)
    c.on_message = on_msg
    try: c.connect(cfg["host"], cfg["port"], 30)
    except Exception as e: _err("local", f"sub: {e}"); return
    c.loop_start(); stop.wait(); c.loop_stop(); c.disconnect()


# --------------------------------------------------------------------------- #
# Rendering helpers
# --------------------------------------------------------------------------- #
def gauge(pct, width=16, invert=False):
    pct = min(max(pct, 0), 100); filled = int(width * pct / 100)
    good = pct > 60 if invert else pct < 60
    warn = pct > 40 if invert else pct < 85
    color = "green" if good else ("yellow" if warn else "red")
    return f"[{color}]{'█'*filled}{'░'*(width-filled)}[/{color}] {pct:5.1f}%"

def spark(vals):
    if not vals: return "[dim]—[/dim]"
    lo, hi = min(vals), max(vals)
    if hi-lo < 1e-9: return SPARK[0]*len(vals)
    return "".join(SPARK[min(int((v-lo)/(hi-lo)*(len(SPARK)-1)), len(SPARK)-1)] for v in vals)

def sig_bars(rsrp, lo=-115, hi=-75):
    bars = int(min(max((rsrp-lo)/(hi-lo), 0), 1)*5)
    color = "red" if bars <= 1 else ("yellow" if bars <= 3 else "green")
    return f"[{color}]{'▮'*bars}{'▯'*(5-bars)}[/{color}] {rsrp:.0f} dBm"

def cpu_c(t): c = "green" if t < 60 else ("yellow" if t < 72 else "red"); return f"[{c}]{t:5.1f} °C[/{c}]"
def cold_c(t): c = "green" if t <= 2 else ("yellow" if t <= 5 else "red"); return f"[{c}]{t:6.1f} °C[/{c}]"
def pdu_c(t): c = "green" if t < 40 else ("yellow" if t < 50 else "red"); return f"[{c}]{t:5.1f} °C[/{c}]"

def grid(style): 
    t = Table.grid(padding=(0, 1)); t.add_column(justify="right", style=style); t.add_column(); return t

def wait_panel(title, style):
    return Panel(Align.center("[dim]waiting for broker...[/dim]", vertical="middle"), title=title, border_style=style)


def gateway_panel(dev):
    d = dev["payload"]
    if not d: return wait_panel("Digi IX20 Gateway", "cyan")
    t = grid("bold cyan")
    t.add_row("Device", f"{d['device_id']} [dim]{d['network']}·{d['operator']}·{d['band']}[/dim]")
    t.add_row("Signal", sig_bars(d["rsrp_dbm"]))
    t.add_row("CPU Temp", cpu_c(d["cpu_temp_c"]))
    t.add_row("CPU/Mem", gauge(d["cpu_util_pct"]))
    t.add_row("", gauge(d["mem_util_pct"]))
    t.add_row("WAN", f"[green]▼{d['wan_rx_mbps']:.0f}[/green] [blue]▲{d['wan_tx_mbps']:.0f}[/blue] Mbps")
    t.add_row("CPU trend", f"[cyan]{spark(list(dev['hist']))}[/cyan]")
    return Panel(t, title="[bold cyan]Digi IX20 Gateway[/bold cyan]", border_style="cyan")

def sensor_panel(dev):
    d = dev["payload"]
    if not d: return wait_panel("SmartSense Sensor", "magenta")
    t = grid("bold magenta")
    door = "[bold red]OPEN[/bold red]" if d["door_open"] else "[green]closed[/green]"
    bc = "green" if d["battery_pct"] > 40 else ("yellow" if d["battery_pct"] > 15 else "red")
    t.add_row("Sensor", f"{d['sensor_id']} [dim]{d['probe']}[/dim]")
    t.add_row("Temp", cold_c(d["temperature_c"]))
    t.add_row("Humidity", gauge(d["humidity_pct"]))
    t.add_row("Door", door)
    t.add_row("Battery", f"[{bc}]{d['battery_pct']:.1f}%[/{bc}] [dim]{d['battery_v']:.2f}V[/dim]")
    t.add_row("Temp trend", f"[magenta]{spark(list(dev['hist']))}[/magenta]")
    return Panel(t, title="[bold magenta]SmartSense Sensor[/bold magenta]", border_style="magenta")

def router_panel(dev):
    d = dev["payload"]
    if not d: return wait_panel("Edge Router", "green")
    t = grid("bold green")
    lc = "green" if d["packet_loss_pct"] < 1 else ("yellow" if d["packet_loss_pct"] < 3 else "red")
    t.add_row("Router", f"{d['device_id']} [dim]{d['model']}[/dim]")
    t.add_row("WAN", f"[green]▼{d['wan_rx_mbps']:.0f}[/green] [blue]▲{d['wan_tx_mbps']:.0f}[/blue]")
    t.add_row("Clients", str(d["lan_clients"]))
    t.add_row("Latency", f"{d['wan_latency_ms']:.0f}ms [{lc}]{d['packet_loss_pct']:.1f}%[/{lc}]")
    t.add_row("CPU Temp", cpu_c(d["cpu_temp_c"]))
    t.add_row("CPU", gauge(d["cpu_util_pct"]))
    t.add_row("trend", f"[green]{spark(list(dev['hist']))}[/green]")
    return Panel(t, title="[bold green]Edge Router[/bold green]", border_style="green")

def ap_panel(dev):
    d = dev["payload"]
    if not d: return wait_panel("Wi-Fi AP", "yellow")
    t = grid("bold yellow")
    rc = "green" if d["retries_pct"] < 6 else ("yellow" if d["retries_pct"] < 12 else "red")
    t.add_row("AP", f"{d['ap_id']} [dim]{d['band']} ch{d['channel']}[/dim]")
    t.add_row("Stations", str(d["stations"]))
    t.add_row("TX power", f"{d['tx_power_dbm']:.0f} dBm")
    t.add_row("Noise", f"{d['noise_floor_dbm']:.0f} dBm")
    t.add_row("Retries", f"[{rc}]{d['retries_pct']:.1f}%[/{rc}]")
    t.add_row("Thrpt", f"{d['throughput_mbps']:.0f} Mbps")
    return Panel(t, title="[bold yellow]Wi-Fi AP[/bold yellow]", border_style="yellow")

def power_panel(dev):
    d = dev["payload"]
    if not d: return wait_panel("Power / PDU Monitor", "red")
    t = grid("bold red")
    t.add_row("PDU", f"{d['device_id']} [dim]{d['phase']}[/dim]")
    t.add_row("Voltage", f"{d['voltage_v']:.1f} V   [dim]{d['frequency_hz']:.2f} Hz[/dim]")
    t.add_row("Current", f"{d['current_a']:.1f} A")
    t.add_row("Power", f"[bold]{d['power_w']:.0f} W[/bold]   [dim]pf {d['power_factor']:.2f}[/dim]")
    t.add_row("Load", gauge(d["load_pct"]))
    t.add_row("Inlet Temp", pdu_c(d["temperature_c"]))
    t.add_row("Energy", f"{d['energy_kwh']:.3f} kWh")
    t.add_row("Temp trend", f"[red]{spark(list(dev['hist']))}[/red]")
    return Panel(t, title="[bold red]Power / PDU Monitor[/bold red]", border_style="red")

def logs_panel(logs, title, n=8):
    sev_color = {"INFO": "green", "WARN": "yellow", "ERR": "red", "EXT": "bold cyan"}
    lines = [f"[dim]{e['ts']}[/dim] [{sev_color.get(e['sev'],'white')}]{e['sev']:4}[/] {e['msg']}"
             for e in list(logs)[-n:]]
    body = "\n".join(lines) if lines else "[dim]waiting for stream + bash script...[/dim]"
    return Panel(Text.from_markup(body), title=title, border_style="white")


def status(b):
    s = shared[b]
    dot = lambda v: "[green]OK[/green]" if v else "[red]DOWN[/red]"
    out = f"pub {dot(s['pub'])} sub {dot(s['sub'])}  tx {s['sent']} rx {s['recv']}"
    if s["err"]: out += f"  [red]{s['err']}[/red]"
    return out


def build(args):
    with lock:
        hm = Text.from_markup(f"[bold]HiveMQ[/bold] [dim]{HIVEMQ['host']}:8883 TLS[/dim]  {status('hivemq')}")
        eq = Text.from_markup(f"[bold]EMQX[/bold] [dim]{EMQX['host']}:8883 TLS[/dim]  {status('emqx')}")
        lo = Text.from_markup(f"[bold]Local[/bold] [dim]{LOCAL['host']}:1883 plain[/dim]  {status('local')}")
        gw = shared["hivemq"]["devices"][GATEWAY_TOPIC]; sn = shared["hivemq"]["devices"][SENSOR_TOPIC]
        rt = shared["emqx"]["devices"][ROUTER_TOPIC]; ap = shared["emqx"]["devices"][AP_TOPIC]
        eq_logs = shared["emqx"]["logs"]
        pw = shared["local"]["devices"][POWER_TOPIC]; lo_logs = shared["local"]["logs"]

    root = Layout()
    root.split_column(Layout(name="r1", ratio=1), Layout(name="r2", ratio=1), Layout(name="r3", ratio=1))

    root["r1"].split_column(Layout(Panel(Align.center(hm), border_style="cyan"), size=3),
                            Layout(name="b1"))
    root["r1"]["b1"].split_row(Layout(gateway_panel(gw)), Layout(sensor_panel(sn)))

    root["r2"].split_column(Layout(Panel(Align.center(eq), border_style="green"), size=3),
                            Layout(name="b2"))
    root["r2"]["b2"].split_row(Layout(router_panel(rt), ratio=2), Layout(ap_panel(ap), ratio=2),
                               Layout(logs_panel(eq_logs, "[bold white]EMQX Log Feed[/bold white]"), ratio=3))

    root["r3"].split_column(Layout(Panel(Align.center(lo), border_style="red"), size=3),
                            Layout(name="b3"))
    root["r3"]["b3"].split_row(Layout(power_panel(pw), ratio=2),
                               Layout(logs_panel(lo_logs, "[bold white]Local Power Log Feed[/bold white]"), ratio=3))
    return root


def main():
    p = argparse.ArgumentParser(description="Triple-broker split dashboard (HiveMQ / EMQX / Local)")
    p.add_argument("--interval", type=float, default=4.0)
    p.add_argument("--emqx-ca", default="emqxsl-ca.crt")
    p.add_argument("--local-host", default=LOCAL["host"])
    args = p.parse_args()

    EMQX["ca"] = args.emqx_ca
    HIVEMQ["host"] = args.local_host
    EMQX["host"] = args.local_host
    LOCAL["host"] = args.local_host

    stop = threading.Event()
    for target, cargs in [
        (hivemq_pub, (HIVEMQ, args.interval, stop)), (hivemq_sub, (HIVEMQ, stop)),
        (emqx_pub, (EMQX, args.interval, stop)),     (emqx_sub, (EMQX, stop)),
        (local_pub, (LOCAL, args.interval, stop)),   (local_sub, (LOCAL, stop)),
    ]:
        threading.Thread(target=target, args=cargs, daemon=True).start()

    console = Console()
    try:
        with Live(build(args), console=console, refresh_per_second=4, screen=True) as live:
            while True:
                live.update(build(args)); time.sleep(0.25)
    except KeyboardInterrupt:
        pass
    finally:
        stop.set(); time.sleep(0.3)


if __name__ == "__main__":
    main()
