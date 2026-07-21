#!/usr/bin/env python3
"""
Publica métricas falsas de "servidor" a HiveMQ Cloud cada N segundos (thread
en background) y muestra un dashboard live en terminal alimentado por una
suscripción real al mismo topic (thread separado), o sea el dashboard pinta
lo que realmente llegó por el broker, no lo que se generó en memoria.

Requisitos:
    pip install paho-mqtt rich --break-system-packages

Uso:
    python3 mqtt_dashboard.py
    python3 mqtt_dashboard.py --host OTRO_HOST --user U --password P --interval 4
"""

import argparse
import json
import random
import ssl
import threading
import time
from dataclasses import dataclass, asdict
from datetime import datetime

import paho.mqtt.client as mqtt
from rich.live import Live
from rich.table import Table
from rich.console import Console
from rich.panel import Panel

DEFAULT_HOST = "3cbcfef821e544faa64258788d78c2f5.s1.eu.hivemq.cloud"
DEFAULT_PORT = 8883
DEFAULT_USER = "digimqtt"
DEFAULT_PASS = "digimqtt"
DEFAULT_TOPIC = "servidor/metrics"

state_lock = threading.Lock()
shared_state = {
    "last_payload": None,
    "last_receive": None,
    "connected_pub": False,
    "connected_sub": False,
    "msgs_sent": 0,
    "msgs_recv": 0,
    "last_error": None,
}


@dataclass
class ServerMetrics:
    cpu_temp_c: float
    cpu_usage_pct: float
    mem_usage_pct: float
    disk_usage_pct: float
    net_throughput_mbps: float
    uptime_s: int

    @classmethod
    def random_walk(cls, prev=None):
        def drift(base, lo, hi, step):
            if prev is None:
                return round(random.uniform(lo, hi), 1)
            val = base + random.uniform(-step, step)
            return round(min(hi, max(lo, val)), 1)

        return cls(
            cpu_temp_c=drift(prev.cpu_temp_c if prev else 55, 35, 85, 3),
            cpu_usage_pct=drift(prev.cpu_usage_pct if prev else 30, 1, 100, 8),
            mem_usage_pct=drift(prev.mem_usage_pct if prev else 40, 5, 95, 5),
            disk_usage_pct=drift(prev.disk_usage_pct if prev else 60, 10, 99, 1),
            net_throughput_mbps=drift(prev.net_throughput_mbps if prev else 50, 0, 950, 60),
            uptime_s=(prev.uptime_s + 4) if prev else random.randint(0, 500000),
        )


def make_client(client_id, user, password):
    client = mqtt.Client(
        client_id=client_id,
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    )
    client.username_pw_set(user, password)
    client.tls_set(tls_version=ssl.PROTOCOL_TLS_CLIENT)
    return client


def publisher_loop(args, stop_event):
    client = make_client(f"dashboard-pub-{random.randint(1000,9999)}", args.user, args.password)

    def on_connect(c, u, flags, reason_code, properties=None):
        with state_lock:
            shared_state["connected_pub"] = (str(reason_code) == "Success")

    def on_disconnect(c, u, flags, reason_code, properties=None):
        with state_lock:
            shared_state["connected_pub"] = False

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    try:
        client.connect(args.host, args.port, keepalive=30)
    except Exception as e:
        with state_lock:
            shared_state["last_error"] = f"publisher connect: {e}"
        return

    client.loop_start()
    metrics = None
    try:
        while not stop_event.is_set():
            metrics = ServerMetrics.random_walk(metrics)
            payload = json.dumps({**asdict(metrics), "ts": datetime.now().isoformat(timespec="seconds")})
            client.publish(args.topic, payload, qos=1)
            with state_lock:
                shared_state["msgs_sent"] += 1
            stop_event.wait(args.interval)
    finally:
        client.loop_stop()
        client.disconnect()


def subscriber_loop(args, stop_event):
    client = make_client(f"dashboard-sub-{random.randint(1000,9999)}", args.user, args.password)

    def on_connect(c, u, flags, reason_code, properties=None):
        with state_lock:
            shared_state["connected_sub"] = (str(reason_code) == "Success")
        c.subscribe(args.topic, qos=1)

    def on_disconnect(c, u, flags, reason_code, properties=None):
        with state_lock:
            shared_state["connected_sub"] = False

    def on_message(c, u, msg):
        try:
            data = json.loads(msg.payload.decode())
        except Exception:
            return
        with state_lock:
            shared_state["last_payload"] = data
            shared_state["last_receive"] = datetime.now()
            shared_state["msgs_recv"] += 1

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    try:
        client.connect(args.host, args.port, keepalive=30)
    except Exception as e:
        with state_lock:
            shared_state["last_error"] = f"subscriber connect: {e}"
        return

    client.loop_start()
    stop_event.wait()
    client.loop_stop()
    client.disconnect()


def bar(pct, width=20):
    pct = min(max(pct, 0), 100)
    filled = int(width * pct / 100)
    color = "green" if pct < 60 else ("yellow" if pct < 85 else "red")
    return f"[{color}]{'█' * filled}{'░' * (width - filled)}[/{color}] {pct:5.1f}%"


def temp_str(t):
    color = "green" if t < 60 else ("yellow" if t < 75 else "red")
    return f"[{color}]{t:5.1f} °C[/{color}]"


def render(args):
    with state_lock:
        data = shared_state["last_payload"]
        pub_ok = shared_state["connected_pub"]
        sub_ok = shared_state["connected_sub"]
        sent = shared_state["msgs_sent"]
        recv = shared_state["msgs_recv"]
        err = shared_state["last_error"]

    table = Table(title=f"servidor · {args.host} · topic {args.topic}", expand=True)
    table.add_column("Métrica")
    table.add_column("Valor")

    if data:
        table.add_row("CPU Temp", temp_str(data["cpu_temp_c"]))
        table.add_row("CPU Uso", bar(data["cpu_usage_pct"]))
        table.add_row("Memoria", bar(data["mem_usage_pct"]))
        table.add_row("Disco", bar(data["disk_usage_pct"]))
        table.add_row("Red", f"{data['net_throughput_mbps']:6.1f} Mbps")
        table.add_row("Uptime", f"{data['uptime_s'] // 3600}h {(data['uptime_s'] % 3600)//60}m")
        table.add_row("Última muestra", data["ts"])
    else:
        table.add_row("—", "esperando primer mensaje del broker...")

    pub_status = "[green]OK[/green]" if pub_ok else "[red]DOWN[/red]"
    sub_status = "[green]OK[/green]" if sub_ok else "[red]DOWN[/red]"
    footer = f"publisher: {pub_status}  subscriber: {sub_status}  enviados: {sent}  recibidos: {recv}"
    if err:
        footer += f"\n[bold red]error: {err}[/bold red]"
    return Panel(table, subtitle=footer)


def main():
    parser = argparse.ArgumentParser(description="MQTT publisher (background) + dashboard live (terminal)")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--user", default=DEFAULT_USER)
    parser.add_argument("--password", default=DEFAULT_PASS)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--interval", type=float, default=4.0)
    args = parser.parse_args()

    stop_event = threading.Event()
    threading.Thread(target=publisher_loop, args=(args, stop_event), daemon=True).start()
    threading.Thread(target=subscriber_loop, args=(args, stop_event), daemon=True).start()

    console = Console()
    try:
        with Live(render(args), console=console, refresh_per_second=2) as live:
            while True:
                live.update(render(args))
                time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        time.sleep(0.3)


if __name__ == "__main__":
    main()
