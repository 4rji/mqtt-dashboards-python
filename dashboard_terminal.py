#!/usr/bin/env python3
"""
Digi MQTT Monitor — a generic terminal monitor that subscribes to '#' on a
broker and builds a live dashboard from whatever traffic it sees. It does NOT
assume fixed device models; it infers devices from topic prefixes, tracks a
rolling messages/sec rate, lists unique topics, and streams every message.

Panels:
  - Stat cards: connection, messages received, msgs/sec (rolling 1s),
    unique topics, inferred devices, last message time, uptime
  - Live stream  : timestamp | topic | payload  (pauseable)
  - Topic explorer: topic -> message count
  - Devices      : inferred device -> count / last seen / freshness
  - System health: connection + stale-device alerts

Controls (when stdin is a TTY):  p = pause/resume feed   q = quit

Requirements:
    pip install paho-mqtt rich --break-system-packages

Usage:
    python3 mqtt_monitor.py                          # monitor real traffic
    python3 mqtt_monitor.py --host 10.10.65.67        # different broker
    python3 mqtt_monitor.py --demo                    # self-generate WR-series traffic
    python3 mqtt_monitor.py -u user -P pass --tls      # auth + TLS
"""

import argparse
import json
import random
import re
import ssl
import sys
import threading
import time
from collections import deque, defaultdict
from datetime import datetime

import paho.mqtt.client as mqtt
from rich.live import Live
from rich.table import Table
from rich.console import Console
from rich.panel import Panel
from rich.layout import Layout
from rich.text import Text
from rich.align import Align

SERIAL_RE = re.compile(r"^(?=.*[A-Za-z])(?=.*\d)[A-Za-z0-9]{4,}(?:-[A-Za-z0-9]+)?$")

lock = threading.Lock()
state = {
    "connected": False,
    "reconnects": 0,
    "total": 0,
    "start": time.time(),
    "last_msg_ts": None,
    "err": None,
    "feed": deque(maxlen=1000),          # (dt, topic, payload_str)
    "rate_ts": deque(maxlen=2000),       # epoch times of recent messages
    "topics": defaultdict(int),          # topic -> count
    "devices": {},                       # device -> {"count":int, "last":float, "topics":set}
    "paused": False,
    "frozen_feed": None,
}


# --------------------------------------------------------------------------- #
# Device inference from topic prefix
# --------------------------------------------------------------------------- #
def infer_device(topic):
    segs = [s for s in topic.split("/") if s]
    for s in segs:
        if SERIAL_RE.match(s) and not s.isalpha():
            return s
    # fallback: second segment (e.g. event/<thing>/...) else first
    return segs[1] if len(segs) > 1 else (segs[0] if segs else "unknown")


# --------------------------------------------------------------------------- #
# MQTT monitor
# --------------------------------------------------------------------------- #
def build_client(args):
    c = mqtt.Client(client_id=f"digi-mon-{random.randint(1000,9999)}",
                    callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    if args.user:
        c.username_pw_set(args.user, args.password)
    if args.tls:
        c.tls_set(tls_version=ssl.PROTOCOL_TLS_CLIENT)
    c.reconnect_delay_set(min_delay=1, max_delay=16)
    return c


def monitor_thread(args, stop):
    c = build_client(args)

    def on_connect(cl, u, f, rc, props=None):
        ok = str(rc) == "Success"
        with lock:
            state["connected"] = ok
            if ok:
                state["err"] = None
        cl.subscribe("#", qos=0)

    def on_disconnect(cl, u, f, rc, props=None):
        with lock:
            if state["connected"]:
                state["reconnects"] += 1
            state["connected"] = False

    def on_message(cl, u, m):
        now = time.time()
        payload = m.payload.decode(errors="replace")
        dev = infer_device(m.topic)
        with lock:
            state["total"] += 1
            state["last_msg_ts"] = now
            state["rate_ts"].append(now)
            state["topics"][m.topic] += 1
            d = state["devices"].setdefault(dev, {"count": 0, "last": 0.0, "topics": set()})
            d["count"] += 1
            d["last"] = now
            d["topics"].add(m.topic)
            if not state["paused"]:
                state["feed"].append((datetime.now(), m.topic, payload))

    c.on_connect = on_connect
    c.on_disconnect = on_disconnect
    c.on_message = on_message

    try:
        c.connect(args.host, args.port, keepalive=30)
    except Exception as e:
        with lock:
            state["err"] = f"connect: {e}"
        # keep retrying via loop
    c.loop_start()
    stop.wait()
    c.loop_stop()
    try: c.disconnect()
    except Exception: pass


# --------------------------------------------------------------------------- #
# Optional demo publisher (WR-series style traffic)
# --------------------------------------------------------------------------- #
DEMO_DEVICES = ["WR64-003536", "WR54-001122", "IX20-778899", "EX15-445566"]


def demo_system_payload():
    # matches the double-encoded shape from the real device sample
    inner = {
        "load_avg": {"1min": f"{random.uniform(0.05,1.4):.2f}",
                     "5min": f"{random.uniform(0.05,1.2):.2f}",
                     "15min": f"{random.uniform(0.05,1.0):.2f}"},
        "disk_usage": {"/opt": None, "/etc/config:": None,
                       "ram": f"{random.randint(6,60)}"},
    }
    return json.dumps(json.dumps(inner))   # string containing JSON, as observed


def demo_cellular_payload():
    return json.dumps({"rsrp": random.randint(-115, -70),
                       "rsrq": random.randint(-16, -6),
                       "sinr": random.randint(-2, 25),
                       "network": random.choice(["LTE", "5G-NSA"])})


def demo_gps_payload():
    return json.dumps({"lat": round(44.9 + random.uniform(-0.05, 0.05), 5),
                       "lon": round(-93.4 + random.uniform(-0.05, 0.05), 5),
                       "fix": random.choice([True, True, False])})


def demo_thread(args, stop):
    c = build_client(args)
    try:
        c.connect(args.host, args.port, keepalive=30)
    except Exception:
        return
    c.loop_start()
    try:
        while not stop.is_set():
            dev = random.choice(DEMO_DEVICES)
            kind = random.random()
            if kind < 0.5:
                c.publish(f"event/router/{dev}/system", demo_system_payload(), qos=0)
            elif kind < 0.8:
                c.publish(f"event/router/{dev}/cellular", demo_cellular_payload(), qos=0)
            else:
                c.publish(f"event/router/{dev}/gps", demo_gps_payload(), qos=0)
            stop.wait(random.uniform(0.4, 1.6))
    finally:
        c.loop_stop(); c.disconnect()


# --------------------------------------------------------------------------- #
# Keyboard (p = pause, q = quit) — TTY only
# --------------------------------------------------------------------------- #
def keyboard_thread(stop):
    if not sys.stdin.isatty():
        return
    import termios, tty, select
    fd = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    try:
        tty.setcbreak(fd)
        while not stop.is_set():
            if select.select([sys.stdin], [], [], 0.2)[0]:
                ch = sys.stdin.read(1).lower()
                if ch == "q":
                    stop.set(); break
                if ch == "p":
                    with lock:
                        state["paused"] = not state["paused"]
                        state["frozen_feed"] = list(state["feed"]) if state["paused"] else None
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)


# --------------------------------------------------------------------------- #
# Rendering
# --------------------------------------------------------------------------- #
def fmt_uptime(sec):
    m, s = divmod(int(sec), 60); h, m = divmod(m, 60)
    return f"{h}h {m}m {s}s" if h else f"{m}m {s}s"


def rate_now():
    now = time.time()
    while state["rate_ts"] and now - state["rate_ts"][0] > 1.0:
        state["rate_ts"].popleft()
    return len(state["rate_ts"])


def stat_card(label, value, accent):
    t = Table.grid()
    t.add_column(justify="left")
    t.add_row(Text(str(value), style=f"bold {accent}"))
    t.add_row(Text(label, style="dim"))
    return Panel(t, border_style=accent, padding=(0, 1))


def stats_row():
    conn = state["connected"]
    conn_txt = "[green]Connected[/green]" if conn else "[red]Disconnected[/red]"
    last = datetime.fromtimestamp(state["last_msg_ts"]).strftime("%H:%M:%S") if state["last_msg_ts"] else "—"
    cards = [
        stat_card("broker status", "UP" if conn else "DOWN", "green" if conn else "red"),
        stat_card("messages received", state["total"], "cyan"),
        stat_card("messages / sec", rate_now(), "yellow"),
        stat_card("unique topics", len(state["topics"]), "magenta"),
        stat_card("inferred devices", len(state["devices"]), "blue"),
        stat_card("last message", last, "white"),
        stat_card("uptime", fmt_uptime(time.time() - state["start"]), "green"),
    ]
    g = Table.grid(expand=True)
    for _ in cards: g.add_column(ratio=1)
    g.add_row(*cards)
    return g


def feed_panel(width_hint=80):
    paused = state["paused"]
    src = state["frozen_feed"] if (paused and state["frozen_feed"] is not None) else state["feed"]
    rows = list(src)[-16:]
    t = Table(expand=True, show_edge=False, pad_edge=False)
    t.add_column("Timestamp", style="dim", no_wrap=True, width=15)
    t.add_column("Topic", style="cyan", no_wrap=True, max_width=34)
    t.add_column("Payload", style="white", overflow="ellipsis", no_wrap=True)
    for dt, topic, payload in rows:
        p = payload.replace("\n", " ")
        if len(p) > 90: p = p[:89] + "…"
        t.add_row(dt.strftime("%H:%M:%S.%f")[:-3], topic, p)
    title = "Live stream  [red](PAUSED)[/red]" if paused else "Live stream"
    return Panel(t, title=f"[bold]{title}[/bold]", border_style="white")


def topics_panel():
    t = Table(expand=True, show_edge=False, pad_edge=False)
    t.add_column("Topic", style="cyan", no_wrap=True, overflow="ellipsis")
    t.add_column("Msgs", justify="right", style="yellow", width=6)
    for topic, cnt in sorted(state["topics"].items(), key=lambda kv: -kv[1])[:10]:
        t.add_row(topic, str(cnt))
    return Panel(t, title=f"[bold]Topic explorer[/bold] [dim]{len(state['topics'])}[/dim]", border_style="magenta")


def devices_panel():
    now = time.time()
    t = Table(expand=True, show_edge=False, pad_edge=False)
    t.add_column("Device", style="blue", no_wrap=True, overflow="ellipsis")
    t.add_column("Msgs", justify="right", width=5)
    t.add_column("Age", justify="right", width=6)
    for dev, d in sorted(state["devices"].items(), key=lambda kv: -kv[1]["count"])[:10]:
        age = now - d["last"]
        color = "green" if age < 15 else ("yellow" if age < 45 else "red")
        t.add_row(dev, str(d["count"]), f"[{color}]{int(age)}s[/{color}]")
    return Panel(t, title=f"[bold]Devices[/bold] [dim]{len(state['devices'])}[/dim]", border_style="blue")


def health_line():
    now = time.time()
    alerts = []
    if not state["connected"]:
        alerts.append("[red]broker disconnected[/red]")
    stale = [dev for dev, d in state["devices"].items() if now - d["last"] > 45]
    if stale:
        alerts.append(f"[yellow]{len(stale)} device(s) stale >45s[/yellow]")
    if state["err"]:
        alerts.append(f"[red]{state['err']}[/red]")
    status = "  ·  ".join(alerts) if alerts else "[green]all systems nominal[/green]"
    reconn = state["reconnects"]
    hint = "[dim]p pause · q quit[/dim]"
    return Text.from_markup(f"System health: {status}    [dim]reconnects {reconn} · feed {len(state['feed'])}[/dim]     {hint}")


def build(args):
    with lock:
        header = Text.from_markup(
            f"[bold]Digi MQTT Monitor[/bold]   [dim]{args.host}:{args.port}"
            f"{' TLS' if args.tls else ''} · subscribed to #[/dim]   "
            + ("[green]● Connected[/green]" if state["connected"] else "[red]● Disconnected[/red]")
        )
        stats = stats_row()
        feed = feed_panel()
        topics = topics_panel()
        devs = devices_panel()
        health = health_line()

    root = Layout()
    root.split_column(
        Layout(Panel(Align.center(header), border_style="cyan"), size=3, name="head"),
        Layout(stats, size=5, name="stats"),
        Layout(name="body"),
        Layout(Panel(health, border_style="white"), size=3, name="foot"),
    )
    root["body"].split_row(
        Layout(feed, ratio=3, name="feed"),
        Layout(name="side", ratio=2),
    )
    root["body"]["side"].split_column(Layout(topics), Layout(devs))
    return root


def main():
    p = argparse.ArgumentParser(description="Digi MQTT Monitor — subscribe to # and dashboard the traffic")
    p.add_argument("--host", default="localhost")
    p.add_argument("--port", type=int, default=1883)
    p.add_argument("-u", "--user", default=None)
    p.add_argument("-P", "--password", default=None)
    p.add_argument("--tls", action="store_true", help="use TLS (typically port 8883)")
    p.add_argument("--demo", action="store_true", help="also publish synthetic WR-series traffic")
    args = p.parse_args()

    stop = threading.Event()
    threading.Thread(target=monitor_thread, args=(args, stop), daemon=True).start()
    if args.demo:
        threading.Thread(target=demo_thread, args=(args, stop), daemon=True).start()
    threading.Thread(target=keyboard_thread, args=(stop,), daemon=True).start()

    console = Console()
    try:
        with Live(build(args), console=console, refresh_per_second=6, screen=True) as live:
            while not stop.is_set():
                live.update(build(args))
                time.sleep(1 / 6)
    except KeyboardInterrupt:
        pass
    finally:
        stop.set(); time.sleep(0.3)


if __name__ == "__main__":
    main()
