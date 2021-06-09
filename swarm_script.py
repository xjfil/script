#!/usr/bin/env python3
import time
import json
import os
from typing import Dict

import requests
import sys
import yaml
import socket


def get_disk_size(path):
    total = 0
    used = 0
    try:
        disk_total_str = os.popen("df -P " + path + " | awk 'NR==2{print $2}'").readlines()[0]
        disk_used_str = os.popen("df -P " + path + " | awk 'NR==2{print $3}'").readlines()[0]
        total = int(disk_total_str)
        used = int(disk_used_str)
    except Exception as e:
        pass
    return total, used


def get_peers(port)->int:
    res = requests.get("http://127.0.0.1:{}/peers".format(port))
    response = res.json()
    return len(response.get("peers", []))


def get_cheque(port):
    total = 0
    res = requests.get("http://127.0.0.1:{}/chequebook/cheque".format(port))
    response = res.json()
    cheques = response.get("lastcheques", [])
    for cheque in cheques:
        peer_id = cheque.get("peer", None)
        if peer_id is None:
            continue
        response = requests.get("http://127.0.0.1:{}/chequebook/cashout/{}".format(port, peer_id))
        response = response.json()
        total += response.get("uncashedAmount", 0)
    return len(cheques), total

def get_father_path(device):
    path_sec = device.split("/")
    path_sec.pop()
    return "/".join(path_sec)


def main() -> None:
    if len(sys.argv) != 3:
        raise Exception("invalid params num")
    with open(sys.argv[1], "r") as f:
        config = yaml.safe_load(f)
    hostname = socket.gethostname()
    services = config.get("services", {})
    cache = {}
    volumes: Dict[str] = config.get("volumes", {})
    total_and_used = {}
    for volume, value in volumes.items():
        if not volume.startswith("bee"):
            continue
        device: str = value.get("driver_opts", {}).get("device", "")
        disk_path = get_father_path(device)
        if cache.get(disk_path, None) is None:
            cache[disk_path] = get_disk_size(disk_path)
        total_and_used[volume] = cache[disk_path]
    swarm_nodes = []
    now = time.time()
    for service, value in services.items():
        if not service.startswith("bee"):
            continue
        ports = value.get("ports", None)
        if ports is None:
            continue
        url_port = 0
        for port in ports:
            if "1635" not in port:
                continue
            tmp = port.split(":")
            if len(tmp) >= 3:
                url_port = tmp[1]
            elif len(tmp) == 2:
                url_port =  tmp[0]
        if url_port == 0:
            continue
        vols = value.get("volumes", None)
        if vols is None or len(vols) < 1:
            continue
        try:
            vol = vols[0].split(":")[0]
            total, used = total_and_used[vol]
            cheques_count, uncashed = get_cheque(url_port)
            peers = get_peers(url_port)
            swarm_nodes.append(
                {
                    "name": "{}-{}".format(hostname, service),
                    "peers": peers,
                    "cheque": cheques_count,
                    "uncashed": uncashed,
                    "disk_free": total - used,
                    "disk_used": used,
                    "disk_total": total,
                    "time": now
                }
            )
        except Exception as e:
            pass
    headers = {
        "Content-Type": "application/json"
    }
    requests.post(sys.argv[2], data=json.dumps(swarm_nodes), headers=headers)



if __name__ == '__main__':
    main()
