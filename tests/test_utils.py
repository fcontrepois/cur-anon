import subprocess
import os
import json
import csv

def run_cli(script, args, check=True, capture_output=False):
    cmd = ['python3', script] + args
    return subprocess.run(cmd, check=check, capture_output=capture_output, text=True)

def read_csv(path):
    with open(path, newline='') as f:
        return list(csv.DictReader(f))

def read_json(path):
    with open(path) as f:
        return json.load(f) 