#!/usr/bin/env python3
"""Download the same observation-list CSV used by Get觀察名單.py."""
from __future__ import annotations
import argparse
from pathlib import Path
import requests
URL = "https://raw.githubusercontent.com/wenchiehlee/Selenium-Actions.Auction/main/%E8%A7%80%E5%AF%9F%E5%90%8D%E5%96%AE.csv"
def main():
    p=argparse.ArgumentParser(); p.add_argument('--output',default='data/StockID_TWSE_TPEX.csv'); args=p.parse_args()
    response=requests.get(URL,timeout=30); response.raise_for_status(); text=response.content.decode('utf-8')
    if not any(line.startswith('0000,') for line in text.splitlines()):
        text=text.rstrip()+"\n0000,台灣加權指數\n"
    output=Path(args.output); output.parent.mkdir(parents=True,exist_ok=True); output.write_text(text,encoding='utf-8'); print(f'Wrote {sum(bool(line.strip()) for line in text.splitlines())} watchlist rows to {output}')
if __name__=='__main__': main()
