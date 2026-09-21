#!/usr/bin/env python3
"""Probe FinMind datasets for the current observation list and update README status."""
from __future__ import annotations
import argparse, csv, sys
from datetime import date, timedelta, datetime
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "skills" / "skill-finmind-fetch" / "scripts"))
import requests
from dotenv import load_dotenv
from token_env import TokenRotator
API='https://api.finmindtrade.com/api/v4/data'
load_dotenv()
TYPES={
 '1':('DividendDetail','TaiwanStockDividend','direct'),'4':('StockBzPerformance','TaiwanStockFinancialStatements','partial'),'5':('ShowSaleMonChart','TaiwanStockMonthRevenue','direct'),'6':('EquityDistribution','TaiwanStockShareholding','partial'),'7':('StockBzPerformance1','TaiwanStockFinancialStatements','partial'),'8':('ShowK_ChartFlow','TaiwanStockPER','direct'),'9':('StockHisAnaQuar','TaiwanStockFinancialStatements','partial'),'10':('EquityDistributionClassHis','TaiwanStockHoldingSharesPer','permission'),'11':('WeeklyTradingData','TaiwanStockInstitutionalInvestorsBuySellWide','partial'),'12':('ShowMonthlyK_ChartFlow','TaiwanStockPER','direct'),'13':('ShowMarginChart','TaiwanStockMarginPurchaseShortSale','direct'),'14':('ShowMarginChartWeek','TaiwanStockMarginPurchaseShortSale','derived'),'15':('ShowMarginChartMonth','TaiwanStockMarginPurchaseShortSale','derived'),'16':('StockFinDetail','TaiwanStockFinancialStatements','partial'),'17':('ShowWeeklyK_ChartFlow','TaiwanStockPER','direct'),'18':('ShowDailyK_ChartFlow','TaiwanStockPER','direct'),'19':('Dividenschedule','TaiwanStockDividend','direct')}
def load_stocks(path):
    with open(path,encoding='utf-8-sig',newline='') as f: rows=list(csv.reader(f))
    out=[]
    for row in rows:
        if not row or not row[0].strip() or row[0].strip() in {'代號','stock_code'}: continue
        code=row[0].strip().zfill(4); name=row[1].strip() if len(row)>1 else ''
        if code not in {x[0] for x in out}: out.append((code,name))
    return out
def probe(dataset,stock_id,token):
    start=(date.today()-timedelta(days=14)).isoformat(); params={'dataset':dataset,'data_id':stock_id,'start_date':start,'end_date':date.today().isoformat()}
    if token: params['token']=token
    r=requests.get(API,params=params,timeout=90)
    try: payload=r.json()
    except ValueError: payload={}
    return r.status_code,payload
def badge(text,color): return f'![](https://img.shields.io/badge/{text.replace(" ","%20")}-{color})'
def main():
    p=argparse.ArgumentParser(); p.add_argument('--stock-list',default='data/StockID_TWSE_TPEX.csv'); p.add_argument('--readme',default='README.md'); args=p.parse_args()
    stocks=load_stocks(args.stock_list); sample=next((x for x in stocks if x[0]!='0000'),stocks[0] if stocks else ('2330','')); rot=TokenRotator(); rows=[]; now=datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')
    for type_id,(folder,dataset,mode) in TYPES.items():
        if mode=='derived': api_text='由 Type 13 daily 聚合'; status='derived'; color='blue'; reason='不重複下載'
        else:
            token=rot.next(); code,payload=probe(dataset,sample[0],token); msg=str(payload.get('msg',''))
            if payload.get('msg')=='success': api_text='success'; status='ready'; color='brightgreen'; reason=f"sample rows={len(payload.get('data',[]))}"
            elif 'level' in msg.lower() or 'sponsor' in msg.lower(): api_text='permission'; status='permission'; color='orange'; reason='FinMind tier 不足'
            else: api_text='failed'; status='failed'; color='red'; reason=msg[:80] or f'HTTP {code}'
        rows.append(f'| {type_id} | {folder} | {dataset} | {len(stocks)} | {api_text} | {mode} | {badge(status,color)} | {reason} |')
    table='\n'.join(['| Type | GoodInfo type | FinMind dataset | Watchlist | API | Adapter | Status | Note |','| -- | -- | -- | --: | -- | -- | -- | -- |',*rows])
    block=f"""<!-- FINMIND_STATUS_START -->
## Status

Update time: {now}

Token rotation pool: `{rot.count}` configured

{table}
<!-- FINMIND_STATUS_END -->"""
    path=Path(args.readme); content=path.read_text(encoding='utf-8'); start='<!-- FINMIND_STATUS_START -->'; end='<!-- FINMIND_STATUS_END -->'
    if start in content and end in content:
        a=content.index(start); b=content.index(end,a)+len(end); content=content[:a]+block+content[b:]
    else: content=block+'\n\n'+content
    path.write_text(content,encoding='utf-8'); print(f'Updated {path} for {len(stocks)} watchlist stocks')
if __name__=='__main__': main()
