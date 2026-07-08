#!/usr/bin/env python3
"""
A股每日收盘数据 — 选股通DDC API版
数据源: 选股通(DDC+Flash) + 东方财富(封单+BK0815) + 腾讯财经(名称)
用法: python daily_fetch.py [YYYY-MM-DD]
输出: data/YYYY-MM-DD.json + dashboard.html
"""
import json, sys, urllib.request, time, hmac, hashlib, os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date, timedelta, timezone
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"

# ── DDC API ──
DDC_BASE = "https://api-ddc-wscn.xuangubao.com.cn"
DDC_FIELDS = ("symbol,last_px,high_px,low_px,open_px,preclose_px,"
              "px_change,px_change_rate,turnover_ratio,turnover_volume,"
              "circulation_value,market_value,pe_rate,delisting_date,"
              "amplitude,volume_ratio")
DDC_BATCH = 80
NAME_CACHE = DATA_DIR / "name_cache.json"
AMT_CACHE = DATA_DIR / "amt_cache.json"


def ddc_sym(c6):
    return f"{c6}.SS" if c6.startswith(("6","9")) else f"{c6}.SZ"


def gen_codes():
    codes = []
    for i in range(600000, 604000): codes.append(str(i).zfill(6))
    for i in range(688000, 689000): codes.append(str(i).zfill(6))  # 扩展到689000覆盖中芯国际
    for i in range(1, 4000): codes.append(str(i).zfill(6))
    for i in range(300000, 301500): codes.append(str(i).zfill(6))
    return codes


def limit_pct(c6):
    return 20.0 if c6.startswith("688") or c6.startswith("30") else 10.0


def load_names():
    if NAME_CACHE.exists():
        with open(NAME_CACHE, encoding="utf-8") as f: return json.load(f)
    return {}


def save_names(c):
    with open(NAME_CACHE, "w", encoding="utf-8") as f: json.dump(c, f, ensure_ascii=False)

def load_amts():
    if AMT_CACHE.exists():
        with open(AMT_CACHE, encoding="utf-8") as f: return json.load(f)
    return {}

def save_amts(c):
    with open(AMT_CACHE, "w", encoding="utf-8") as f: json.dump(c, f, ensure_ascii=False)


def last_trading_day(today_str):
    """返回上一个交易日（跳过周末）"""
    d = datetime.strptime(today_str, "%Y-%m-%d")
    while True:
        d = d - timedelta(days=1)
        if d.weekday() < 5:
            return d.strftime("%Y-%m-%d")


# ── DDC 全市场扫描 ──
def fetch_ddc(symbols):
    url = f"{DDC_BASE}/market/real?fields={DDC_FIELDS}&prod_code={','.join(symbols)}"
    req = urllib.request.Request(url, headers={"User-Agent":UA, "Origin":"https://xuangutong.com.cn"})
    try:
        r = urllib.request.urlopen(req, timeout=15)
        d = json.loads(r.read().decode())
    except: return []
    fs = d.get("data",{}).get("fields",[])
    snap = d.get("data",{}).get("snapshot",{})
    out = []
    for sym, vals in snap.items():
        info = dict(zip(fs, vals))
        c6 = sym.split(".")[0]
        if c6.startswith("8") or info.get("delisting_date",0) > 0: continue
        px = info.get("last_px",0) or 0
        pre = info.get("preclose_px",0) or 0
        if px <= 0: continue
        lp = limit_pct(c6)
        lup = round(pre*(1+lp/100),2)
        ldn = round(pre*(1-lp/100),2)
        vol = info.get("turnover_volume",0) or 0
        out.append({"code":c6,"name":"","price":px,
            "high":info.get("high_px",0) or 0,
            "low":info.get("low_px",0) or 0,
            "open":info.get("open_px",0) or 0,
            "prev_close":pre,
            "change_pct":info.get("px_change_rate",0) or 0,
            "turnover_ratio":info.get("turnover_ratio",0) or 0,
            "amount_yi":round(px*vol/1e8,2),
            "mcap_yi":round((info.get("market_value",0) or 0)/1e8,2),
            "pe_rate":info.get("pe_rate",0) or 0,
            "limit_up_price":lup,"limit_down_price":ldn,"limit_pct":lp})
    return out


def fetch_all():
    codes = gen_codes()
    batches = []
    for i in range(0, len(codes), DDC_BATCH):
        syms = [ddc_sym(c) for c in codes[i:i+DDC_BATCH]]
        batches.append(syms)
    print(f"  DDC: {len(codes)}候选 {len(batches)}批次 8线程")
    alls = []

    def _f(idx_batch):
        return fetch_ddc(idx_batch[1])

    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = [ex.submit(_f, (i,b)) for i,b in enumerate(batches)]
        for f in as_completed(futures):
            alls.extend(f.result())

    print(f"  DDC完成: {len(alls)}只有效股票")
    return alls


# ── 名称（腾讯财经+缓存）──
def enrich_names(stocks):
    cache = load_names()
    all_codes = [s["code"] for s in stocks]

    # 刷新含ST/退的缓存名（摘帽检测）
    stale_st = [c for c in all_codes if c in cache and any(k in cache[c] for k in ["ST","退"])]
    if stale_st:
        print(f"  摘帽检测: {len(stale_st)}只ST/退缓存待刷新...")
        for c in stale_st:
            del cache[c]

    amt_cache = load_amts()
    missing = [c for c in all_codes if c not in cache]
    if missing:
        print(f"  名称: {len(missing)}只未缓存, 腾讯财经批量...")
        for i in range(0, len(missing), 40):
            batch = missing[i:i+40]
            pref = []
            for c in batch:
                pref.append(f"sh{c}" if c.startswith(("6","9")) else f"sz{c}")
            url = "https://qt.gtimg.cn/q=" + ",".join(pref)
            req = urllib.request.Request(url, headers={"User-Agent":UA})
            try:
                r = urllib.request.urlopen(req, timeout=10)
                raw = r.read().decode("gbk", errors="replace")
                for line in raw.split(";"):
                    if '="' not in line: continue
                    v = line.split('"')[1].split("~")
                    if len(v) < 5: continue
                    k = line.split("=")[0].split("_")[-1]
                    c6 = k[2:]
                    if v[1] and c6 in missing: cache[c6] = v[1]
                    if len(v) > 37 and v[37] and c6 in missing:
                        amt_cache[c6] = round(float(v[37]) / 10000, 2)  # 万元→亿
            except: pass
            time.sleep(0.05)
        save_names(cache)
        save_amts(amt_cache)
    for s in stocks:
        s["name"] = cache.get(s["code"], s["code"])


# ── 腾讯成交额补充 ──
def enrich_amounts_tx(stocks, top_n=100):
    """用腾讯财经补充前N只股票的准确成交额"""
    sorted_by_ddc = sorted(stocks, key=lambda x: -x["amount_yi"])
    candidates = sorted_by_ddc[:top_n]
    print(f"  腾讯成交额: 补充前{len(candidates)}只...")
    updated = 0
    for i in range(0, len(candidates), 80):
        batch = candidates[i:i+80]
        pref = []
        for s in batch:
            pref.append(f"sh{s['code']}" if s['code'].startswith(("6","9")) else f"sz{s['code']}")
        url = "https://qt.gtimg.cn/q=" + ",".join(pref)
        req = urllib.request.Request(url, headers={"User-Agent":UA})
        try:
            r = urllib.request.urlopen(req, timeout=10)
            raw = r.read().decode("gbk", errors="replace")
            for line in raw.split(";"):
                if '="' not in line: continue
                v = line.split('"')[1].split("~")
                if len(v) < 38: continue
                k = line.split("=")[0].split("_")[-1]
                c6 = k[2:]
                if v[37]:
                    amt_yi = round(float(v[37]) / 10000, 2)
                    for s in batch:
                        if s["code"] == c6:
                            s["amount_yi"] = amt_yi
                            updated += 1
                            break
        except: pass
        time.sleep(0.03)
    print(f"  腾讯成交额: {updated}只更新")


# ── 选股通Flash API: 涨停/跌停池 ──
def fetch_flash_pool(pool_name, date_str):
    """选股通Flash API - 涨停池/跌停池, 与盯盘页数据一致"""
    url = f"https://flash-api.xuangubao.cn/api/pool/detail?pool_name={pool_name}&date={date_str}"
    req = urllib.request.Request(url, headers={
        "User-Agent": UA, "Origin": "https://xuangutong.com.cn",
        "Referer": "https://xuangutong.com.cn/dingpan"})
    try:
        r = urllib.request.urlopen(req, timeout=15)
        d = json.loads(r.read().decode())
        pool = d.get("data", [])
        if pool is None: pool = []
        result = []
        for item in pool:
            sym = item.get("symbol", "")
            code = sym.split(".")[0] if "." in sym else sym
            name = item.get("stock_chi_name", "")
            if not code or any(k in name for k in ["ST","退"]): continue
            # 解析封板时间 (Unix timestamp -> HH:MM:SS)
            fbt_ts = item.get("first_limit_up", 0) or 0
            lbt_ts = item.get("last_limit_up", 0) or 0
            fbt = datetime.fromtimestamp(fbt_ts).strftime("%H:%M:%S") if fbt_ts > 0 else ""
            lbt = datetime.fromtimestamp(lbt_ts).strftime("%H:%M:%S") if lbt_ts > 0 else ""
            # 封单金额估算: buy_lock_volume_ratio * 流通市值
            blvr = item.get("buy_lock_volume_ratio", 0) or 0
            nrc = item.get("non_restricted_capital", 0) or 0
            sealed = round(blvr * nrc / 1e8, 2) if blvr > 0 and nrc > 0 else 0
            # 题材(用板块名,≤10字) + 详细归因
            reason = ""
            detail_reason = ""
            sr = item.get("surge_reason", {})
            if isinstance(sr, dict):
                plates = sr.get("related_plates", [])
                if plates and isinstance(plates, list) and len(plates) > 0:
                    reason = str(plates[0].get("plate_name", ""))[:10]
                if not reason:
                    reason = str(sr.get("stock_reason", ""))[:10]
                detail_reason = str(sr.get("stock_reason", ""))
            result.append({
                "code": code, "name": name,
                "price": item.get("price", 0) or 0,
                "change_pct": round((item.get("change_percent", 0) or 0) * 100, 2),
                "board_days": item.get("limit_up_days", 0) or 0,
                "first_block_time": fbt,
                "last_block_time": lbt,
                "open_count": item.get("break_limit_up_times", 0) or 0,
                "sealed_amount": sealed,
                "turnover_ratio": round((item.get("turnover_ratio", 0) or 0) * 100, 2),
                "mcap_yi": round(nrc / 1e8, 2),
                "reason": reason,
                "detail_reason": detail_reason,
                "limit_pct": 20.0 if code.startswith("688") or code.startswith("30") else 10.0,
            })
        return result
    except Exception as e:
        print(f"  [WARN] Flash {pool_name}: {e}")
        return []


# ── 东方财富涨停详情（补充封单金额）──
def fetch_em_sealed_amount(date_str):
    """东方财富ZTPool - 仅用于获取封单金额补充Flash数据"""
    date_c = date_str.replace("-", "")
    url = (f"http://push2ex.eastmoney.com/getTopicZTPool"
           f"?ut=7eea3edcaed734bea9cbfc24409ed989"
           f"&dpt=wz.ztzt&Pageindex=0&pagesize=1000"
           f"&sort=fbt:asc&date={date_c}")
    req = urllib.request.Request(url, headers={
        "User-Agent": UA, "Referer": "http://quote.eastmoney.com/"})
    try:
        r = urllib.request.urlopen(req, timeout=15)
        d = json.loads(r.read().decode())
        pool = d.get("data", {}).get("pool", [])
        result = {}
        for item in pool:
            c = str(item.get("c", "")).zfill(6)
            if not c: continue
            fund = item.get("fund", 0) or 0
            result[c] = round(fund / 1e8, 2)
        print(f"  东财封单: {len(result)}只有效封单")
        return result
    except Exception as e:
        print(f"  [WARN] 东财封单: {e}")
        return {}


# ── 昨日涨停板块 BK0815 ──
def fetch_yesterday_zt_board():
    """获取东方财富'昨日涨停'概念板块(BK0815)今日涨跌幅"""
    url = ("https://push2.eastmoney.com/api/qt/stock/get"
           "?secid=90.BK0815&fields=f50,f43,f170,f171&fltt=2&invt=2")
    req = urllib.request.Request(url, headers={
        "User-Agent": UA, "Referer": "http://quote.eastmoney.com/"})
    try:
        r = urllib.request.urlopen(req, timeout=10)
        d = json.loads(r.read().decode())
        data = d.get("data", {})
        if data and data.get("f50") is not None:
            return {"change_pct": data.get("f50", 0),
                    "current": (data.get("f43", 0) or 0) / 100,
                    "up_count": data.get("f170", 0),
                    "down_count": data.get("f171", 0)}
    except Exception as e:
        print(f"  [WARN] BK0815板块: {e}")
    return None


# ── 炸板计算（DDC价格数据）──
def compute_board_break(all_stocks, limit_up_codes):
    """用DDC数据计算炸板: 触及涨停但未封住"""
    breaks = []
    for s in all_stocks:
        c = s["code"]
        if c in limit_up_codes: continue  # 已在涨停中
        lup, hi, px = s["limit_up_price"], s["high"], s["price"]
        if lup > 0 and hi > 0 and hi >= lup and px < lup * 0.999:
            breaks.append({
                "code": c, "name": s["name"],
                "price": px, "high": hi,
                "change_pct": s["change_pct"],
                "limit_pct": s["limit_pct"],
                "amount_yi": s["amount_yi"],
                "turnover_ratio": s["turnover_ratio"],
                "mcap_yi": s["mcap_yi"],
                "limit_up_price": lup,
            })
    breaks.sort(key=lambda x: -x.get("amount_yi", 0))
    return breaks


def is_bad(name):
    return bool(name) and any(k in name for k in ["ST","退","st"])


# ── 题材归类 ──
def compute_theme_boards(limit_up_pool):
    """按题材归类涨停股, 返回涨停数≥5只的前三大题材, 组内按板数降序"""
    themes = {}
    for s in limit_up_pool:
        reason = s.get("reason", "").strip()
        if not reason:
            continue
        themes.setdefault(reason, []).append(s)
    top = sorted(
        [(k, v) for k, v in themes.items() if len(v) >= 4],
        key=lambda x: -len(x[1])
    )[:3]
    result = []
    for theme_name, stocks in top:
        stocks.sort(key=lambda x: (-x.get("board_days", 1), -x.get("sealed_amount", 0)))
        # 拼凑涨停潮原因（取前3只非空detail_reason）
        reasons = []
        for s in stocks:
            dr = s.get("detail_reason", "").strip()
            if dr and dr not in reasons:
                reasons.append(dr)
            if len(reasons) >= 2:
                break
        theme_reason = "；".join(reasons) if reasons else ""

        result.append({
            "theme": theme_name,
            "count": len(stocks),
            "reason": theme_reason,
            "stocks": [{"code": s["code"], "name": s["name"],
                        "board_days": s.get("board_days", 1),
                        "change_pct": s.get("change_pct", 0),
                        "limit_time": s.get("first_block_time", "")} for s in stocks]
        })
    return result


# ── 龙虎榜：机构净买/净卖（东财机构买卖统计）──
def fetch_dragon_tiger_inst(today_str, reason_map=None):
    """从东财机构买卖统计获取数据，拆分为净买>1亿和净卖>8000万"""
    if reason_map is None:
        reason_map = {}
    try:
        url = ("https://datacenter-web.eastmoney.com/api/data/v1/get"
               "?reportName=RPT_ORGANIZATION_TRADE_DETAILS"
               "&columns=ALL"
               "&pageNumber=1&pageSize=200&sortColumns=TRADE_DATE&sortTypes=-1"
               "&source=WEB&client=WEB")
        req = urllib.request.Request(url, headers={
            "User-Agent": UA, "Referer": "https://data.eastmoney.com/stock/jgmmtj.html"})
        r = urllib.request.urlopen(req, timeout=15)
        d = json.loads(r.read().decode())
        data = d.get("result", {}).get("data", [])
    except Exception as e:
        print(f"  [WARN] 机构买卖统计: {e}")
        return [], []

    # 取最新日期
    latest = [row for row in data if str(row.get("TRADE_DATE", ""))[:10] == today_str]
    if not latest:
        # fallback: use most recent date in data
        dates = sorted({str(r.get("TRADE_DATE", ""))[:10] for r in data}, reverse=True)
        if dates:
            use_date = dates[0]
            latest = [row for row in data if str(row.get("TRADE_DATE", ""))[:10] == use_date]
            print(f"  机构统计最新日期: {use_date}")

    net_buy = []
    net_sell = []
    for row in latest:
        code = row.get("SECURITY_CODE", "")
        name = row.get("SECURITY_NAME_ABBR", "")
        chg = row.get("CHANGE_RATE", 0) or 0
        buy_amt = row.get("BUY_AMT", 0) or 0
        sell_amt = row.get("SELL_AMT", 0) or 0
        net_amt = row.get("NET_BUY_AMT", 0) or 0
        total_amt = row.get("ACCUM_AMOUNT", 0) or 0
        ratio = row.get("RATIO", 0) or 0
        expl = str(row.get("EXPLANATION", "") or "")
        is_3day = "3个交易" in expl or "三个交易" in expl
        entry = {
            "code": code, "name": name,
            "change_pct": round(chg, 2),
            "buy_yi": round(buy_amt / 1e8, 2),
            "sell_yi": round(sell_amt / 1e8, 2),
            "inst_net_yi": round(net_amt / 1e8, 2),
            "inst_net_ratio": round(ratio, 1),
            "reason": reason_map.get(code, ""),
            "is_3day": is_3day,
        }
        if net_amt >= 1e8:
            net_buy.append(entry)
        elif net_amt <= -80000000:
            net_sell.append(entry)

    net_buy.sort(key=lambda x: -x["inst_net_yi"])
    net_sell.sort(key=lambda x: x["inst_net_yi"])  # most negative first
    return net_buy, net_sell


# ── 严重异常波动（交易所公告，巨潮聚合）──
def fetch_abnormal_volatility(today_str):
    """从巨潮获取严重异常波动公告，过滤向上波动+异常期间内"""
    from datetime import datetime as dt, timedelta
    stocks = {}
    today_d = dt.strptime(today_str, "%Y-%m-%d")
    for plate in ['szse_main', 'szse_cyb', 'sse_main']:
        payload = (f'stock=&tabName=fulltext&pageSize=50&pageNum=1'
                   f'&column=&category=&plate={plate}&seDate='
                   f'&searchkey=严重异常波动&secid=&sortName=&sortType=&isHLtitle=true')
        try:
            req = urllib.request.Request(
                "https://www.cninfo.com.cn/new/hisAnnouncement/query",
                data=payload.encode(),
                headers={"User-Agent": UA, "Content-Type": "application/x-www-form-urlencoded",
                         "Referer": "https://www.cninfo.com.cn/"})
            r = urllib.request.urlopen(req, timeout=15)
            d = json.loads(r.read().decode())
            for a in (d.get("announcements") or []):
                title = a.get("announcementTitle", "")
                sec_code = a.get("secCode", "")
                sec_name = a.get("secName", "")
                ts = a.get("announcementTime", 0)
                if "向下" in title or "*ST" in sec_name:
                    continue
                if isinstance(ts, (int, float)):
                    if ts > 1e12: ts = ts / 1000
                    try:
                        date_str = dt.fromtimestamp(ts).strftime("%Y-%m-%d")
                    except:
                        date_str = str(ts)[:10]
                else:
                    date_str = str(ts)[:10]
                if sec_code and sec_code not in stocks:
                    stocks[sec_code] = {"code": sec_code, "name": sec_name, "start_date": date_str}
        except Exception as e:
            print(f"  [WARN] 巨潮{plate}: {e}")

    active = []
    for code, s in stocks.items():
        try:
            start = dt.strptime(s["start_date"], "%Y-%m-%d")
            end = start + timedelta(days=14)  # ~10个交易日
            if today_d <= end:
                s["end_date"] = end.strftime("%Y-%m-%d")
                active.append(s)
        except:
            pass
    active.sort(key=lambda x: x["start_date"], reverse=True)
    return active


# ── K线连板(仅用于验证,非主要数据源) ──
def kline(code, days=10):
    pfx = "sh" if code.startswith(("6","9")) else "sz"
    url = f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={pfx}{code},day,,,{days},qfq"
    req = urllib.request.Request(url, headers={"User-Agent":UA, "Referer":"https://gu.qq.com/"})
    try:
        r = urllib.request.urlopen(req, timeout=10)
        d = json.loads(r.read().decode())
        st = d.get("data",{}).get(f"{pfx}{code}",{})
        return st.get("day",[]) or st.get("qfqday",[])
    except: return []


# ── HTML ──
def gen_html(data, path):
    j = json.dumps(data, ensure_ascii=False)
    h = f'''<!DOCTYPE html><html lang="zh-CN"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"><title>A股收盘 — {data["date"]}</title><style>
:root{{--bg:#0d1117;--card:#161b22;--bd:#30363d;--tx:#c9d1d9;--t2:#8b949e;--rd:#f85149;--gn:#3fb950;--ac:#58a6ff;}}
*{{margin:0;padding:0;box-sizing:border-box;}}
body{{font-family:-apple-system,BlinkMacSystemFont,'PingFang SC','Microsoft YaHei',sans-serif;background:var(--bg);color:var(--tx);min-height:100vh;}}
.hdr{{background:#1a1f2b;border-bottom:1px solid var(--bd);padding:10px 20px;display:flex;justify-content:space-between;align-items:center;position:sticky;top:0;z-index:100;}}
.hdr h1{{font-size:17px;font-weight:700;}}.hdr span{{color:var(--t2);font-size:12px;}}
.ctn{{max-width:1600px;margin:0 auto;padding:8px 10px;display:grid;gap:6px;}}
.card{{background:var(--card);border:1px solid var(--bd);border-radius:6px;overflow:hidden;}}
.ch{{padding:7px 12px;border-bottom:1px solid var(--bd);font-weight:600;font-size:13px;display:flex;justify-content:space-between;align-items:center;background:rgba(255,255,255,.02);}}
.badge{{font-size:10px;padding:1px 6px;border-radius:3px;background:var(--ac);color:#fff;}}
table{{width:100%;border-collapse:collapse;font-size:11px;table-layout:fixed;}}
th{{background:rgba(255,255,255,.03);padding:4px 8px;text-align:right;font-weight:600;color:var(--t2);border-bottom:2px solid var(--bd);font-size:10px;white-space:nowrap;cursor:pointer;user-select:none;}}
th:first-child{{text-align:left;}}td{{padding:3px 8px;text-align:right;border-bottom:1px solid rgba(48,54,61,.5);white-space:nowrap;}}td:first-child{{text-align:left;}}tr:hover{{background:rgba(255,255,255,.02);}}.bs thead{{position:sticky;top:0;z-index:1;}}.bs thead th{{background:var(--card);border-bottom-color:var(--ac);}}
.up{{color:var(--rd);}}.dn{{color:var(--gn);}}.sn{{font-weight:500;display:inline-block;width:4.2em;text-align:left;}}.sc{{color:var(--t2);font-size:10px;margin-left:6px;}}.rt{{color:var(--ac);font-size:10px;margin-left:4px;}}
.g2{{display:grid;grid-template-columns:1fr 1fr;gap:6px;}}@media(max-width:900px){{.g2{{grid-template-columns:1fr;}}}}
.g3{{display:grid;grid-template-columns:repeat(3,1fr);gap:6px;}}@media(max-width:1100px){{.g3{{grid-template-columns:1fr;}}}}
.g4{{display:grid;grid-template-columns:repeat(4,1fr);gap:6px;}}@media(max-width:1400px){{.g4{{grid-template-columns:1fr 1fr;}}}}@media(max-width:700px){{.g4{{grid-template-columns:1fr;}}}}
.sr{{display:grid;grid-template-columns:repeat(5,1fr);gap:6px;padding:6px;}}@media(max-width:1100px){{.sr{{grid-template-columns:repeat(3,1fr);}}}}@media(max-width:600px){{.sr{{grid-template-columns:repeat(2,1fr);}}}}
.si{{text-align:center;padding:8px;background:rgba(255,255,255,.02);border-radius:5px;border:1px solid var(--bd);}}
.sv{{font-size:22px;font-weight:700;line-height:1.2;}}.sl{{font-size:11px;color:var(--t2);margin-top:2px;}}.sb{{font-size:10px;color:var(--t2);margin-top:1px;}}
.bs{{max-height:450px;overflow-y:auto;}}.em{{padding:20px;text-align:center;color:var(--t2);font-size:12px;}}
.ft{{text-align:center;padding:8px;color:var(--t2);font-size:10px;border-top:1px solid var(--bd);margin-top:8px;}}
::-webkit-scrollbar{{height:4px;width:4px;}}::-webkit-scrollbar-thumb{{background:var(--bd);border-radius:2px;}}.hi{{color:var(--rd);font-weight:700;}}.tc{{color:#d2991d;font-size:10px;white-space:nowrap;}}
</style></head><body>
<div class="hdr"><h1>A股收盘数据</h1><span>{data["date"]} | {data["update_time"]}</span></div>
<div class="ctn" id="app"></div><div class="ft">选股通Flash+DDC + 东方财富 + BK0815 | 仅供参考</div>
<script>
const D={j};
function F(n,d){{return(n==null||isNaN(n))?'—':Number(n).toFixed(d||2);}}
function A(n){{if(n==null||isNaN(n))return'—';return n>=10000?(n/10000).toFixed(2)+'万亿':n.toFixed(0)+'亿';}}
function P(n){{if(n==null||isNaN(n))return'—';var v=Number(n);return(v>0?'+':'')+v.toFixed(2)+'%';}}
function C(n){{return n>0?'up':(n<0?'dn':'');}}
function E(s){{var d=document.createElement('div');d.textContent=s||'';return d.innerHTML;}}
document.getElementById('app').innerHTML=
'<div class="g4">'+
 '<div class="card"><div class="ch">主要指数<span class="badge">涨跌幅/成交额/昨成交额/同比</span></div><table><thead><tr><th>指数</th><th>涨跌幅</th><th>成交额</th><th>昨成交额</th><th>成交额同比</th></tr></thead><tbody>'+
 D.indices.map(function(i){{var pa=i.prev_amount_yi;var ar=(pa!=null&&pa>0)?((i.amount_yi/pa-1)*100):null;var arT=ar!=null?(ar>0?\'+\':\'\')+ar.toFixed(1)+\'%\':\'—\';var arC=ar!=null&&ar>0?\'up\':(ar!=null&&ar<0?\'dn\':\'\');return\'<tr><td><span class="sn">\'+E(i.name)+\'</span></td><td class="\'+C(i.change_pct)+\'">\'+P(i.change_pct)+\'</td><td>\'+A(i.amount_yi)+\'</td><td>\'+(pa!=null?A(pa):\'—\')+\'</td><td class="\'+arC+\'">\'+arT+\'</td></tr>\';}}).join('')+
 '</tbody></table></div>'+
 (function(){{var top=D.top20_turnover||[];var parts=[top.slice(0,7),top.slice(7,14),top.slice(14,20)];var labels=["1-7","8-14","15-20"];return parts.map(function(p,i){{return\'<div class="card"><div class="ch">成交额TOP20<span class="badge">\'+labels[i]+\'</span></div><table><thead><tr><th style="width:24px;">#</th><th>股票</th><th>成交额</th><th>涨幅</th></tr></thead><tbody>\'+p.map(function(s){{return\'<tr><td style="width:24px;text-align:center;">\'+s.rank+\'</td><td><span class="sn">\'+E(s.name)+\'</span><span class="sc">\'+s.code+\'</span></td><td class="\'+C(s.change_pct)+\'"><strong>\'+A(s.amount_yi)+\'</strong></td><td class="\'+C(s.change_pct)+\'">\'+P(s.change_pct)+\'</td></tr>\';}}).join(\'\')+\'</tbody></table></div>\';}}).join(\'\');}})()+
 '</div>'+
 '<div class="card"><div class="ch">涨跌停统计<span class="badge">选股通盯盘数据 | 剔除ST/退市</span></div><div class="sr">'+
 (function(){{var ls=D.limit_stats;var pu=ls.prev_up_count!=null?ls.prev_up_count:\'—\';var pd=ls.prev_down_count!=null?ls.prev_down_count:\'—\';var pdate=ls.prev_date?\'(\'+ls.prev_date+\')\':\'\';var ud=ls.prev_up_count!=null?(ls.up_count-ls.prev_up_count):null;var dd=ls.prev_down_count!=null?(ls.down_count-ls.prev_down_count):null;function dt(d){{if(d==null)return\'\';return d>0?\'<span class="up">+\'+d+\'</span>\':(d<0?\'<span class="dn">\'+d+\'</span>\':\'<span style="color:var(--t2);">平</span>\');}}var br=ls.break_count||0,brr=ls.break_rate||0;var ar=ls.advance_rate,ac=ls.advance_count,pt=ls.prev_up_total;var zb=D.zt_board;var av=zb?zb.change_pct:ls.prev_up_avg_change;var avSrc=zb?\'BK0815\':(ls.prev_up_avg_change!=null?\'手动\':\'\');var arT=ar!=null?ar+\'%\':\'—\';var arS=ar!=null&&ac!=null&&pt!=null?ac+\'/\'+pt+\'只晋级\':\'—\';var avT=av!=null?(av>0?\'+\':\'\')+av.toFixed(2)+\'%\':\'—\';var avC=av!=null&&av>0?\'up\':(av!=null&&av<0?\'dn\':\'\');return\'<div class="si"><div class="sv \'+(ls.up_count>0?\'up\':\'\')+\'">\'+ls.up_count+\'</div><div class="sl">涨停</div><div class="sb">昨日\'+pu+pdate+\' \'+dt(ud)+\'</div></div><div class="si"><div class="sv \'+(ls.down_count>0?\'dn\':\'\')+\'">\'+ls.down_count+\'</div><div class="sl">跌停</div><div class="sb">昨日\'+pd+pdate+\' \'+dt(dd)+\'</div></div><div class="si"><div class="sv" style="color:#d2991d;">\'+br+\'</div><div class="sl">炸板</div><div class="sb">炸板率\'+brr+\'%</div></div><div class="si"><div class="sv" style="color:#58a6ff;">\'+arT+\'</div><div class="sl">涨停晋级率</div><div class="sb">\'+arS+\'</div></div><div class="si"><div class="sv \'+avC+\'">\'+avT+\'</div><div class="sl">昨日涨停今日均值</div><div class="sb">\'+avSrc+\'</div></div>\';}})()+
 '</div></div>'+
 '<div class="g2" style="align-items:start;">'+
 ' <div style="display:flex;flex-direction:column;gap:6px;">'+
 (function(){{var boards=D.consecutive_boards||{{}};var bds=Object.keys(boards).map(Number).sort(function(a,b){{return b-a;}});if(bds.length===0)return\'<div class="card"><div class="ch"><span class="hi">连板</span><span class="badge">0只</span></div><div class="em">今日无连板</div></div>\';return bds.map(function(bd){{var lst=boards[bd]||[];return\'<div class="card"><div class="ch"><span class="hi">\'+bd+\'连板</span><span class="badge">\'+lst.length+\'只</span></div><div class="bs">\'+(lst.length===0?\'<div class="em">无</div>\':\'<table><thead><tr><th>股票</th><th>涨幅</th><th>成交额</th><th>封板</th><th>开板</th><th>封单</th><th>题材</th></tr></thead><tbody>\'+lst.map(function(s){{return\'<tr><td><span class="sn">\'+E(s.name)+\'</span><span class="sc">\'+s.code+\'</span></td><td class="up">\'+P(s.change_pct)+\'</td><td>\'+A(s.amount_yi)+\'</td><td class="tc">\'+(s.last_block_time||\'—\')+\'</td><td>\'+(s.open_count||0)+\'</td><td>\'+(s.sealed_amount?s.sealed_amount+\'亿\':\'—\')+\'</td><td>\'+(s.reason?\'<span class="rt">\'+E(s.reason)+\'</span>\':\'—\')+\'</td></tr>\';}}).join(\'\')+\'</tbody></table>\')+\'</div></div>\';}}).join(\'\');}})()+
	 (function(){{var th=D.theme_boards||[];var bdLabel=function(d){{return d>=2?d+\'连板\':\'首板\';}};var cols=[];for(var i=0;i<3;i++){{if(i<th.length){{var t=th[i];var rows=t.stocks.slice(0,15).map(function(s){{return\'<tr><td><span class="sn">\'+E(s.name)+\'</span><span class="sc">\'+s.code+\'</span></td><td class="hi">\'+bdLabel(s.board_days)+\'</td><td class="tc">\'+(s.limit_time||\'—\')+\'</td></tr>\';}}).join(\'\');cols.push(\'<div class="card"><div class="ch"><span style="color:#d2991d;font-weight:700;font-size:11px;">\'+E(t.theme)+\'</span><span class="badge">\'+t.count+\'只</span></div><div class="bs" style="max-height:none;overflow:visible;"><table style="table-layout:fixed;width:100%;"><thead><tr><th style="width:45%;">股票</th><th style="width:20%;">板数</th><th style="width:35%;">涨停时间</th></tr></thead><tbody>\'+rows+\'</tbody></table></div></div>\');}}else{{cols.push(\'<div class="card"><div class="ch"><span style="color:var(--t2);">--</span></div><div class="em">暂无数据</div></div>\');}}}}return\'<div class="card"><div class="ch"><span style="color:#d2991d;font-weight:700;">热门题材</span><span class="badge">\'+th.length+\'个</span></div><div style="display:grid;grid-template-columns:repeat(3,1fr);gap:6px;padding:6px;">\'+cols.join(\'\')+\'</div></div>\';}})()+
 ' </div>'+
 ' <div class="card"><div class="ch"><span style="color:var(--ac);font-weight:700;">首板</span><span class="badge">\'+(D.first_limit_up||[]).length+\'只</span></div><div class="bs">\'+((D.first_limit_up||[]).length===0?\'<div class="em">今日无首板</div>\':\'<table><thead><tr><th>股票</th><th>涨幅</th><th>成交额</th><th>封板</th><th>开板</th><th>封单</th><th>题材</th></tr></thead><tbody>\'+D.first_limit_up.map(function(s){{return\'<tr><td><span class="sn">\'+E(s.name)+\'</span><span class="sc">\'+s.code+\'</span></td><td class="up">\'+P(s.change_pct)+\'</td><td>\'+A(s.amount_yi)+\'</td><td class="tc">\'+(s.last_block_time||\'—\')+\'</td><td>\'+(s.open_count||0)+\'</td><td>\'+(s.sealed_amount?s.sealed_amount+\'亿\':\'—\')+\'</td><td>\'+(s.reason?\'<span class="rt">\'+E(s.reason)+\'</span>\':\'—\')+\'</td></tr>\';}}).join(\'\')+\'</tbody></table>\')+\'</div></div>\'+
 '</div>'+
 '<div class="g3" style="align-items:start;">'+
 (function(){{var db=D.dragon_tiger_buy||[];if(db.length===0)return\'<div class="card"><div class="ch"><span style="color:var(--rd);font-weight:700;">机构净买</span><span class="badge">>1亿</span></div><div class="em">今日无机构净买入>1亿</div></div>\';return\'<div class="card"><div class="ch"><span style="color:var(--rd);font-weight:700;">机构净买</span><span class="badge">>1亿 \'+db.length+\'只</span></div><div class="bs"><table><thead><tr><th>股票</th><th>涨幅</th><th>净买额</th><th>占比</th><th>题材</th></tr></thead><tbody>\'+db.map(function(s){{return\'<tr><td><span class="sn">\'+E(s.name)+\'</span><span class="sc">\'+s.code+\'</span></td><td class="\'+C(s.change_pct)+\'">\'+P(s.change_pct)+\'</td><td class="up">+\'+s.inst_net_yi.toFixed(2)+\'亿\'+(s.is_3day?\'<span style=\"font-size:9px;color:#d2991d;margin-left:4px;\">3日</span>\':\'\')+\'</td><td>\'+s.inst_net_ratio+\'%</td><td>\'+(s.reason?\'<span class="rt">\'+E(s.reason)+\'</span>\':\'—\')+\'</td></tr>\';}}).join(\'\')+\'</tbody></table></div></div>\';}})()+
 (function(){{var ds=D.dragon_tiger_sell||[];if(ds.length===0)return\'<div class="card"><div class="ch"><span style="color:var(--gn);font-weight:700;">机构净卖</span><span class="badge">>8000万</span></div><div class="em">今日无机构净卖出>8000万</div></div>\';return\'<div class="card"><div class="ch"><span style="color:var(--gn);font-weight:700;">机构净卖</span><span class="badge">>8000万 \'+ds.length+\'只</span></div><div class="bs"><table><thead><tr><th>股票</th><th>涨幅</th><th>净卖额</th><th>占比</th><th>题材</th></tr></thead><tbody>\'+ds.map(function(s){{return\'<tr><td><span class="sn">\'+E(s.name)+\'</span><span class="sc">\'+s.code+\'</span></td><td class="\'+C(s.change_pct)+\'">\'+P(s.change_pct)+\'</td><td class="dn">\'+s.inst_net_yi.toFixed(2)+\'亿\'+(s.is_3day?\'<span style=\"font-size:9px;color:#d2991d;margin-left:4px;\">3日</span>\':\'\')+\'</td><td>\'+s.inst_net_ratio+\'%</td><td>\'+(s.reason?\'<span class="rt">\'+E(s.reason)+\'</span>\':\'—\')+\'</td></tr>\';}}).join(\'\')+\'</tbody></table></div></div>\';}})()+
 (function(){{var ab=D.abnormal_volatility||[];if(ab.length===0)return\'<div class="card"><div class="ch"><span style="color:#d2991d;font-weight:700;">严重异常波动</span><span class="badge">向上·期间内</span></div><div class="em">近期无严重异常波动（向上）</div></div>\';return\'<div class="card"><div class="ch"><span style="color:#d2991d;font-weight:700;">严重异常波动</span><span class="badge">向上·期间内 \'+ab.length+\'只</span></div><div class="bs"><table><thead><tr><th>股票</th><th>起始日期</th><th>结束日期</th></tr></thead><tbody>\'+ab.map(function(s){{return\'<tr><td><span class="sn">\'+E(s.name)+\'</span><span class="sc">\'+s.code+\'</span></td><td>\'+s.start_date+\'</td><td>\'+s.end_date+\'</td></tr>\';}}).join(\'\')+\'</tbody></table></div></div>\';}})()+
 '</div>'+
 '</div>';
(function(){{var l=document.querySelector(\'.g2>div:first-child\');var r=document.querySelector(\'.g2>.card:last-child .bs\');if(l&&r){{r.style.maxHeight=l.offsetHeight+\'px\';}}}})();
(function(){{function V(c,r){{var t=((r.children[c]||{{}}).textContent||'').trim();if(/^[0-9]{{1,2}}:[0-9]{{2}}$/.test(t))return parseInt(t.replace(':',''),10);var n=parseFloat(t);if(isNaN(n))return t;return t.includes('万亿')?n*10000:t.includes('亿')?n:n;}}
var T=document.querySelectorAll('.card table');
for(var i=0;i<T.length;i++){{
  var H=T[i].querySelector('thead');
  if(!H)continue;var h=H.querySelectorAll('th');
  for(var j=0;j<h.length;j++)(function(c,tbl){{
    h[c].onclick=function(){{
      var B=tbl.querySelector('tbody');if(!B)return;
      var asc=this._s!='a',R=[].slice.call(B.querySelectorAll('tr'));
      var hds=tbl.querySelectorAll('th');
      for(var k=0;k<hds.length;k++){{hds[k]._s=null;hds[k].textContent=hds[k].textContent.replace(/[▲▼]/g,'').trim();}}
      this._s=asc?'a':'d';
      R.sort(function(x,y){{var a=V(c,x),b=V(c,y);
        return typeof a=='number'&&typeof b=='number'?asc?a-b:b-a:String(a).localeCompare(String(b));
      }});
      for(var k=0;k<R.length;k++)B.appendChild(R[k]);
      this.textContent+=' '+(asc?'▲':'▼');
    }};
  }})(j,T[i]);
}};
}})();
</script></body></html>'''
    with open(path, "w", encoding="utf-8") as f: f.write(h)


# ── 指数 ──
IDX = {"1.000001":("000001","上证指数"),"0.399001":("399001","深证成指"),
       "0.399006":("399006","创业板指"),"1.000300":("000300","沪深300"),
       "1.000905":("000905","中证500"),"1.000016":("000016","上证50"),
       "2.930050":("930050","中证A50")}

def fetch_indices():
    """获取指数今日+昨日数据（东财 ulist.np）"""
    secids = ",".join(IDX.keys())
    fields = "f2,f3,f4,f5,f6,f12,f14,f15,f16,f17,f18"
    url = f"http://push2.eastmoney.com/api/qt/ulist.np/get?fltt=2&invt=2&fields={fields}&secids={secids}"
    req = urllib.request.Request(url, headers={
        "User-Agent": UA, "Referer": "http://quote.eastmoney.com/"})
    raw_data = {}
    try:
        r = urllib.request.urlopen(req, timeout=15)
        d = json.loads(r.read().decode())
        for item in d.get("data", {}).get("diff", []):
            code = item.get("f12", "")
            if code:
                raw_data[code] = item
    except Exception as e:
        print(f"  [WARN] 东财ulist: {e}")
        return [], {}

    # 昨日成交额: index_history → 历史JSON
    idx_prev_amt = {}
    idx_hist_file = DATA_DIR / "index_history.json"
    idx_history = {}
    if idx_hist_file.exists():
        try:
            with open(idx_hist_file, encoding="utf-8") as f:
                idx_history = json.load(f)
        except: pass

    if not idx_prev_amt:
        prev_td = last_trading_day(datetime.now().strftime("%Y-%m-%d"))
        pf_target = DATA_DIR / f"{prev_td}.json"
        for pf in [pf_target] + [DATA_DIR / f"{(datetime.strptime(prev_td,'%Y-%m-%d')-timedelta(days=i)).strftime('%Y-%m-%d')}.json" for i in range(1, 6)]:
            if pf.exists():
                try:
                    with open(pf, encoding="utf-8") as f:
                        old = json.load(f)
                    for ix in old.get("indices", []):
                        idx_prev_amt[ix["code"]] = ix.get("amount_yi")
                    if idx_prev_amt:
                        break
                except: pass

    results = []
    today_str = datetime.now().strftime("%Y-%m-%d")
    today_amounts = {}
    for secid, (code, name) in IDX.items():
        item = raw_data.get(code, {})
        prev_amt = idx_prev_amt.get(code)
        if prev_amt is None and idx_history:
            dates = sorted(idx_history.keys(), reverse=True)
            for dt in dates:
                if dt == today_str: continue  # 跳过今天
                if code in idx_history[dt]:
                    prev_amt = idx_history[dt][code]
                    break
        price = item.get("f2", 0) or 0
        prev_close = item.get("f18", 0) or 0
        change_amt = item.get("f4", 0) or 0
        change_pct = item.get("f3", 0) or 0
        amount_yi = round((item.get("f6", 0) or 0) / 1e8, 2)
        results.append({"code": code, "name": name,
                        "price": price,
                        "prev_close": prev_close,
                        "change_amt": change_amt,
                        "change_pct": change_pct,
                        "amount_yi": amount_yi,
                        "prev_amount_yi": prev_amt})
        today_amounts[code] = amount_yi

    return results, today_amounts



# ── 东方财富成交额TOP20 ──
def fetch_em_top20():
    """东方财富clist API - 全A股按成交额排序TOP20"""
    url = ("https://push2.eastmoney.com/api/qt/clist/get"
           "?pn=1&pz=20&po=1&fid=f6"
           "&fs=m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23"
           "&fields=f2,f3,f4,f5,f6,f12,f14,f15,f16,f17,f18,f20"
           "&fltt=2&invt=2")
    req = urllib.request.Request(url, headers={
        "User-Agent": UA, "Referer": "https://quote.eastmoney.com/"})
    try:
        r = urllib.request.urlopen(req, timeout=15)
        d = json.loads(r.read().decode())
        items = d.get("data", {}).get("diff", [])
        result = []
        for i, item in enumerate(items):
            code = str(item.get("f12", "")).zfill(6)
            name = item.get("f14", "")
            if not code: continue
            amt = item.get("f6", 0) or 0  # 成交额(元)
            result.append({
                "rank": i + 1,
                "code": code, "name": name,
                "price": item.get("f2", 0) or 0,
                "change_pct": item.get("f3", 0) or 0,
                "amount_yi": round(amt / 1e8, 2),
                "turnover_ratio": item.get("f8", 0) or 0 if hasattr(item,'f8') else 0,
                "mcap_yi": round((item.get("f20", 0) or 0) / 1e8, 2),
            })
        print(f"  东财TOP20: {len(result)}只")
        return result
    except Exception as e:
        print(f"  [WARN] 东财TOP20: {e}")
        return []
# ── 主流程 ──
# ── 腾讯云COS上传 ──
def upload_to_cos(local_path):
    """上传文件到腾讯云COS，返回公网URL"""
    cfg_file = Path(__file__).parent / "cos_config.json"
    if not cfg_file.exists():
        print("  [WARN] cos_config.json不存在，跳过上传")
        return None
    with open(cfg_file, encoding="utf-8") as f:
        cfg = json.load(f)
    bucket = cfg["bucket"]
    region = cfg["region"]
    secret_id = cfg["secret_id"]
    secret_key = cfg["secret_key"]
    key_name = os.path.basename(local_path)
    host = f"{bucket}.cos.{region}.myqcloud.com"
    url = f"https://{host}/{key_name}"

    # 读取文件
    with open(local_path, "rb") as f:
        body = f.read()

    # COS签名
    now = datetime.now(timezone.utc)
    sign_time = f"{int(now.timestamp())-60};{int(now.timestamp())+3600}"
    http_method = "put"
    uri = f"/{key_name}"
    content_type = "text/html; charset=utf-8"
    # 按字典序排列header: host -> content-type
    http_headers = f"content-type={content_type}&host={host}"
    sha1 = hashlib.sha1(body).hexdigest()
    http_string = f"{http_method}\n{uri}\n\n{http_headers}\n"
    string_to_sign = f"sha1\n{sign_time}\n{hashlib.sha1(http_string.encode()).hexdigest()}\n"
    sign_key = hmac.new(secret_key.encode(), sign_time.encode(), hashlib.sha1).hexdigest()
    signature = hmac.new(sign_key.encode(), string_to_sign.encode(), hashlib.sha1).hexdigest()
    auth = (f"q-sign-algorithm=sha1&q-ak={secret_id}&q-sign-time={sign_time}"
            f"&q-key-time={sign_time}&q-header-list=content-type;host&q-url-param-list="
            f"&q-signature={signature}")

    req = urllib.request.Request(url, data=body, method="PUT")
    req.add_header("Host", host)
    req.add_header("Content-Type", content_type)
    req.add_header("Authorization", auth)
    try:
        urllib.request.urlopen(req, timeout=30)
        return url
    except Exception as e:
        print(f"  [WARN] COS上传失败: {e}")
        return None


def main(target_date=None):
    today = target_date or date.today().strftime("%Y-%m-%d")
    prev_trade_day = last_trading_day(today)
    print(f"{'='*50}\n  A股收盘 — {today} (上一交易日: {prev_trade_day})\n{'='*50}")

    print("\n[1/5] 指数...")
    indices, idx_today_amt = fetch_indices()
    for i in indices:
        a = "UP" if i["change_pct"]>0 else ("DN" if i["change_pct"]<0 else "--")
        print(f"  [{a}] {i['name']}: {i['price']:.2f} {i['change_pct']:+.2f}% 成交{i['amount_yi']:.0f}亿")

    print("\n[2/5] DDC全市场扫描...")
    all_stocks = fetch_all()
    enrich_names(all_stocks)
    enrich_amounts_tx(all_stocks)

    print(f"\n[3/5] 选股通Flash涨停/跌停池...")
    limit_up_pool = fetch_flash_pool("limit_up", today)
    limit_down_pool = fetch_flash_pool("limit_down", today)
    print(f"  涨停池:{len(limit_up_pool)} 跌停池:{len(limit_down_pool)}")

    # 东方财富封单补充
    em_sealed = fetch_em_sealed_amount(today)
    for s in limit_up_pool:
        if s["sealed_amount"] == 0 and s["code"] in em_sealed:
            s["sealed_amount"] = em_sealed[s["code"]]

    # 炸板: DDC数据计算 (触及涨停但未在涨停池中)
    up_codes = {s["code"] for s in limit_up_pool}
    board_breaks = compute_board_break(all_stocks, up_codes)
    # 过滤ST
    board_breaks = [s for s in board_breaks if not is_bad(s.get("name",""))]
    print(f"  炸板(DDC计算):{len(board_breaks)}")

    # 补充涨停股的成交额(从DDC全市场数据)
    ddc_map = {s["code"]: s for s in all_stocks}
    for s in limit_up_pool:
        d = ddc_map.get(s["code"], {})
        if d:
            s["amount_yi"] = d.get("amount_yi", 0)
            s["turnover_ratio"] = d.get("turnover_ratio", 0) or s.get("turnover_ratio", 0)

    # 连板分组(使用Flash API的limit_up_days)
    boards = {}
    for s in limit_up_pool:
        bd = s.get("board_days", 1)
        if bd >= 2:
            boards.setdefault(bd, []).append(s)
    for bd in boards:
        boards[bd].sort(key=lambda x: -x.get("sealed_amount", 0))
    first_up = [s for s in limit_up_pool if s.get("board_days", 1) == 1]
    first_up.sort(key=lambda x: -x.get("sealed_amount", 0))

    total_board = sum(len(v) for v in boards.values())
    bd_list = sorted(boards.keys(), reverse=True)
    print(f"\n[4/5] 连板:{total_board}只({len(boards)}档) 首板:{len(first_up)}")
    for bd in bd_list[:5]:
        names = ",".join(f"{s['name']}({s['code']})" for s in boards[bd][:3])
        print(f"    {bd}连板({len(boards[bd])}只): {names}")

    # 题材归类
    theme_boards = compute_theme_boards(limit_up_pool)
    if theme_boards:
        print(f"  题材归类: {len(theme_boards)}个题材")
        for tb in theme_boards:
            top3 = ",".join(f"{s['name']}({s['board_days']}板)" for s in tb["stocks"][:3])
            print(f"    {tb['theme']}({tb['count']}只): {top3}")

    print("\n[5/5] TOP20(腾讯成交额)...")
    sorted_amt = sorted(all_stocks, key=lambda x: -x["amount_yi"])
    top20 = [{"rank":i+1,"code":s["code"],"name":s["name"],"price":s["price"],
              "change_pct":s["change_pct"],"amount_yi":s["amount_yi"],
              "turnover_ratio":s["turnover_ratio"],"mcap_yi":s["mcap_yi"]}
             for i,s in enumerate(sorted_amt[:20])]
    for s in top20[:10]:
        a = "UP" if s["change_pct"]>0 else ("DN" if s["change_pct"]<0 else "--")
        print(f"  {s['rank']:>2}. [{a}] {s['name']}({s['code']}): {s['amount_yi']:.1f}亿 {s['change_pct']:+.2f}%")

    # 昨日涨停数据: 从选股通Flash API获取上一交易日
    yesterday_up = fetch_flash_pool("limit_up", prev_trade_day)
    prev_up_total = len(yesterday_up) if yesterday_up else None
    prev_up = prev_up_total
    # 跌停也从Flash获取昨日数据
    yesterday_dn = fetch_flash_pool("limit_down", prev_trade_day)
    prev_down = len(yesterday_dn) if yesterday_dn else None
    prev_date = prev_trade_day

    # 晋级率 = 今日连板总数/昨日涨停总数
    adv_rate = None; prev_up_avg = None; adv_cnt = None
    if prev_up_total and prev_up_total > 0:
        adv_rate = round(total_board / prev_up_total * 100, 1)
        adv_cnt = total_board
    if prev_up_total:
        print(f"  昨日涨停(选股通):{prev_up_total}只", end="")
    if adv_rate is not None:
        print(f" 晋级率:{adv_rate}% ({adv_cnt}/{prev_up_total})", end="")

    # 昨日涨停板块 BK0815
    zt_board = fetch_yesterday_zt_board()
    if zt_board:
        prev_up_avg = zt_board["change_pct"]
        print(f" 昨日涨停(BK0815):{prev_up_avg:+.2f}%")
    elif yesterday_up:
        # Fallback: 手动计算昨日涨停今日涨跌幅
        gains = []
        ddc_chg = {s["code"]: s["change_pct"] for s in all_stocks}
        for ps in yesterday_up:
            tchg = ddc_chg.get(ps["code"])
            if tchg is not None: gains.append(tchg)
        if gains:
            prev_up_avg = round(sum(gains) / len(gains), 2)
        print(f" 昨日涨停(手动):{prev_up_avg:+.2f}%")

    # 保存指数历史
    idx_hist_file = DATA_DIR / "index_history.json"
    idx_history = {}
    if idx_hist_file.exists():
        try:
            with open(idx_hist_file, encoding="utf-8") as f:
                idx_history = json.load(f)
        except: pass
    idx_history[today] = idx_today_amt
    dates = sorted(idx_history.keys(), reverse=True)
    for old_d in dates[60:]:
        del idx_history[old_d]
    with open(idx_hist_file, "w", encoding="utf-8") as f:
        json.dump(idx_history, f, ensure_ascii=False)

    br_count = len(board_breaks)
    total_touched = len(limit_up_pool) + br_count
    br_rate = round(br_count / total_touched * 100, 1) if total_touched > 0 else 0

    # 龙虎榜 + 严重异常波动
    print("\n[6/6] 龙虎榜+异常波动...")
    # 题材映射: 当日涨停 + 昨日涨停 + 东财概念
    reason_map = {s["code"]: s.get("reason", "") for s in limit_up_pool}
    for s in (yesterday_up or []):
        c = s.get("code", "")
        if c and c not in reason_map:
            reason_map[c] = s.get("reason", "")
    # 东财概念补充: 对净买净卖中没有题材的股票查概念
    def _enrich_concept(code):
        try:
            m = "1" if code.startswith("6") else "0"
            url_c = (f"http://push2.eastmoney.com/api/qt/ulist.np/get"
                     f"?fltt=2&invt=2&fields=f14,f100&secids={m}.{code}")
            req_c = urllib.request.Request(url_c, headers={
                "User-Agent": UA, "Referer": "http://quote.eastmoney.com/"})
            r_c = urllib.request.urlopen(req_c, timeout=8)
            d_c = json.loads(r_c.read().decode())
            items = d_c.get("data", {}).get("diff", [])
            if items:
                concept = items[0].get("f100", "")
                return str(concept) if concept else ""
        except:
            pass
        return ""

    dt_buy, dt_sell = fetch_dragon_tiger_inst(today, reason_map)
    # 补充题材: 对没找到题材的股票查东财概念
    for entry in dt_buy + dt_sell:
        if not entry.get("reason"):
            entry["reason"] = _enrich_concept(entry["code"])
    print(f"  机构净买>1亿: {len(dt_buy)}只")
    for s in dt_buy:
        print(f"    {s['name']}({s['code']}): 涨幅{s['change_pct']:+.2f}% 净买{s['inst_net_yi']:.2f}亿 占比{s['inst_net_ratio']}%")
    print(f"  机构净卖>8000万: {len(dt_sell)}只")
    for s in dt_sell:
        print(f"    {s['name']}({s['code']}): 涨幅{s['change_pct']:+.2f}% 净卖{s['inst_net_yi']:.2f}亿")

    abnormal = fetch_abnormal_volatility(today)
    print(f"  严重异常波动(向上/期间内): {len(abnormal)}只")
    for s in abnormal[:3]:
        print(f"    {s.get('name','')}({s.get('code','')}): {s.get('start_date','')} ~ {s.get('end_date','')}")

    result = {"date":today,"update_time":datetime.now().strftime("%H:%M:%S"),
              "prev_trade_date": prev_trade_day,
              "indices":indices,"top20_turnover":top20,
              "limit_stats":{"up_count":len(limit_up_pool),"down_count":len(limit_down_pool),
                             "break_count":br_count,"break_rate":br_rate,
                             "prev_up_count":prev_up,"prev_down_count":prev_down,"prev_date":prev_date,
                             "advance_rate":adv_rate,"prev_up_avg_change":prev_up_avg,
                             "prev_up_total":prev_up_total,"advance_count":adv_cnt},
              "limit_up_stocks":limit_up_pool,"limit_down_stocks":limit_down_pool,
              "board_break_stocks":board_breaks,
              "consecutive_boards":boards,"first_limit_up":first_up,
              "theme_boards":theme_boards,
              "dragon_tiger_buy":dt_buy,
              "dragon_tiger_sell":dt_sell,
              "abnormal_volatility":abnormal,
              "zt_board":zt_board}

    jf = DATA_DIR / f"{today}.json"
    with open(jf,"w",encoding="utf-8") as f:
        json.dump(result,f,ensure_ascii=False,indent=2)
    hf_date = Path(__file__).parent / f"dashboard_{today}.html"
    gen_html(result, hf_date)
    # index.html（静态网站默认入口）
    hf_index = Path(__file__).parent / "index.html"
    gen_html(result, hf_index)
    # Git提交推送
    try:
        import subprocess
        subprocess.run(["git", "add", "index.html", f"dashboard_{today}.html"],
                       cwd=str(Path(__file__).parent), capture_output=True)
        subprocess.run(["git", "commit", "-m", f"update {today}"],
                       cwd=str(Path(__file__).parent), capture_output=True)
        subprocess.run(["git", "push", "github", "master:main"], cwd=str(Path(__file__).parent),
                       capture_output=True, timeout=120)
        print(f"  🚀 GitHub: https://a-stock-dashboard.github.io/")
    except Exception as e:
        print(f"  [WARN] Git推送失败: {e}")

    # 生成PDF
    pf = Path(__file__).parent / "dashboard.pdf"
    try:
        import subprocess, os as _os
        edge = (r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"
                if _os.path.exists(r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe")
                else r"C:\Program Files\Microsoft\Edge\Application\msedge.exe")
        if _os.path.exists(edge):
            subprocess.run(
                [edge, "--headless=new", "--disable-gpu",
                 f"--print-to-pdf={pf.resolve()}",
                 f"file:///{hf_date.resolve().as_posix()}"],
                timeout=30, check=True, capture_output=True)
            print(f"  PDF → {pf}")
        else:
            print("  [WARN] 未找到Edge，跳过PDF")
    except Exception as e:
        print(f"  [WARN] PDF失败: {e}")
    print(f"\n{'='*50}\n  JSON → {jf}\n  HTML → {hf_date}\n{'='*50}")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv)>1 else None)
