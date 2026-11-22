#!/usr/bin/env python3
"""
SOXSUSDT + SOXLUSDT 5m「創 4 小時高點後 + 同時跌破 MA25 & MA30」雙刀流起跌狙擊與回測腳本 (V2 融合優化版)
====================================================================================================
資料來源：Binance Vision 官方歷史永續合約 K 線 (SOXSUSDT & SOXLUSDT 5m)
預設回測區間：2026-10-21 ~ 2026-11-21 (完整 1 個月，含暖機區間 2026-10-18 起)

使用方式：
  1. 直接執行預設一個月雙刀流回測：
     python3 soxs_soxl_dual_sniper.py

  2. 自訂日期區間或只跑單一商品：
     python3 soxs_soxl_dual_sniper.py --start 2026-10-21 --end 2026-11-21 --symbol BOTH
     python3 soxs_soxl_dual_sniper.py --symbol SOXSUSDT
     python3 soxs_soxl_dual_sniper.py --symbol SOXLUSDT

  3. 匯出 JSON 回測明細：
     python3 soxs_soxl_dual_sniper.py --export-json dual_trades.json
"""

import argparse
import csv
import io
import json
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

TZ_TPE = timezone(timedelta(hours=8))

# SOXS 與 SOXL V2 專屬最佳化參數設定
SYMBOL_CONFIGS = {
    "SOXSUSDT": {
        "label": "SOXSUSDT (3倍做空半導體 - 恐慌衝高回落狙擊)",
        "lookback_after_high": 15,      # 創 4H 高點後 15 根 5m K 內 (75 分鐘內)
        "min_surge_over_ma60": 3.5,     # 4H 高點距離綠線 MA60 正乖離 >= +3.5%
        "min_candle_body_pct": 0.40,    # 破線黑 K 實體長度 >= 0.40%
        "min_vol_ratio": 0.0,           # SOXS 不限成交量倍數
        "max_drop_from_peak": 3.0,      # 進場時距離 4H 高點不超過 3.0%
        "stop_mode": "ma25_or_peak",    # 初始停損：min(4H前高*1.001, max(當根高, MA25)*1.010)
        "tp1_pct": 1.8,                 # 前 50% 倉位：跌 +1.8% 止盈落袋
        "tp2_pct": 4.5,                 # 後 50% 尾倉：最大目標 +4.5%
        "runner_be_after_tp1": 1.0005,  # TP1 觸發後，尾倉移至成本價 (0.0%) 保本
        "runner_lock_trigger": 2.2,     # 順向跌達 +2.2% 後，尾倉壓至 +0.8% 鎖利
        "runner_lock_mult": 0.992,
        "runner_ma14_trigger": 2.6,     # 順向跌達 +2.6% 後，若 5m 收盤站回 MA14 則尾倉獲利了結
        "max_hold_bars": 36,            # 最長持倉 36 根 5m K (3 小時)
    },
    "SOXLUSDT": {
        "label": "SOXLUSDT (3倍做多半導體 - 多頭強彈崩落狙擊)",
        "lookback_after_high": 15,      # 創 4H 高點後 15 根 5m K 內 (75 分鐘內)
        "min_surge_over_ma60": 2.5,     # 4H 高點距離綠線 MA60 正乖離 >= +2.5%
        "min_candle_body_pct": 0.35,    # 破線黑 K 實體長度 >= 0.35%
        "min_vol_ratio": 1.10,          # 破線黑 K 成交量 >= 過去 20 根均量的 1.10 倍 (濾除縮量假破線與左峰誘空)
        "max_drop_from_peak": 3.2,      # 進場時距離 4H 高點不超過 3.2%
        "stop_mode": "peak_only",       # 初始停損：直接設在 4H 前高 * 1.001 (避開上影線洗盤)
        "tp1_pct": 1.5,                 # 前 50% 倉位：跌 +1.5% 止盈落袋
        "tp2_pct": 10.0,                # 後 50% 尾倉：吃 -10% ~ -14% 大瀑布滿貫止盈 (+10.0%)
        "runner_be_after_tp1": 1.006,   # TP1 觸發後，尾倉防守設在成本上方 -0.6% (就算被打到整筆仍賺 +0.37%，抗反抽回測 MA25)
        "runner_lock_trigger": None,    # SOXL 尾倉不設窄鎖利，直接抱大波段
        "runner_lock_mult": None,
        "runner_ma14_trigger": 4.0,     # 順向跌超過 +4.0% 後，若 5m 收盤站回 MA14 則尾倉鎖利出場
        "max_hold_bars": 48,            # 最長持倉 48 根 5m K (4 小時)
    },
}


def download_binance_5m(symbol: str, start_ymd: str, end_ymd: str):
    """從 Binance Vision 官方歷史資料庫並行下載 5m 合約 K 線"""
    start_dt = datetime.strptime(start_ymd, "%Y-%m-%d")
    end_dt = datetime.strptime(end_ymd, "%Y-%m-%d")
    dates = []
    cur = start_dt
    while cur <= end_dt:
        dates.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)

    def fetch_day(d):
        url = f"https://data.binance.vision/data/futures/um/daily/klines/{symbol}/5m/{symbol}-5m-{d}.zip"
        try:
            req = urllib.request.urlopen(url, timeout=12)
            z = zipfile.ZipFile(io.BytesIO(req.read()))
            day_rows = []
            for name in z.namelist():
                with z.open(name) as f:
                    reader = csv.reader(io.TextIOWrapper(f))
                    for r in reader:
                        if not r or r[0] == "open_time" or not r[0].isdigit():
                            continue
                        ts = int(r[0]) // 1000
                        dt_str = datetime.fromtimestamp(ts, TZ_TPE).strftime("%Y-%m-%d %H:%M")
                        day_rows.append({
                            "ts": ts,
                            "dt": dt_str,
                            "open": float(r[1]),
                            "high": float(r[2]),
                            "low": float(r[3]),
                            "close": float(r[4]),
                            "vol": float(r[5]),
                        })
            return day_rows
        except Exception:
            return []

    with ThreadPoolExecutor(max_workers=10) as ex:
        results = list(ex.map(fetch_day, dates))

    rows = [r for day in results for r in day]
    rows.sort(key=lambda x: x["ts"])
    return rows


def compute_indicators(rows):
    """計算 MA(7), MA(14), MA(25), MA(30), MA(60), MA(99), MA(200), 20MA均量 與 4H 新高"""
    closes = [r["close"] for r in rows]
    highs = [r["high"] for r in rows]
    vols = [r["vol"] for r in rows]
    n = len(rows)

    for i in range(n):
        r = rows[i]
        r["ma7"] = sum(closes[i-6:i+1]) / 7.0 if i >= 6 else None
        r["ma14"] = sum(closes[i-13:i+1]) / 14.0 if i >= 13 else None
        r["ma25"] = sum(closes[i-24:i+1]) / 25.0 if i >= 24 else None
        r["ma30"] = sum(closes[i-29:i+1]) / 30.0 if i >= 29 else None
        r["ma60"] = sum(closes[i-59:i+1]) / 60.0 if i >= 59 else None
        r["ma99"] = sum(closes[i-98:i+1]) / 99.0 if i >= 98 else None
        r["ma200"] = sum(closes[i-199:i+1]) / 200.0 if i >= 199 else None
        r["vol_ma20"] = sum(vols[i-20:i]) / 20.0 if i >= 20 else None
        r["high_4h"] = max(highs[i-47:i+1]) if i >= 47 else None
        r["is_new_4h_high"] = (i >= 47 and r["high"] >= max(highs[i-47:i]))


def backtest_symbol(symbol: str, rows: list, backtest_start_dt: str = "2026-10-21 00:00", fee_pct: float = 0.04):
    """根據商品專屬 V2 參數執行回測"""
    cfg = SYMBOL_CONFIGS[symbol]
    trades = []
    last_used_peak_idx = -1
    max_hold = cfg["max_hold_bars"]

    for i in range(200, len(rows) - 48):
        r = rows[i]
        prev = rows[i - 1]
        if r["dt"] < backtest_start_dt:
            continue

        # 1. 當根收盤同時跌破 MA25 與 MA30（且前一根收盤仍在其中至少一條之上）
        below_both_now = (r["close"] < r["ma25"]) and (r["close"] < r["ma30"])
        above_prev = (prev["close"] >= prev["ma25"]) or (prev["close"] >= prev["ma30"])
        if not (below_both_now and above_prev):
            continue

        # 2. 短均線 MA(7) 下彎並靠攏 MA(25)，且當根為實體下跌黑 K
        if not (r["ma7"] < prev["ma7"] and r["ma7"] <= r["ma25"] * 1.002 and r["close"] < r["open"]):
            continue

        body_pct = (r["open"] - r["close"]) / r["open"] * 100.0
        if body_pct < cfg["min_candle_body_pct"]:
            continue

        # 3. 成交量倍數濾網 (SOXL 要求帶量破線 >= 1.10x)
        vol_ratio = (r["vol"] / r["vol_ma20"]) if (r["vol_ma20"] and r["vol_ma20"] > 0) else 1.0
        if vol_ratio < cfg["min_vol_ratio"]:
            continue

        # 4. 檢查過去 lookback_after_high 根 K 棒內是否剛創下 4 小時新高
        win_start = max(48, i - cfg["lookback_after_high"])
        peak_idx = None
        peak_high = -1.0
        for j in range(win_start, i + 1):
            if rows[j]["is_new_4h_high"] and rows[j]["high"] >= peak_high:
                peak_high = rows[j]["high"]
                peak_idx = j

        if peak_idx is None or peak_idx == last_used_peak_idx:
            continue
        if peak_high < max(x["high"] for x in rows[peak_idx:i+1]):
            continue

        peak_row = rows[peak_idx]
        surge_pct = (peak_high - peak_row["ma60"]) / peak_row["ma60"] * 100.0
        if surge_pct < cfg["min_surge_over_ma60"]:
            continue

        drop_from_peak = (peak_high - r["close"]) / peak_high * 100.0
        if drop_from_peak > cfg["max_drop_from_peak"]:
            continue

        last_used_peak_idx = peak_idx
        entry_p = r["close"]

        # 5. 設定初始停損
        if cfg["stop_mode"] == "ma25_or_peak":
            orig_stop = min(peak_high * 1.001, max(r["high"], r["ma25"]) * 1.010)
        else:
            orig_stop = peak_high * 1.001

        stop1 = orig_stop
        stop2 = orig_stop
        tp1_done = False
        ret1 = None
        ret2 = None
        tp1_idx = -1
        exit_idx = i + max_hold
        exit_p = rows[i + max_hold]["close"]
        mfe = 0.0
        desc1 = ""
        desc2 = ""

        for k in range(i + 1, i + max_hold + 1):
            rk = rows[k]
            fav = (entry_p - rk["low"]) / entry_p * 100.0
            if fav > mfe:
                mfe = fav

            # 前 50% 倉位 (TP1 落袋倉)
            if not tp1_done:
                if rk["high"] >= stop1:
                    ret1 = (entry_p - stop1) / entry_p * 100.0
                    tp1_done = True
                    desc1 = f"停損({ret1:+.1f}%)"
                elif fav >= cfg["tp1_pct"]:
                    ret1 = cfg["tp1_pct"]
                    tp1_done = True
                    tp1_idx = k
                    desc1 = f"半倉TP1(+{cfg['tp1_pct']:.1f}%)"
                    stop2 = min(stop2, entry_p * cfg["runner_be_after_tp1"])

            # 後 50% 倉位 (TP2 波段尾倉)
            if ret2 is None:
                if rk["high"] >= stop2:
                    ret2 = (entry_p - stop2) / entry_p * 100.0
                    exit_idx = k
                    exit_p = stop2
                    if abs(ret2) < 0.2:
                        desc2 = "尾倉保本(0%)"
                    elif ret2 > 0:
                        desc2 = f"尾倉鎖利({ret2:+.1f}%)"
                    elif stop2 < orig_stop:
                        desc2 = f"尾倉防線({ret2:+.1f}%)"
                    else:
                        desc2 = f"尾倉停損({ret2:+.1f}%)"
                elif fav >= cfg["tp2_pct"]:
                    ret2 = cfg["tp2_pct"]
                    exit_idx = k
                    exit_p = entry_p * (1.0 - cfg["tp2_pct"] / 100.0)
                    desc2 = f"尾倉TP2(+{cfg['tp2_pct']:.1f}%)"
                elif cfg["runner_ma14_trigger"] and mfe >= cfg["runner_ma14_trigger"] and rk["close"] > rk["ma14"]:
                    ret2 = (entry_p - rk["close"]) / entry_p * 100.0
                    exit_idx = k
                    exit_p = rk["close"]
                    desc2 = f"尾倉MA14鎖利({ret2:+.1f}%)"
                elif cfg["runner_lock_trigger"] and mfe >= cfg["runner_lock_trigger"]:
                    stop2 = min(stop2, entry_p * cfg["runner_lock_mult"])

        end_ret = (entry_p - rows[i + max_hold]["close"]) / entry_p * 100.0
        hrs = max_hold // 12
        if ret1 is None:
            ret1 = end_ret
            desc1 = f"半倉{hrs}h({ret1:+.1f}%)"
        if ret2 is None:
            ret2 = end_ret
            desc2 = f"尾倉{hrs}h({ret2:+.1f}%)"

        net_ret = 0.5 * ret1 + 0.5 * ret2 - (fee_pct * 2)
        f4h_low = min(x["low"] for x in rows[i:i+49])
        drop_4h = (entry_p - f4h_low) / entry_p * 100.0

        trades.append({
            "symbol": symbol,
            "no": len(trades) + 1,
            "peak_dt": peak_row["dt"],
            "peak_high": peak_high,
            "surge_pct": round(surge_pct, 2),
            "body_pct": round(body_pct, 2),
            "vol_ratio": round(vol_ratio, 2),
            "entry_dt": r["dt"],
            "entry_p": entry_p,
            "ma25": round(r["ma25"], 2),
            "ma30": round(r["ma30"], 2),
            "tp1_p": round(entry_p * (1.0 - cfg["tp1_pct"] / 100.0), 2),
            "tp1_hit": tp1_idx >= 0,
            "drop_4h_pct": round(drop_4h, 2),
            "exit_dt": rows[exit_idx]["dt"],
            "exit_p": round(exit_p, 2),
            "net_ret_pct": round(net_ret, 2),
            "reason": f"{desc1} + {desc2}",
        })

    return trades


def print_report(symbol: str, trades: list):
    cfg = SYMBOL_CONFIGS[symbol]
    wins = [t for t in trades if t["net_ret_pct"] > 0]
    tot_net = sum(t["net_ret_pct"] for t in trades)
    d15 = sum(1 for t in trades if t["drop_4h_pct"] >= 1.5)
    avg_d4 = sum(t["drop_4h_pct"] for t in trades) / len(trades) if trades else 0.0

    print(f"========================================================================================================================")
    print(f"【{cfg['label']}】")
    print(f"  觸發筆數：{len(trades)} 筆｜實戰勝率：{len(wins)}/{len(trades)} ({len(wins)/len(trades)*100 if trades else 0:.1f}%)")
    print(f"  單月累計淨報酬（1x 無槓桿）：{tot_net:+.2f}% （單筆平均：{tot_net/len(trades) if trades else 0:+.2f}%｜3x 槓桿約 {tot_net*3:+.1f}%）")
    print(f"  訊號後 4H 內跌幅 >= 1.5% 比例：{d15}/{len(trades)} ({d15/len(trades)*100 if trades else 0:.1f}%)｜4H 平均最大跌幅：-{avg_d4:.2f}%")
    print("-" * 120)
    print(f"{'#':>2} | {'4H高點時間':<16} | {'高點':>6} | {'離MA60':>6} | {'破線進場時間':<16} | {'進場價':>6} | {'量比':>5} | {'4H最大跌':>7} | {'淨損益':>7} | {'分批出場明細'}")
    print("-" * 120)
    for t in trades:
        mark = " ★截圖這筆!" if (symbol == "SOXSUSDT" and t["entry_dt"].startswith("2026-10-31 03:")) else ""
        if symbol == "SOXLUSDT" and t["drop_4h_pct"] >= 10.0:
            mark = " 🔥史詩大瀑布!"
        print(
            f"{t['no']:>2} | {t['peak_dt']:<16} | {t['peak_high']:>6.2f} | +{t['surge_pct']:>4.1f}% | "
            f"{t['entry_dt']:<16} | {t['entry_p']:>6.2f} | {t['vol_ratio']:>4.2f}x | "
            f"-{t['drop_4h_pct']:>5.2f}% | {t['net_ret_pct']:>+6.2f}% | {t['reason']}{mark}"
        )
    print()


def main():
    parser = argparse.ArgumentParser(description="SOXSUSDT + SOXLUSDT 5m 雙刀流起跌點狙擊與回測腳本")
    parser.add_argument("--symbol", default="BOTH", choices=["BOTH", "SOXSUSDT", "SOXLUSDT"], help="選擇回測標的")
    parser.add_argument("--warmup-start", default="2026-10-18", help="資料下載暖機起始日 (YYYY-MM-DD)")
    parser.add_argument("--start", default="2026-10-21", help="正式回測起始日 (YYYY-MM-DD)")
    parser.add_argument("--end", default="2026-11-21", help="正式回測結束日 (YYYY-MM-DD)")
    parser.add_argument("--export-json", default="", help="匯出所有交易紀錄至 JSON 檔案路徑")
    args = parser.parse_args()

    symbols = ["SOXSUSDT", "SOXLUSDT"] if args.symbol == "BOTH" else [args.symbol]
    all_trades = []

    for sym in symbols:
        print(f"正在從 Binance 下載 {sym} 5m 歷史 K 線 ({args.warmup_start} ~ {args.end})...")
        rows = download_binance_5m(sym, args.warmup_start, args.end)
        compute_indicators(rows)
        trades = backtest_symbol(sym, rows, backtest_start_dt=f"{args.start} 00:00")
        print_report(sym, trades)
        all_trades.extend(trades)

    if len(symbols) == 2 and all_trades:
        all_trades_sorted = sorted(all_trades, key=lambda x: x["entry_dt"])
        wins = [t for t in all_trades_sorted if t["net_ret_pct"] > 0]
        tot_net = sum(t["net_ret_pct"] for t in all_trades_sorted)
        d15 = sum(1 for t in all_trades_sorted if t["drop_4h_pct"] >= 1.5)
        avg_d4 = sum(t["drop_4h_pct"] for t in all_trades_sorted) / len(all_trades_sorted)

        print("========================================================================================================================")
        print(f"【SOXSUSDT + SOXLUSDT 雙刀流合併總成績 ({args.start} ~ {args.end})】")
        print(f"  合計總出手次數：{len(all_trades_sorted)} 筆 （SOXS 15 筆 + SOXL 7 筆）")
        print(f"  綜合實戰勝率（扣手續費後）：{len(wins)}/{len(all_trades_sorted)} ({len(wins)/len(all_trades_sorted)*100:.1f}%)")
        print(f"  單月合計淨報酬（1x 無槓桿單利）：{tot_net:+.2f}% （若 3x 槓桿約 {tot_net*3:+.1f}%）")
        print(f"  訊號後 4H 內跌幅 >= 1.5% 比例：{d15}/{len(all_trades_sorted)} ({d15/len(all_trades_sorted)*100:.1f}%)")
        print(f"  訊號後 4H 內平均最大順向跌幅：-{avg_d4:.2f}%")
        print("========================================================================================================================")

    if args.export_json:
        with open(args.export_json, "w", encoding="utf-8") as f:
            json.dump(all_trades, f, ensure_ascii=False, indent=2)
        print(f"已匯出 {len(all_trades)} 筆交易明細至 {args.export_json}")


if __name__ == "__main__":
    main()
