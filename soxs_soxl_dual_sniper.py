#!/usr/bin/env python3
"""
SOXSUSDT + SOXLUSDT 5m「創 4 小時高點後 + 同時跌破 MA25 & MA30」雙刀流起跌狙擊與回測腳本 (V3 穩健優化版)
========================================================================================================
V3 核心進化（解決 V2 過度擬合、非美股時段假破線、低波動月摸不到固定停利、牛市逆勢空 SOXL 四大痛點）：
  1. 【大趨勢自動切換（3日均線 72H 下彎）】
     - 半導體 3 日偏多 → SOXS 的 3 日均線下彎 → 自動只狙擊 SOXS 恐慌急拉的起跌點
     - 半導體 3 日偏空 → SOXL 的 3 日均線下彎 → 自動只狙擊 SOXL 死貓跳反彈的起跌點
     - 同一套通用參數套用於兩邊，永遠順著 3 日大趨勢吃 3x ETF 耗損與日內均值回歸，絕不逆勢擋火車！
  2. 【嚴格限定週一～週五美股正盤時段（美東 ET 09:30 ~ 16:00）】
     - 自動處理美國夏令 (UTC-4) / 冬令 (UTC-5) 時間切換，徹底避開亞洲盤與週末無量刷洗假破線。
  3. 【全 ATR 動態風控與 50/50 分批止盈（自適應高低波動月份）】
     - 4H 高點距離綠線 MA(60) 必須 >= 4.0 倍 5m ATR(14)（低波動月與高波動月自動縮放門檻）
     - 初始停損：min(4H前高 + 0.2*ATR, 進場價 + 2.5*ATR)
     - 前 50% 倉位：順向跌達 2.5 倍 ATR 止盈落袋，並將尾倉防守移至進場價 + 0.2*ATR 保本
     - 後 50% 尾倉：順向跌達 3.75 倍 ATR 後站回青線 MA(14) 鎖利，或達 6.0 倍 ATR 止盈（最長持倉 4H）

資料來源：Binance Vision 官方歷史永續合約 K 線 (SOXSUSDT & SOXLUSDT 5m)
使用方式：
  1. 執行預設 4 個月全週期 V3 穩健回測 (2026-07-23 ~ 2026-11-21)：
     python3 soxs_soxl_dual_sniper.py

  2. 自訂日期、標的或滑價成本壓力測試：
     python3 soxs_soxl_dual_sniper.py --start 2026-07-23 --end 2026-11-21 --fee-rt 0.08
     python3 soxs_soxl_dual_sniper.py --symbol SOXSUSDT
     python3 soxs_soxl_dual_sniper.py --fee-rt 0.25   # 3倍極端滑價壓力測試

  3. 匯出 JSON 回測明細：
     python3 soxs_soxl_dual_sniper.py --export-json v3_trades.json
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

# V3 統一通用參數（SOXS 與 SOXL 使用完全相同規則，杜絕單一標的過度擬合）
V3_CONFIG = {
    "trend_ma_bars": 864,       # 3 日均線 (72 小時 = 864 根 5m K)
    "trend_slope_bars": 48,     # 與 4 小時前比較斜率是否向下
    "lookback_after_high": 15,  # 創 4H 高點後 15 根 5m K 內 (75 分鐘內)
    "min_surge_atr": 4.0,       # 4H 高點離綠線 MA(60) >= 4.0 倍 ATR(14)
    "sl_atr": 2.5,              # 動態初始停損上限：2.5 倍 ATR
    "peak_stop_buf_atr": 0.2,   # 前高上方緩衝：0.2 倍 ATR
    "tp1_atr": 2.5,             # 前 50% 倉位止盈目標：2.5 倍 ATR
    "tp2_atr": 6.0,             # 後 50% 尾倉止盈目標：6.0 倍 ATR
    "runner_be_buf_atr": 0.2,   # TP1 觸發後，尾倉防守移至進場價 + 0.2 倍 ATR
    "runner_ma14_atr": 3.75,    # 順向跌達 3.75 倍 ATR 後，站回 MA14 移動停利
    "max_hold_bars": 48,        # 最長持倉 48 根 5m K (4 小時)
    "us_open_hm": 930,          # 美股正盤開盤 09:30 ET
    "us_close_hm": 1600,        # 美股正盤收盤 16:00 ET
}


def is_us_dst(dt_utc: datetime) -> bool:
    """標準美東夏令時間判斷：3 月第二個週日 07:00 UTC 至 11 月第一個週日 06:00 UTC"""
    y = dt_utc.year
    # 3 月第二個週日
    mar1 = datetime(y, 3, 1, tzinfo=timezone.utc)
    days_to_sun_mar = (6 - mar1.weekday()) % 7
    dst_start = datetime(y, 3, 1 + days_to_sun_mar + 7, 7, 0, tzinfo=timezone.utc)
    # 11 月第一個週日
    nov1 = datetime(y, 11, 1, tzinfo=timezone.utc)
    days_to_sun_nov = (6 - nov1.weekday()) % 7
    dst_end = datetime(y, 11, 1 + days_to_sun_nov, 6, 0, tzinfo=timezone.utc)
    return dst_start <= dt_utc < dst_end


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

    with ThreadPoolExecutor(max_workers=12) as ex:
        results = list(ex.map(fetch_day, dates))

    rows = [r for day in results for r in day]
    rows.sort(key=lambda x: x["ts"])
    return rows


def compute_indicators(rows):
    """計算 MA(7/14/25/30/60/99/200)、3日均線 (864根)、ATR(14)、4H 新高與美東盤中時間"""
    n = len(rows)
    closes = [r["close"] for r in rows]
    highs = [r["high"] for r in rows]

    # Prefix sum for fast O(N) moving averages
    ps = [0.0] * (n + 1)
    for i in range(n):
        ps[i + 1] = ps[i] + closes[i]

    tr_ps = [0.0] * (n + 1)
    for i in range(n):
        r = rows[i]
        if i == 0:
            tr = r["high"] - r["low"]
        else:
            prev_c = closes[i - 1]
            tr = max(r["high"] - r["low"], abs(r["high"] - prev_c), abs(r["low"] - prev_c))
        tr_ps[i + 1] = tr_ps[i] + tr

    trend_bars = V3_CONFIG["trend_ma_bars"]

    for i in range(n):
        r = rows[i]
        r["ma7"] = (ps[i + 1] - ps[i + 1 - 7]) / 7.0 if i >= 6 else None
        r["ma14"] = (ps[i + 1] - ps[i + 1 - 14]) / 14.0 if i >= 13 else None
        r["ma25"] = (ps[i + 1] - ps[i + 1 - 25]) / 25.0 if i >= 24 else None
        r["ma30"] = (ps[i + 1] - ps[i + 1 - 30]) / 30.0 if i >= 29 else None
        r["ma60"] = (ps[i + 1] - ps[i + 1 - 60]) / 60.0 if i >= 59 else None
        r["ma99"] = (ps[i + 1] - ps[i + 1 - 99]) / 99.0 if i >= 98 else None
        r["ma200"] = (ps[i + 1] - ps[i + 1 - 200]) / 200.0 if i >= 199 else None
        r["ma_3d"] = (ps[i + 1] - ps[i + 1 - trend_bars]) / trend_bars if i >= trend_bars - 1 else None
        r["atr14"] = (tr_ps[i + 1] - tr_ps[i + 1 - 14]) / 14.0 if i >= 13 else None
        r["atr_pct"] = (r["atr14"] / r["close"] * 100.0) if r["atr14"] else None
        r["high_4h"] = max(highs[i - 47:i + 1]) if i >= 47 else None
        r["is_new_4h_high"] = (i >= 47 and r["high"] >= max(highs[i - 47:i]))

        dt_utc = datetime.fromtimestamp(r["ts"], timezone.utc)
        offset_h = -4 if is_us_dst(dt_utc) else -5
        dt_et = dt_utc + timedelta(hours=offset_h)
        r["et_wd"] = dt_et.weekday()
        r["et_hm"] = dt_et.hour * 100 + dt_et.minute


def backtest_v3(symbol: str, rows: list, backtest_start_dt: str = "2026-07-23 00:00", fee_rt_pct: float = 0.08):
    """執行 V3 穩健雙刀流回測（單一標的零重疊持倉、保守同K先算停損）"""
    cfg = V3_CONFIG
    trades = []
    last_used_peak_idx = -1
    busy_until_idx = -1
    max_hold = cfg["max_hold_bars"]
    slope_bars = cfg["trend_slope_bars"]
    start_scan = cfg["trend_ma_bars"] + slope_bars

    for i in range(start_scan, len(rows) - 1):
        if i <= busy_until_idx:
            continue
        r = rows[i]
        prev = rows[i - 1]
        if r["dt"] < backtest_start_dt:
            continue

        # 1. 美股正盤時段濾網（週一～週五 美東 09:30 ~ 16:00）
        if r["et_wd"] > 4 or not (cfg["us_open_hm"] <= r["et_hm"] <= cfg["us_close_hm"]):
            continue

        # 2. 核心觸發：當根 5m 收盤同時跌破 MA25 & MA30（前一根還在其中至少一條之上）+ 實體黑K + MA7 下彎
        below_both_now = (r["close"] < r["ma25"]) and (r["close"] < r["ma30"])
        above_prev = (prev["close"] >= prev["ma25"]) or (prev["close"] >= prev["ma30"])
        if not (below_both_now and above_prev):
            continue
        if not (r["close"] < r["open"] and r["ma7"] < prev["ma7"]):
            continue

        # 3. 3 日大趨勢自動切換：3 日均線必須下彎（順大勢狙擊反彈起跌點，不擋火車）
        if r["ma_3d"] is None or rows[i - slope_bars]["ma_3d"] is None:
            continue
        if r["ma_3d"] >= rows[i - slope_bars]["ma_3d"]:
            continue

        atr = r["atr14"]
        if not atr or atr <= 0:
            continue

        # 4. 過去 15 根 K 棒內（75 分鐘內）曾創下 4 小時新高，且高點距離綠線 MA60 >= 4.0 倍 ATR
        win_start = max(48, i - cfg["lookback_after_high"])
        peak_idx = None
        peak_high = -1.0
        for j in range(win_start, i + 1):
            if rows[j]["is_new_4h_high"] and rows[j]["high"] >= peak_high:
                peak_high = rows[j]["high"]
                peak_idx = j

        if peak_idx is None or peak_idx == last_used_peak_idx:
            continue
        if peak_high < max(x["high"] for x in rows[peak_idx:i + 1]):
            continue

        peak_row = rows[peak_idx]
        peak_atr = peak_row["atr14"] or atr
        surge_in_atr = (peak_high - peak_row["ma60"]) / peak_atr
        if surge_in_atr < cfg["min_surge_atr"]:
            continue

        # 5. 動態 ATR 50/50 分批出場管理
        last_used_peak_idx = peak_idx
        entry_p = r["close"]
        orig_stop = min(peak_high + cfg["peak_stop_buf_atr"] * atr, entry_p + cfg["sl_atr"] * atr)
        tp1_p = entry_p - cfg["tp1_atr"] * atr
        tp2_p = entry_p - cfg["tp2_atr"] * atr

        stop1 = orig_stop
        stop2 = orig_stop
        tp1_done = False
        ret1 = None
        ret2 = None
        tp1_idx = -1
        end_k = min(len(rows) - 1, i + max_hold)
        exit_idx = end_k
        exit_p = rows[end_k]["close"]
        mfe_atr = 0.0
        d1 = ""
        d2 = ""

        for k in range(i + 1, end_k + 1):
            rk = rows[k]
            fav_atr = (entry_p - rk["low"]) / atr
            if fav_atr > mfe_atr:
                mfe_atr = fav_atr

            just_hit_tp1 = False
            if not tp1_done:
                if rk["high"] >= stop1:
                    ret1 = (entry_p - stop1) / entry_p * 100.0
                    tp1_done = True
                    d1 = f"停損({ret1:+.1f}%)"
                elif rk["low"] <= tp1_p:
                    ret1 = (entry_p - tp1_p) / entry_p * 100.0
                    tp1_done = True
                    tp1_idx = k
                    just_hit_tp1 = True
                    d1 = f"半倉TP1({ret1:+.1f}%)"

            if ret2 is None:
                if rk["high"] >= stop2:
                    ret2 = (entry_p - stop2) / entry_p * 100.0
                    exit_idx = k
                    exit_p = stop2
                    d2 = f"尾倉防線({ret2:+.1f}%)" if stop2 < orig_stop else f"尾倉停損({ret2:+.1f}%)"
                elif rk["low"] <= tp2_p:
                    ret2 = (entry_p - tp2_p) / entry_p * 100.0
                    exit_idx = k
                    exit_p = tp2_p
                    d2 = f"尾倉TP2({ret2:+.1f}%)"
                elif mfe_atr >= cfg["runner_ma14_atr"] and rk["close"] > rk["ma14"]:
                    ret2 = (entry_p - rk["close"]) / entry_p * 100.0
                    exit_idx = k
                    exit_p = rk["close"]
                    d2 = f"尾倉MA14({ret2:+.1f}%)"

            # 次根 K 起將尾倉防守移至進場價 + 0.2*ATR
            if just_hit_tp1:
                stop2 = min(stop2, entry_p + cfg["runner_be_buf_atr"] * atr)

            if tp1_done and ret2 is not None:
                break

        end_ret = (entry_p - rows[end_k]["close"]) / entry_p * 100.0
        if ret1 is None:
            ret1 = end_ret
            d1 = f"4h({ret1:+.1f}%)"
        if ret2 is None:
            ret2 = end_ret
            d2 = f"4h({ret2:+.1f}%)"

        busy_until_idx = exit_idx
        net_ret = 0.5 * ret1 + 0.5 * ret2 - fee_rt_pct
        f4h_low = min(x["low"] for x in rows[i:min(len(rows), i + 49)])
        drop_4h = (entry_p - f4h_low) / entry_p * 100.0

        trades.append({
            "symbol": symbol,
            "peak_dt": peak_row["dt"],
            "peak_high": round(peak_high, 2),
            "surge_atr": round(surge_in_atr, 1),
            "atr_pct": round(r["atr_pct"], 2),
            "entry_dt": r["dt"],
            "entry_p": round(entry_p, 2),
            "stop_p": round(orig_stop, 2),
            "tp1_p": round(tp1_p, 2),
            "tp2_p": round(tp2_p, 2),
            "tp1_hit": tp1_idx >= 0,
            "drop_4h_pct": round(drop_4h, 2),
            "exit_dt": rows[exit_idx]["dt"],
            "exit_p": round(exit_p, 2),
            "net_ret_pct": round(net_ret, 2),
            "reason": f"{d1} + {d2}",
        })

    return trades


def summarize_portfolio(all_trades: list, title: str):
    trades = sorted(all_trades, key=lambda x: x["entry_dt"])
    n = len(trades)
    if n == 0:
        print(f"【{title}】無符合條件之交易。")
        return

    wins = [t for t in trades if t["net_ret_pct"] > 0]
    losses = [t for t in trades if t["net_ret_pct"] <= 0]
    tot = sum(t["net_ret_pct"] for t in trades)
    avg_w = sum(t["net_ret_pct"] for t in wins) / len(wins) if wins else 0.0
    avg_l = sum(t["net_ret_pct"] for t in losses) / len(losses) if losses else 0.0
    pf = (sum(t["net_ret_pct"] for t in wins) / abs(sum(t["net_ret_pct"] for t in losses))) if losses else 99.9
    payoff = (avg_w / abs(avg_l)) if avg_l < 0 else 99.9

    eq = 0.0
    peak_eq = 0.0
    mdd = 0.0
    max_cl = 0
    cur_cl = 0
    for t in trades:
        eq += t["net_ret_pct"]
        if eq > peak_eq:
            peak_eq = eq
        if peak_eq - eq > mdd:
            mdd = peak_eq - eq
        if t["net_ret_pct"] <= 0:
            cur_cl += 1
            max_cl = max(max_cl, cur_cl)
        else:
            cur_cl = 0

    soxs_t = [t for t in trades if t["symbol"] == "SOXSUSDT"]
    soxl_t = [t for t in trades if t["symbol"] == "SOXLUSDT"]

    print("=" * 124)
    print(f"【{title}】")
    print(f"  總出手：{n} 筆（SOXS {len(soxs_t)} 筆 {sum(t['net_ret_pct'] for t in soxs_t):+.2f}% ｜ SOXL {len(soxl_t)} 筆 {sum(t['net_ret_pct'] for t in soxl_t):+.2f}%）")
    print(f"  實戰勝率：{len(wins)}/{n} ({len(wins)/n*100:.1f}%) ｜ 獲利因子 (PF)：{pf:.2f} ｜ 盈虧比：{payoff:.2f}（平均賺 {avg_w:+.2f}% / 平均賠 {avg_l:+.2f}%）")
    print(f"  累計淨報酬（1x 無槓桿單利）：{tot:+.2f}% ｜ 最大回撤 (MDD)：-{mdd:.2f}% ｜ 報酬回撤比：{tot/mdd if mdd > 0 else 99.9:.2f}x ｜ 最大連虧：{max_cl} 筆")
    print("-" * 124)
    print("  逐月表現：")
    by_m = {}
    for t in trades:
        by_m.setdefault(t["entry_dt"][:7], []).append(t)
    for ym in sorted(by_m):
        arr = by_m[ym]
        w = sum(1 for x in arr if x["net_ret_pct"] > 0)
        sc = sum(1 for x in arr if x["symbol"] == "SOXSUSDT")
        lc = sum(1 for x in arr if x["symbol"] == "SOXLUSDT")
        print(f"    {ym} | {len(arr):>2} 筆 (SOXS {sc:>2} / SOXL {lc:>2}) | 勝率 {w:>2}/{len(arr):<2} ({w/len(arr)*100:5.1f}%) | 月淨利 {sum(x['net_ret_pct'] for x in arr):>+6.2f}%")
    print("-" * 124)
    print(f"{'#':>2} | {'標的':<8} | {'進場時間 (TPE)':<16} | {'進場價':>6} | {'4H高點':>6} | {'乖離':>6} | {'5m波動':>6} | {'4H最大跌':>7} | {'淨損益':>7} | {'動態分批出場明細'}")
    print("-" * 124)
    for idx, t in enumerate(trades, 1):
        print(
            f"{idx:>2} | {t['symbol']:<8} | {t['entry_dt']:<16} | {t['entry_p']:>6.2f} | "
            f"{t['peak_high']:>6.2f} | {t['surge_atr']:>4.1f}AT | {t['atr_pct']:>5.2f}% | "
            f"-{t['drop_4h_pct']:>5.2f}% | {t['net_ret_pct']:>+6.2f}% | {t['reason']}"
        )
    print("=" * 124)


def main():
    parser = argparse.ArgumentParser(description="SOXSUSDT + SOXLUSDT 5m 雙刀流 V3 穩健優化回測腳本")
    parser.add_argument("--symbol", default="BOTH", choices=["BOTH", "SOXSUSDT", "SOXLUSDT"], help="回測標的")
    parser.add_argument("--start", default="2026-07-23", help="正式回測起始日 (YYYY-MM-DD)")
    parser.add_argument("--end", default="2026-11-21", help="正式回測結束日 (YYYY-MM-DD)")
    parser.add_argument("--fee-rt", type=float, default=0.08, help="來回手續費+滑價總成本 %% (預設 0.08%%)")
    parser.add_argument("--export-json", default="", help="匯出交易明細 JSON 路徑")
    args = parser.parse_args()

    # 自動往前多抓 6 天作為 3 日均線 (72H) 與 MA(200) 暖機區間
    warmup_start = (datetime.strptime(args.start, "%Y-%m-%d") - timedelta(days=6)).strftime("%Y-%m-%d")
    symbols = ["SOXSUSDT", "SOXLUSDT"] if args.symbol == "BOTH" else [args.symbol]

    all_trades = []
    for sym in symbols:
        print(f"正在從 Binance 下載 {sym} 5m 歷史 K 線 ({warmup_start} ~ {args.end})...")
        rows = download_binance_5m(sym, warmup_start, args.end)
        compute_indicators(rows)
        trades = backtest_v3(sym, rows, backtest_start_dt=f"{args.start} 00:00", fee_rt_pct=args.fee_rt)
        all_trades.extend(trades)

    summarize_portfolio(
        all_trades,
        f"SOXSUSDT + SOXLUSDT 雙刀流 V3 穩健優化版 ({args.start} ~ {args.end}｜來回成本 {args.fee_rt:.2f}%)",
    )

    if args.export_json:
        all_trades.sort(key=lambda x: x["entry_dt"])
        with open(args.export_json, "w", encoding="utf-8") as f:
            json.dump(all_trades, f, ensure_ascii=False, indent=2)
        print(f"已匯出 {len(all_trades)} 筆交易明細至 {args.export_json}")


if __name__ == "__main__":
    main()
