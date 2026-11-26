#!/usr/bin/env python3
"""
SOXSUSDT 5m「漂亮均線 + 回踩 MA200 站回 MA25/30」做多起漲點回測腳本 (V4 精準/動態雙模式)
==========================================================================================
專注單一標的：SOXSUSDT (Binance 3倍做空半導體永續合約)
策略定位：專門狙擊美股盤前～早盤「七線收斂糾結 + 平靜縮幅 + 回踩 MA200 支撐反彈」的多頭起漲段，
          與做空腳本 soxs_sniper.py 互補。

核心規則：
  1. 【黃金早盤窗】：嚴格限定週一～週五 美東 ET 04:00 ~ 12:00（美股盤前＋早盤前 2.5 小時，自動處理夏/冬令時），
     徹底剔除美股午後無量耗損、盤後與週末洗盤。
  2. 【MA200 支撐結構】：進場前第 6~15 根 K 棒皆收盤站穩 5m MA(200) 之上；最近 4 根內回踩觸及 MA(200)
     附近（低點距 MA200 <= 1.5 倍 ATR 或直接穿觸 MA200）；當根出現實體陽 K 同時收盤站回 MA(7)、MA(25)、
     MA(30) 與 MA(200) 之上，且貼近 MA(200)：
       - sniper  精準版 (預設)：收盤價離 MA(200) <= +1.2%
       - dynamic 動態版       ：收盤價離 MA(200) <= 2.5 倍 ATR(14)
  3. 【漂亮均線過濾（只留乾淨底部起漲）】：
       - 七線收斂：MA(7/14/25/30/99/120/200) 全部 7 條均線上下總寬度 <= 2.6 倍 ATR(14)
       - 無長線壓頂：收盤價站上 MA(99) 與 MA(120)（允許 0.1% 誤差）
       - 進場前平靜無長針：前 8 根 K 棒（40 分鐘）單根高低振幅皆 <= 2.2 倍 ATR(14)
  4. 【貼 MA200 緊停損 + 50/50 動態分批出場】：
       - 初始停損：max(min(MA200 - 0.8*ATR, 進場價 - 1.2*ATR), 進場價 - 2.2*ATR)
       - 前 50% 倉位 (TP1)：順向漲達 +2.5 ATR 立刻落袋，次根 K 起將尾倉防守移至進場價 - 0.2*ATR
       - 後 50% 尾倉 (TP2)：順向漲達 +3.75 ATR 後收盤跌破青線 MA(14) 鎖利，或達 +8.0 ATR 止盈 (最長持倉 4H)

使用方式：
  python3 soxs_long_ma200.py                                # 預設精準版 (2026-07-23 ~ 2026-11-25)
  python3 soxs_long_ma200.py --mode dynamic                 # 切換 5m 全 ATR 動態版
  python3 soxs_long_ma200.py --fee-rt 0.25                  # 3 倍高滑價壓力測試
  python3 soxs_long_ma200.py --export-json long_trades.json
"""

import argparse
import csv
import io
import json
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

SYMBOL = "SOXSUSDT"
TZ_TPE = timezone(timedelta(hours=8))

LONG_CONFIG = {
    "et_start_hm": 400,         # 美東 04:00（美股盤前開始）
    "et_end_hm": 1200,          # 美東 12:00（剔除美股午後無量段）
    "above_ma200_from": 15,     # 進場前第 15 根到第 6 根皆收盤在 MA200 之上
    "above_ma200_to": 5,
    "touch_lookback": 3,        # 最近 4 根 (i-3 ~ i) 內回踩觸及 MA200 附近
    "touch_tol_atr": 1.5,       # 回踩容許距離：1.5 倍 ATR
    "sniper_max_pct": 1.2,      # 精準版：進場價離 MA200 <= +1.2%
    "dynamic_max_atr": 2.5,     # 動態版：進場價離 MA200 <= 2.5 倍 ATR
    "max_7ma_spread_atr": 2.6,  # 七條均線 (7/14/25/30/99/120/200) 上下總寬 <= 2.6 ATR
    "max_pre_bar_atr": 2.2,     # 前 8 根單根高低振幅皆 <= 2.2 ATR（無長針洗盤）
    "pre_calm_bars": 8,
    "sl_ma200_buf_atr": 0.8,    # 停損設於 MA200 下方 0.8 ATR
    "sl_min_atr": 1.2,          # 停損至少距離進場價 1.2 ATR
    "sl_max_atr": 2.2,          # 停損上限不超過進場價 2.2 ATR
    "tp1_atr": 2.5,             # 前 50% 倉位止盈：+2.5 ATR
    "tp2_atr": 8.0,             # 後 50% 尾倉止盈：+8.0 ATR
    "runner_be_buf_atr": 0.2,   # TP1 觸發後尾倉防守移至進場價 - 0.2 ATR
    "runner_ma14_atr": 3.75,    # 漲達 +3.75 ATR 後收盤跌破 MA14 移動停利
    "max_hold_bars": 48,        # 最長持倉 48 根 5m K (4 小時)
}


def is_us_dst(dt_utc: datetime) -> bool:
    """標準美東夏令時間判斷：3 月第二個週日 07:00 UTC 至 11 月第一個週日 06:00 UTC"""
    y = dt_utc.year
    mar1 = datetime(y, 3, 1, tzinfo=timezone.utc)
    days_to_sun_mar = (6 - mar1.weekday()) % 7
    dst_start = datetime(y, 3, 1 + days_to_sun_mar + 7, 7, 0, tzinfo=timezone.utc)

    nov1 = datetime(y, 11, 1, tzinfo=timezone.utc)
    days_to_sun_nov = (6 - nov1.weekday()) % 7
    dst_end = datetime(y, 11, 1 + days_to_sun_nov, 6, 0, tzinfo=timezone.utc)
    return dst_start <= dt_utc < dst_end


def download_soxs_5m(start_ymd: str, end_ymd: str):
    """從 Binance Vision 官方歷史資料庫並行下載 SOXSUSDT 5m 永續合約 K 線"""
    start_dt = datetime.strptime(start_ymd, "%Y-%m-%d")
    end_dt = datetime.strptime(end_ymd, "%Y-%m-%d")
    dates = []
    cur = start_dt
    while cur <= end_dt:
        dates.append(cur.strftime("%Y-%m-%d"))
        cur += timedelta(days=1)

    def fetch_day(d):
        url = f"https://data.binance.vision/data/futures/um/daily/klines/{SYMBOL}/5m/{SYMBOL}-5m-{d}.zip"
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
    """計算 7 條均線 MA(7/14/25/30/99/120/200)、ATR(14) 與美東時間"""
    n = len(rows)
    closes = [r["close"] for r in rows]

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

    for i in range(n):
        r = rows[i]
        r["ma7"] = (ps[i + 1] - ps[i + 1 - 7]) / 7.0 if i >= 6 else None
        r["ma14"] = (ps[i + 1] - ps[i + 1 - 14]) / 14.0 if i >= 13 else None
        r["ma25"] = (ps[i + 1] - ps[i + 1 - 25]) / 25.0 if i >= 24 else None
        r["ma30"] = (ps[i + 1] - ps[i + 1 - 30]) / 30.0 if i >= 29 else None
        r["ma99"] = (ps[i + 1] - ps[i + 1 - 99]) / 99.0 if i >= 98 else None
        r["ma120"] = (ps[i + 1] - ps[i + 1 - 120]) / 120.0 if i >= 119 else None
        r["ma200"] = (ps[i + 1] - ps[i + 1 - 200]) / 200.0 if i >= 199 else None
        r["atr14"] = (tr_ps[i + 1] - tr_ps[i + 1 - 14]) / 14.0 if i >= 13 else None
        r["atr_pct"] = (r["atr14"] / r["close"] * 100.0) if r["atr14"] else None

        dt_utc = datetime.fromtimestamp(r["ts"], timezone.utc)
        offset_h = -4 if is_us_dst(dt_utc) else -5
        dt_et = dt_utc + timedelta(hours=offset_h)
        r["et_wd"] = dt_et.weekday()
        r["et_hm"] = dt_et.hour * 100 + dt_et.minute


def backtest_soxs_long_ma200(
    rows: list,
    mode: str = "sniper",
    backtest_start_dt: str = "2026-07-23 00:00",
    fee_rt_pct: float = 0.08,
    next_open: bool = False,
):
    """執行 SOXSUSDT 5m 漂亮均線 MA200 做多回測（零重疊持倉、保守同K先算停損）"""
    cfg = LONG_CONFIG
    trades = []
    busy_until_idx = -1
    max_hold = cfg["max_hold_bars"]

    for i in range(220, len(rows) - 2):
        if i <= busy_until_idx:
            continue
        r = rows[i]
        if r["dt"] < backtest_start_dt:
            continue

        # 1. 美股黃金早盤窗（週一～週五 美東 04:00 ~ 12:00）
        if r["et_wd"] > 4 or not (cfg["et_start_hm"] <= r["et_hm"] <= cfg["et_end_hm"]):
            continue

        atr = r["atr14"]
        m200 = r["ma200"]
        if not atr or atr <= 0 or m200 is None:
            continue

        # 2. 多頭回踩 MA200 結構：先前連 10 根站穩 MA200 之上，近 4 根內回踩觸及 MA200 附近
        if not all(
            rows[j]["ma200"] is not None and rows[j]["close"] > rows[j]["ma200"]
            for j in range(i - cfg["above_ma200_from"], i - cfg["above_ma200_to"])
        ):
            continue

        touched = any(
            abs(rows[j]["low"] - rows[j]["ma200"]) <= cfg["touch_tol_atr"] * atr
            or (rows[j]["low"] <= rows[j]["ma200"] <= rows[j]["high"])
            for j in range(i - cfg["touch_lookback"], i + 1)
        )
        if not touched:
            continue

        # 3. 實體陽 K 站回 MA7 / MA25 / MA30 / MA200 之上，且貼近 MA200
        if not (
            r["close"] > r["open"]
            and r["close"] > m200
            and r["close"] > r["ma7"]
            and r["close"] > r["ma25"]
            and r["close"] > r["ma30"]
        ):
            continue

        dist_pct = (r["close"] - m200) / m200 * 100.0
        dist_atr = (r["close"] - m200) / atr
        if mode == "sniper" and dist_pct > cfg["sniper_max_pct"]:
            continue
        if mode == "dynamic" and dist_atr > cfg["dynamic_max_atr"]:
            continue

        # 4. 漂亮均線過濾：七線收斂 + 無長線蓋頭 + 前 8 根平靜無長針
        mas7 = [r["ma7"], r["ma14"], r["ma25"], r["ma30"], r["ma99"], r["ma120"], m200]
        spread_atr = (max(mas7) - min(mas7)) / atr
        if spread_atr > cfg["max_7ma_spread_atr"]:
            continue
        if r["close"] < max(r["ma99"], r["ma120"]) * 0.999:
            continue
        pre_max_bar_atr = max(
            (rows[k]["high"] - rows[k]["low"]) for k in range(i - cfg["pre_calm_bars"], i)
        ) / atr
        if pre_max_bar_atr > cfg["max_pre_bar_atr"]:
            continue

        # 5. 貼 MA200 緊停損 + 50/50 動態分批出場
        entry_p = rows[i + 1]["open"] if next_open else r["close"]
        orig_stop = max(
            min(m200 - cfg["sl_ma200_buf_atr"] * atr, entry_p - cfg["sl_min_atr"] * atr),
            entry_p - cfg["sl_max_atr"] * atr,
        )
        tp1_p = entry_p + cfg["tp1_atr"] * atr
        tp2_p = entry_p + cfg["tp2_atr"] * atr

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
            fav_atr = (rk["high"] - entry_p) / atr
            if fav_atr > mfe_atr:
                mfe_atr = fav_atr

            just_hit_tp1 = False
            if not tp1_done:
                if rk["low"] <= stop1:
                    ret1 = (stop1 - entry_p) / entry_p * 100.0
                    tp1_done = True
                    d1 = f"停損({ret1:+.1f}%)"
                elif rk["high"] >= tp1_p:
                    ret1 = (tp1_p - entry_p) / entry_p * 100.0
                    tp1_done = True
                    tp1_idx = k
                    just_hit_tp1 = True
                    d1 = f"半倉TP1({ret1:+.1f}%)"

            if ret2 is None:
                if rk["low"] <= stop2:
                    ret2 = (stop2 - entry_p) / entry_p * 100.0
                    exit_idx = k
                    exit_p = stop2
                    d2 = f"尾倉保本({ret2:+.1f}%)" if stop2 > orig_stop else f"尾倉停損({ret2:+.1f}%)"
                elif rk["high"] >= tp2_p:
                    ret2 = (tp2_p - entry_p) / entry_p * 100.0
                    exit_idx = k
                    exit_p = tp2_p
                    d2 = f"尾倉TP2({ret2:+.1f}%)"
                elif mfe_atr >= cfg["runner_ma14_atr"] and rk["close"] < rk["ma14"]:
                    ret2 = (rk["close"] - entry_p) / entry_p * 100.0
                    exit_idx = k
                    exit_p = rk["close"]
                    d2 = f"尾倉MA14({ret2:+.1f}%)"

            if just_hit_tp1:
                stop2 = max(stop2, entry_p - cfg["runner_be_buf_atr"] * atr)

            if tp1_done and ret2 is not None:
                break

        end_ret = (rows[end_k]["close"] - entry_p) / entry_p * 100.0
        if ret1 is None:
            ret1 = end_ret
            d1 = f"4h({ret1:+.1f}%)"
        if ret2 is None:
            ret2 = end_ret
            d2 = f"4h({ret2:+.1f}%)"

        busy_until_idx = exit_idx
        net_ret = 0.5 * ret1 + 0.5 * ret2 - fee_rt_pct
        f4h_high = max(x["high"] for x in rows[i:min(len(rows), i + 49)])
        gain_4h = (f4h_high - entry_p) / entry_p * 100.0

        trades.append({
            "symbol": SYMBOL,
            "mode": mode,
            "session": "盤前" if r["et_hm"] < 930 else "早盤",
            "entry_dt": r["dt"],
            "entry_p": round(entry_p, 2),
            "ma200": round(m200, 2),
            "dist_ma200_pct": round(dist_pct, 2),
            "spread_7ma_atr": round(spread_atr, 2),
            "atr_pct": round(r["atr_pct"], 2),
            "stop_p": round(orig_stop, 2),
            "tp1_p": round(tp1_p, 2),
            "tp2_p": round(tp2_p, 2),
            "tp1_hit": tp1_idx >= 0,
            "gain_4h_pct": round(gain_4h, 2),
            "exit_dt": rows[exit_idx]["dt"],
            "exit_p": round(exit_p, 2),
            "net_ret_pct": round(net_ret, 2),
            "reason": f"{d1} + {d2}",
        })

    return trades


def print_report(trades: list, title: str):
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

    print("=" * 120)
    print(f"【{title}】")
    print(
        f"  總出手：{n} 筆 ｜ 實戰勝率：{len(wins)}/{n} ({len(wins)/n*100:.1f}%) ｜ "
        f"獲利因子 (PF)：{pf:.2f} ｜ 盈虧比：{payoff:.2f}（均賺 {avg_w:+.2f}% / 均賠 {avg_l:+.2f}%）"
    )
    print(
        f"  累計淨報酬（1x 無槓桿單利）：{tot:+.2f}%（3x 槓桿估算 {tot*3:+.1f}%） ｜ "
        f"最大回撤 (MDD)：-{mdd:.2f}% ｜ 最大連虧：{max_cl} 筆"
    )
    print("-" * 120)
    print("  逐月表現：")
    by_m = {}
    for t in trades:
        by_m.setdefault(t["entry_dt"][:7], []).append(t)
    for ym in sorted(by_m):
        arr = by_m[ym]
        w = sum(1 for x in arr if x["net_ret_pct"] > 0)
        print(
            f"    {ym} | {len(arr):>2} 筆 | 勝率 {w:>2}/{len(arr):<2} ({w/len(arr)*100:5.1f}%) | "
            f"月淨利 {sum(x['net_ret_pct'] for x in arr):>+6.2f}%"
        )
    print("-" * 120)
    print(
        f"{'#':>2} | {'進場時間 (TPE)':<16} | {'時段':<4} | {'進場價':>6} | {'MA200':>6} | "
        f"{'距200':>6} | {'7線寬':>6} | {'4H最大漲':>7} | {'淨損益':>7} | {'動態分批出場明細'}"
    )
    print("-" * 120)
    for idx, t in enumerate(trades, 1):
        print(
            f"{idx:>2} | {t['entry_dt']:<16} | {t['session']:<4} | {t['entry_p']:>6.2f} | "
            f"{t['ma200']:>6.2f} | +{t['dist_ma200_pct']:>4.2f}% | {t['spread_7ma_atr']:>4.2f}AT | "
            f"+{t['gain_4h_pct']:>5.2f}% | {t['net_ret_pct']:>+6.2f}% | {t['reason']}"
        )
    print("=" * 120)


def main():
    parser = argparse.ArgumentParser(description="SOXSUSDT 5m 漂亮均線 MA200 做多起漲點回測腳本")
    parser.add_argument("--mode", choices=["sniper", "dynamic"], default="sniper", help="sniper=精準版(<=+1.2%%) / dynamic=動態版(<=2.5ATR)")
    parser.add_argument("--start", default="2026-07-23", help="正式回測起始日 (YYYY-MM-DD)")
    parser.add_argument("--end", default="2026-11-25", help="正式回測結束日 (YYYY-MM-DD)")
    parser.add_argument("--fee-rt", type=float, default=0.08, help="來回手續費+滑價總成本 %% (預設 0.08%%)")
    parser.add_argument("--next-open", action="store_true", help="改用次根 5m K 開盤價進場驗證")
    parser.add_argument("--export-json", default="", help="匯出交易明細 JSON 路徑")
    args = parser.parse_args()

    warmup_start = (datetime.strptime(args.start, "%Y-%m-%d") - timedelta(days=4)).strftime("%Y-%m-%d")
    print(f"正在從 Binance 下載 {SYMBOL} 5m 歷史 K 線 ({warmup_start} ~ {args.end})...")
    rows = download_soxs_5m(warmup_start, args.end)
    compute_indicators(rows)
    trades = backtest_soxs_long_ma200(
        rows,
        mode=args.mode,
        backtest_start_dt=f"{args.start} 00:00",
        fee_rt_pct=args.fee_rt,
        next_open=args.next_open,
    )

    mode_lbl = "精準版 (<=+1.2% MA200)" if args.mode == "sniper" else "動態版 (<=2.5 ATR)"
    print_report(
        trades,
        f"SOXSUSDT 5m 漂亮均線 MA200 做多｜{mode_lbl} ({args.start} ~ {args.end}｜成本 {args.fee_rt:.2f}%)",
    )

    if args.export_json:
        with open(args.export_json, "w", encoding="utf-8") as f:
            json.dump(trades, f, ensure_ascii=False, indent=2)
        print(f"已匯出 {len(trades)} 筆交易明細至 {args.export_json}")


if __name__ == "__main__":
    main()
