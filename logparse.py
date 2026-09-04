#!/usr/bin/env python
"""Разбор S3 access-логов и расчёт показателей нагрузки.

Модуль самодостаточен: внешних зависимостей нет, ничего вне этого каталога
не импортируется. Каталог сервера можно скопировать на любую машину как есть.

Формат строки — стандартный S3 server access log:
    owner bucket [time] remote_ip requester req_id operation key "request_uri"
    status error bytes_sent object_size total_time turn_around "referer" "ua" ...

Агрегаты (ключ везде начинается с минуты события):
    C   (minute, net, prefix) -> запросов
    B   (minute, net, prefix) -> bytes_sent            (исходящий трафик)
    E   (minute, net, prefix) -> запросов со статусом 404
    WB  (minute, net, prefix) -> object_size PUT/POST  (входящий трафик)
    WN  (minute, net, prefix) -> операций записи
    IPB (minute, ip)          -> bytes_sent
    WIP (minute, ip)          -> object_size записи
    LAT (minute, бакет)       -> запросов GET.OBJECT; бакет = int(4*log2(мс)),
                                 шаг 2^(1/4) ~ 19%, значение = 2**(бакет/4)
    UA  user-agent            -> запросов
    K4  prefix -> список ключей, отдавших 404 (для разбора причин)
"""
import collections
import math
import re
import statistics

LINE = re.compile(
    r'^(\S+) (\S+) \[([^\]]+)\] (\S+) (\S+) (\S+) (\S+) (\S+) "([^"]*)" '
    r'(\S+) (\S+) (\S+) (\S+) (\S+) (\S+) "([^"]*)" "([^"]*)"'
)
MON = {m: i + 1 for i, m in enumerate(
    "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split())}

PREFIX_DEPTH = 3          # сколько сегментов ключа считать «назначением»
MAX_404_SAMPLES = 200

MIB = 1 << 20
TIB = 1 << 40

COUNTER_KEYS = ("C", "B", "E", "WB", "WN", "IPB", "WIP", "LAT", "UA")


def netof(ip):
    o = ip.split(".")
    return "%s.%s.0.0/16" % (o[0], o[1]) if len(o) == 4 else "?"


def prefix_of(key, depth=PREFIX_DEPTH):
    parts = [x for x in key.split("/") if x][:depth]
    return "/".join(parts) or "-"


# ------------------------------------------------------------------ разбор

def work(path):
    """Разбирает один файл журнала. Возвращает кортеж агрегатов."""
    C = collections.Counter(); B = collections.Counter(); E = collections.Counter()
    WB = collections.Counter(); WN = collections.Counter()
    IPB = collections.Counter(); WIP = collections.Counter()
    LAT = collections.Counter(); UA = collections.Counter()
    K4 = collections.defaultdict(set)

    with open(path, "r", errors="replace") as fh:
        for line in fh:
            m = LINE.match(line)
            if not m:
                continue
            g = m.groups()
            ts = g[2]                      # 03/Sep/2026:09:56:26 +0000
            try:
                minute = "%s-%02d-%s %s:%s" % (
                    ts[7:11], MON[ts[3:6]], ts[0:2], ts[12:14], ts[15:17])
            except (KeyError, IndexError):
                continue

            ip, op, key, st = g[3], g[6], g[7].replace("%2F", "/"), g[9]
            sent = int(g[11]) if g[11].isdigit() else 0
            osize = int(g[12]) if g[12].isdigit() else 0
            ttime = int(g[13]) if g[13].isdigit() else 0

            net, pref = netof(ip), prefix_of(key)
            k = (minute, net, pref)

            C[k] += 1
            B[k] += sent
            IPB[(minute, ip)] += sent
            UA[g[16].split()[0] if g[16] and g[16] != "-" else "-"] += 1

            if st == "404":
                E[k] += 1
                if len(K4[pref]) < MAX_404_SAMPLES:
                    K4[pref].add(key)

            # POST.UPLOAD (завершение многочастичной загрузки) намеренно
            # пропускается: в нём object_size равен размеру всего собранного
            # объекта, и его учёт даёт ложные пики записи в десятки GiB.
            if op.endswith(".PUT.OBJECT") or op.endswith(".PUT.PART"):
                WB[k] += osize
                WN[k] += 1
                WIP[(minute, ip)] += osize

            if op == "REST.GET.OBJECT":
                # шаг 2^(1/4): разрешение ~19%, иначе p50 округлялся бы вдвое
                LAT[(minute, 0 if ttime <= 0
                     else min(96, int(4 * math.log2(ttime))))] += 1

    return C, B, E, WB, WN, IPB, WIP, LAT, UA, dict(K4)


def merge(agg, results):
    """Подмешивает результаты work() в общий словарь агрегатов."""
    k4 = collections.defaultdict(set)
    for res in results:
        for name, part in zip(COUNTER_KEYS, res[:9]):
            agg[name].update(part)
        for pref, keys in res[9].items():
            if len(k4[pref]) < MAX_404_SAMPLES:
                k4[pref] |= set(keys)
    if k4:
        agg["K4"] = {p: sorted(v)[:MAX_404_SAMPLES] for p, v in k4.items()}
    return agg


def empty_agg():
    agg = {k: collections.Counter() for k in COUNTER_KEYS}
    agg["K4"] = {}
    return agg


# ---------------------------------------------------------------- расчёты

def per_minute(agg):
    req = collections.Counter(); rd = collections.Counter()
    e404 = collections.Counter(); wr = collections.Counter(); wn = collections.Counter()
    for (mi, _n, _p), v in agg["C"].items():
        req[mi] += v
    for (mi, _n, _p), v in agg["B"].items():
        rd[mi] += v
    for (mi, _n, _p), v in agg["E"].items():
        e404[mi] += v
    for (mi, _n, _p), v in agg["WB"].items():
        wr[mi] += v
    for (mi, _n, _p), v in agg["WN"].items():
        wn[mi] += v
    return req, rd, e404, wr, wn


def trim_incomplete_tail(mins, req):
    """Отрезает хвост, который журнал ещё не догнал.

    Последняя минута всегда дописывается прямо сейчас и выглядит как обвал
    нагрузки. Идём с конца, пока минута заметно ниже медианы десяти
    предшествующих.
    """
    if len(mins) < 15:
        return mins, 0
    i = len(mins) - 1
    while i > 10:
        ref = statistics.median(req[m] for m in mins[i - 10:i])
        if ref and req[mins[i]] < 0.95 * ref:
            i -= 1
        else:
            break
    return mins[:i + 1], len(mins) - (i + 1)


def flows(agg, window, kind):
    wset = set(window)
    byte_key = "B" if kind == "read" else "WB"
    cnt_key = "C" if kind == "read" else "WN"
    b = collections.Counter(); c = collections.Counter(); e = collections.Counter()
    for (mi, n, p), v in agg[byte_key].items():
        if mi in wset:
            b[(n, p)] += v
    for (mi, n, p), v in agg[cnt_key].items():
        if mi in wset:
            c[(n, p)] += v
    for (mi, n, p), v in agg["E"].items():
        if mi in wset:
            e[(n, p)] += v
    return b, c, e


def subnets(agg, window, kind):
    wset = set(window)
    byte_key = "B" if kind == "read" else "WB"
    cnt_key = "C" if kind == "read" else "WN"
    ip_key = "IPB" if kind == "read" else "WIP"
    b = collections.Counter(); c = collections.Counter()
    nodes = collections.defaultdict(set)
    for (mi, n, _p), v in agg[byte_key].items():
        if mi in wset:
            b[n] += v
    for (mi, n, _p), v in agg[cnt_key].items():
        if mi in wset:
            c[n] += v
    for (mi, ip), _v in agg[ip_key].items():
        if mi in wset:
            nodes[netof(ip)].add(ip)
    return b, c, nodes


def top_nodes(agg, window, kind, n=8):
    wset = set(window)
    key = "IPB" if kind == "read" else "WIP"
    acc = collections.Counter()
    for (mi, ip), v in agg[key].items():
        if mi in wset:
            acc[ip] += v
    return acc.most_common(n)


def latency(agg, window):
    wset = set(window)
    hist = collections.Counter()
    for (mi, bucket), v in agg["LAT"].items():
        if mi in wset:
            hist[bucket] += v
    total = sum(hist.values())
    out, acc = {}, 0
    for bucket in sorted(hist):
        acc += hist[bucket]
        for q in (0.5, 0.9, 0.99):
            if q not in out and acc >= total * q:
                out[q] = round(2 ** (bucket / 4))
    return total, out


def shape(key):
    """Путь с цифрами, заменёнными на N — чтобы увидеть повторяющуюся форму."""
    return "/".join(re.sub(r"\d+", "N", s) for s in key.split("/"))


def suspicious_404(agg, flow_c, flow_e, secs, min_share=0.15, min_rps=1.0):
    """Префиксы, где заметная доля запросов не возвращает данных.

    Порог по абсолютной частоте отсекает шум: пара промахов на служебном
    префиксе даёт «50% 404», но нагрузки не создаёт.
    """
    out = []
    agg404 = collections.Counter(); aggreq = collections.Counter()
    for (_n, p), v in flow_e.items():
        agg404[p] += v
    for (_n, p), v in flow_c.items():
        aggreq[p] += v
    for p, miss in agg404.most_common(10):
        total = aggreq[p]
        if not total or miss / total < min_share or miss / secs < min_rps:
            continue
        shapes = collections.Counter(shape(k) for k in agg.get("K4", {}).get(p, []))
        out.append({"prefix": p, "miss": miss, "total": total,
                    "share": 100 * miss / total, "rps": miss / secs,
                    "shapes": shapes.most_common(2),
                    "samples": agg.get("K4", {}).get(p, [])[:2]})
    return out


def analyse(agg, win_minutes=10, trim=True):
    """Сводка по агрегатам: окно «сейчас», потоки, узлы, задержка."""
    req, rd, e404, wr, wn = per_minute(agg)
    all_mins = sorted(req)
    if not all_mins:
        return None
    if trim:
        mins, trimmed = trim_incomplete_tail(all_mins, req)
    else:
        # исторический период доставлен полностью — резать хвост нечего
        mins, trimmed = all_mins, 0
    window = mins[-win_minutes:] if len(mins) >= win_minutes else mins
    if not window:
        return None
    secs = len(window) * 60

    w_req = sum(req[m] for m in window)
    w_rd = sum(rd[m] for m in window)
    w_wr = sum(wr[m] for m in window)
    w_wn = sum(wn[m] for m in window)
    w_e4 = sum(e404[m] for m in window)

    observed = 0.0
    for i in range(max(0, len(mins) - 9)):
        chunk = mins[i:i + 10]
        if len(chunk) == 10:
            observed = max(observed, sum(rd[m] for m in chunk) / (600 * MIB))

    fb, fc, fe = flows(agg, window, "read")
    wb, wc, _ = flows(agg, window, "write")
    nb, nc, nodes = subnets(agg, window, "read")
    _total, lat = latency(agg, window)

    rd_rows = [{"net": n, "prefix": p, "mibs": b / secs / MIB,
                "rps": fc[(n, p)] / secs,
                "pct404": 100 * fe[(n, p)] / fc[(n, p)] if fc[(n, p)] else 0}
               for (n, p), b in fb.most_common(12)]
    wr_rows = [{"net": n, "prefix": p, "mibs": b / secs / MIB,
                "ops": wc[(n, p)] / secs}
               for (n, p), b in wb.most_common(10) if b > 0]
    net_rows = [{"net": n, "nodes": len(nodes.get(n, ())), "rps": nc[n] / secs,
                 "mibs": b / secs / MIB} for n, b in nb.most_common(8)]

    top_rd = [(ip, b / secs / MIB) for ip, b in top_nodes(agg, window, "read")]
    top_wr = [(ip, b / secs / MIB) for ip, b in top_nodes(agg, window, "write") if b > 0]

    return {
        "records": sum(req.values()),
        "series": [(m, rd[m] / 60 / MIB, wr[m] / 60 / MIB) for m in all_mins],
        "trimmed": trimmed,
        "ceiling_observed": observed,
        "nodes_total": sum(len(v) for v in nodes.values()),
        "win_label": "%s–%s" % (window[0][11:], window[-1][11:]),
        "window": {
            "from": window[0][11:], "to": window[-1][11:], "mins": len(window),
            "rps": w_req / secs, "pct404": 100 * w_e4 / w_req if w_req else 0,
            "read_mibs": w_rd / secs / MIB, "write_mibs": w_wr / secs / MIB,
            "total_mibs": (w_rd + w_wr) / secs / MIB,
            "gbit": (w_rd + w_wr) * 8 / secs / 1e9,
            "puts": w_wn / secs,
            "p50": lat.get(0.5, 0), "p90": lat.get(0.9, 0), "p99": lat.get(0.99, 0),
        },
        "day": {"from": all_mins[0][11:], "to": all_mins[-1][11:],
                "read_tib": sum(rd.values()) / TIB,
                "write_tib": sum(wr.values()) / TIB},
        "rd": rd_rows, "wr": wr_rows, "rd_nets": net_rows,
        "top_rd": top_rd, "top_wr": top_wr,
        "tops_rows": ([{"ip": ip, "dir": "чтение", "mibs": v} for ip, v in top_rd[:5]] +
                      [{"ip": ip, "dir": "запись", "mibs": v} for ip, v in top_wr[:3]]),
        "susp": suspicious_404(agg, fc, fe, secs),
    }
