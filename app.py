import os
# จำกัด OCR ให้ใช้ 1 CPU thread ต่อการเรียก — ต้องตั้งก่อน import onnxruntime/ddddocr
# กันไม่ให้ worker เยอะๆ ทำ onnxruntime แตก thread จน CPU thrash แย่งกับเว็บอื่นบนเครื่อง
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("ORT_NUM_THREADS", "1")

import random
import time
import threading
import uuid
import concurrent.futures
from flask import Flask, request, jsonify, send_from_directory
import requests
import ddddocr

app = Flask(__name__, static_folder="static")

CAPTCHA_URL = "https://party.xd.com/captcha/captcha/{}"
SUBMIT_URL  = "https://party.xd.com/event/2021feba/ajax_submit"

# จำนวนรอบที่จะ retry ทั้งชุด (captcha + submit) เมื่อ "เติม code แล้วไม่เข้า"
# ตั้งสูงหน่อยเพราะยอมช้าได้ ขอให้พยายามจนกว่าจะผ่าน (server แน่น/captcha ผิด → รอแล้วลองใหม่)
MAX_ROUNDS = 5
# หน่วงเวลาระหว่าง request — กำหนดตายตัวฝั่ง server ไม่ให้ user ตั้งเอง (กันยิงถี่จนโดน block)
FIXED_DELAY = 3
# จำนวนคู่ที่เติมพร้อมกัน — ปรับได้ผ่าน env `MAX_WORKERS` โดยไม่ต้องแก้โค้ด
# เครื่องแรม 7.8GB รับ 20 ได้สบาย (ใช้ ~1.3GB เหลือให้เว็บอื่น 5GB+)
# เพดานจริงคือ rate-limit ของ party.xd.com ถ้าเห็น fail/captcha พุ่งให้ลดลง
MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "20"))
# อายุของ job ก่อนถูกลบทิ้ง (กันหน่วยความจำโตไม่มีวันสิ้นสุด)
JOB_TTL = 1800  # วินาที

ocr = ddddocr.DdddOcr(show_ad=False)

jobs = {}
jobs_lock = threading.Lock()
# lock แยกสำหรับอัปเดตสถานะภายใน job (นับ done/success, ต่อ list) กัน race ตอนหลาย worker
state_lock = threading.Lock()
# session แยกต่อ thread — captcha ผูกกับ session จึงห้ามใช้ session เดียวกันข้าม worker
_thread_local = threading.local()


def worker_session():
    s = getattr(_thread_local, "session", None)
    if s is None:
        s = new_session()
        _thread_local.session = s
    return s


def new_session():
    """สร้าง session แยกต่อ 1 job — กัน cookie/captcha ของแต่ละงานทับกัน"""
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://party.xd.com/event/2021feba",
    })
    return s


def prune_jobs(now):
    """ลบ job ที่จบแล้วและเก่าเกิน JOB_TTL"""
    with jobs_lock:
        stale = [
            jid for jid, j in jobs.items()
            if j.get("status") in ("done", "stopped")
            and now - j.get("finished_at", now) > JOB_TTL
        ]
        for jid in stale:
            jobs.pop(jid, None)


def get_captcha(session):
    captcha_id = random.random()
    r = session.get(CAPTCHA_URL.format(captcha_id), timeout=10)
    r.raise_for_status()
    # onnxruntime ปลอดภัยกับ multi-thread — ให้ OCR ทำขนานได้ตามจำนวน worker
    text = ocr.classification(r.content).strip()
    return text, str(captcha_id)


def redeem_one(session, server_id, player_id, code, max_retry=5):
    """ยิง submit 1 ครั้ง โดย retry เฉพาะกรณี captcha ผิด"""
    result, captcha = {}, "-"
    for attempt in range(1, max_retry + 1):
        captcha, captcha_id = get_captcha(session)
        data = {
            "server_id": server_id,
            "playerid": player_id,
            "code": code,
            "captcha": captcha,
            "captcha_identifier": captcha_id,
        }
        res = session.post(SUBMIT_URL, data=data, timeout=10)
        result = res.json()
        raw = str(result).lower()
        if any(x in raw for x in ["verification", "captcha", "wrong"]):
            time.sleep(1)
            continue
        return result, captcha, attempt
    return result, captcha, max_retry


def classify(result):
    raw = str(result).lower()
    if any(x in raw for x in ["success", '"code":0', "'code': 0"]):
        return "ok"
    if any(x in raw for x in ["already", "used", "redeemed"]):
        return "skip"
    if any(x in raw for x in ["verification", "captcha", "wrong"]):
        return "captcha"
    return "fail"


def process_pair(job, server_id, index, code, pid, delay):
    """เติม 1 คู่ (code+pid) พร้อม retry — ทำงานใน worker thread"""
    if job.get("stop"):
        return

    session = worker_session()
    status, msg, captcha, attempts, round_no = "error", "", "-", 0, 0

    # ทำเครื่องหมายว่า "กำลังเติม" คู่นี้ เพื่อโชว์สถานะสดให้ user เห็นว่ากำลังทำงาน
    with state_lock:
        job["active"][index] = {"pid": pid, "code": code}

    # retry ทั้งชุดสูงสุด MAX_ROUNDS รอบ ถ้า "เติมแล้วไม่เข้า" (fail/captcha/error)
    for round_no in range(1, MAX_ROUNDS + 1):
        try:
            result, captcha, attempts = redeem_one(session, server_id, pid, code)
            msg = result.get("msg") or result.get("message") or str(result)
            status = classify(result)
            if status == "captcha":
                msg = f"captcha ผิดครบ {attempts} ครั้ง | {msg}"
        except Exception as e:
            status, msg, captcha, attempts = "error", str(e), "-", 0

        if status in ("ok", "skip"):
            break
        # ไม่เข้า (captcha ผิด / server แน่น / fail) → รอแบบ backoff แล้วลองใหม่จนครบรอบ
        if round_no < MAX_ROUNDS and not job.get("stop"):
            wait = min(2 * round_no, 12)  # 2, 4, 6, 8... สูงสุด 12 วิ
            msg = f"รอบ {round_no}/{MAX_ROUNDS} ไม่เข้า → รอ {wait}s ลองใหม่ | {msg}"
            time.sleep(wait)

    # อัปเดตสถานะงานภายใต้ lock (หลาย worker เขียนพร้อมกัน)
    with state_lock:
        job["active"].pop(index, None)  # เติมคู่นี้เสร็จแล้ว เอาออกจาก "กำลังเติม"
        bucket = job["by_pid"].setdefault(pid, {"ok": [], "skip": [], "fail": []})
        if status == "ok":
            job["success"] += 1
            bucket["ok"].append(code)
        elif status == "skip":
            bucket["skip"].append(code)
        else:  # fail / captcha / error
            bucket["fail"].append(code)
            # เก็บรายการ "ไม่ผ่าน" ครบทุกตัว เพื่อสรุปให้ไปเติมเองในเกม
            job["failed"].append({"pid": pid, "code": code, "status": status, "msg": msg})

        job["logs"].append({
            "index": index + 1,
            "pid": pid,
            "code": code,
            "captcha": captcha,
            "attempts": attempts,
            "rounds": round_no,
            "status": status,
            "msg": msg,
        })
        job["done"] += 1

    # หน่วงต่อ worker ให้สุภาพกับปลายทาง (throughput รวม ≈ MAX_WORKERS/FIXED_DELAY)
    if not job.get("stop"):
        time.sleep(delay)


def run_job(job_id, server_id, player_ids, codes, delay):
    job = jobs[job_id]
    pairs = [(code, pid) for code in codes for pid in player_ids]
    job["total"] = len(pairs)

    # เติมพร้อมกันแบบ async ด้วย worker pool ที่จำกัดจำนวน
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [
            ex.submit(process_pair, job, server_id, i, code, pid, delay)
            for i, (code, pid) in enumerate(pairs)
        ]
        for f in concurrent.futures.as_completed(futures):
            # เผยแพร่ exception ที่ไม่คาดคิด (ถ้ามี) เพื่อไม่ให้เงียบหาย
            f.result()

    if job.get("stop"):
        job["status"] = "stopped"
    else:
        job["status"] = "done"
    job["finished_at"] = time.time()


@app.route("/api/start", methods=["POST"])
def start():
    data = request.get_json(silent=True) or {}
    player_ids = data.get("player_ids") or []
    codes = data.get("codes") or []
    server_id = data.get("server_id")
    if not player_ids or not codes or not server_id:
        return jsonify({"error": "missing server_id / player_ids / codes"}), 400

    now = time.time()
    prune_jobs(now)

    job_id = str(uuid.uuid4())
    jobs[job_id] = {
        "status": "running",
        "total": 0,
        "done": 0,
        "success": 0,
        "logs": [],
        "failed": [],
        "by_pid": {},
        "active": {},
        "stop": False,
        "finished_at": now,
    }
    t = threading.Thread(
        target=run_job,
        args=(
            job_id,
            server_id,
            player_ids,
            codes,
            FIXED_DELAY,
        ),
        daemon=True,
    )
    t.start()
    return jsonify({"job_id": job_id})


@app.route("/api/status/<job_id>")
def status(job_id):
    job = jobs.get(job_id)
    if not job:
        return jsonify({"error": "not found"}), 404
    return jsonify({
        "status": job["status"],
        "total": job["total"],
        "done": job["done"],
        "success": job["success"],
        "logs": job["logs"][-50:],
        "failed": job["failed"],
        "by_pid": job["by_pid"],
        "active": list(job["active"].values()),
    })


@app.route("/api/stop/<job_id>", methods=["POST"])
def stop(job_id):
    job = jobs.get(job_id)
    if job:
        job["stop"] = True
    return jsonify({"ok": True})


@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def serve(path):
    return send_from_directory("static", "index.html")


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
