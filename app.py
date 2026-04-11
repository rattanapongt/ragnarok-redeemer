import random
import time
import threading
import uuid
from flask import Flask, request, jsonify, send_from_directory
import requests
import ddddocr

app = Flask(__name__, static_folder="static")

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://party.xd.com/event/2021feba",
})

CAPTCHA_URL = "https://party.xd.com/captcha/captcha/{}"
SUBMIT_URL  = "https://party.xd.com/event/2021feba/ajax_submit"

ocr = ddddocr.DdddOcr(show_ad=False)

# job storage (in-memory)
jobs = {}


def get_captcha():
    captcha_id = random.random()
    r = SESSION.get(CAPTCHA_URL.format(captcha_id), timeout=10)
    r.raise_for_status()
    text = ocr.classification(r.content).strip()
    return text, str(captcha_id)


def redeem_one(server_id, player_id, code):
    captcha, captcha_id = get_captcha()
    data = {
        "server_id": server_id,
        "playerid": player_id,
        "code": code,
        "captcha": captcha,
        "captcha_identifier": captcha_id,
    }
    res = SESSION.post(SUBMIT_URL, data=data, timeout=10)
    return res.json(), captcha


def run_job(job_id, server_id, player_ids, codes, delay):
    job = jobs[job_id]
    pairs = [(code, pid) for code in codes for pid in player_ids]
    job["total"] = len(pairs)

    for i, (code, pid) in enumerate(pairs):
        if job.get("stop"):
            job["status"] = "stopped"
            break

        try:
            result, captcha = redeem_one(server_id, pid, code)
            msg = result.get("msg") or result.get("message") or str(result)
            raw = str(result).lower()

            if any(x in raw for x in ["success", '"code":0', "'code': 0"]):
                status = "ok"
                job["success"] += 1
            elif any(x in raw for x in ["already", "used", "redeemed"]):
                status = "skip"
            elif any(x in raw for x in ["verification", "captcha", "wrong"]):
                status = "captcha"
            else:
                status = "fail"

        except Exception as e:
            msg = str(e)
            captcha = "-"
            status = "error"

        job["logs"].append({
            "index": i + 1,
            "pid": pid,
            "code": code,
            "captcha": captcha,
            "status": status,
            "msg": msg,
        })
        job["done"] = i + 1

        if i < len(pairs) - 1 and not job.get("stop"):
            time.sleep(delay)

    if not job.get("stop"):
        job["status"] = "done"


@app.route("/api/start", methods=["POST"])
def start():
    data = request.json
    job_id = str(uuid.uuid4())
    jobs[job_id] = {
        "status": "running",
        "total": 0,
        "done": 0,
        "success": 0,
        "logs": [],
        "stop": False,
    }
    t = threading.Thread(
        target=run_job,
        args=(
            job_id,
            data["server_id"],
            data["player_ids"],
            data["codes"],
            int(data.get("delay", 3)),
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
        "logs": job["logs"][-50:],  # ส่งแค่ 50 บรรทัดล่าสุด
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
    app.run(host="0.0.0.0", port=8080)
