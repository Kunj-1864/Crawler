# ✅ 1️⃣ Prepare the VPS

SSH into your VPS:

```bash
ssh user@your-server-ip
```

Then update the system and install dependencies:

```bash
sudo apt update && sudo apt install -y python3 python3-venv python3-pip tor git
```

Check that Tor works:

```bash
sudo systemctl enable --now tor
sudo systemctl status tor
```

You should see "Active: active (running) or Active: active (excited)"

---

# ✅ 2️⃣ Create the Crawler directory on the VPS

Make a clean directory:

```bash
sudo mkdir -p /opt/paritybit-crawler
sudo chown $USER:$USER /opt/paritybit-crawler
cd /opt/paritybit-crawler
```

---

# ✅ 3️⃣ Transfer your project files

 On the VPS:

   ```bash
   cd /opt
   git clone https://github.com/youruser/yourrepo.git paritybit-crawler
   ```


# ✅ 4️⃣ Create and activate the virtual environment on VPS

Inside your project folder:

```bash
cd /opt/paritybit-crawler
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install requests[socks] pyyaml beautifulsoup4 lxml pysocks stem rapidfuzz pandas
```

Test:

```bash
python -c "import requests, yaml, bs4; print('venv OK')"
```

---

# ✅ 5️⃣ Start Tor (system service)

If you installed via apt/dnf, Tor will already run on startup.

To verify:

```bash
sudo systemctl status tor
```

If it’s not running:

```bash
sudo systemctl start tor
sudo systemctl enable tor
```

---

# ✅ 6️⃣ Test the crawler and scooper

Run a manual test first:

```bash
cd /opt/paritybit-crawler
source venv/bin/activate

python crawler.py --single-run
python scooper.py --once
```

✅ You should see:

* Crawling logs.
* `run_complete.flag` and `results.json` created.

---


# ✅ 9️⃣ Verify everything

On the VPS, you should see:

```
/opt/paritybit-crawler/
├─ crawler.py
├─ scooper.py
├─ sites.yaml
├─ keywords.txt
├─ Source/
├─ dead_sites.json
├─ run_complete.flag
├─ results.json
└─ venv/
```

# 🧠 TL;DR summary

| Step | Action                               |
| ---- | ------------------------------------ |
| 1    | Install Tor & Python on VPS          |
| 2    | Copy `/opt/paritybit-crawler` folder |
| 3    | Recreate venv & install deps         |
| 4    | Test manually                        |
---

# To run both Crawler and Scooper

Crawler
```
source venv/bin/activate
python crawler.py --single-run   #for single run
python crawler.py --interval-minutes 60   #for cyclic run
```

Scooper (in a seperate terminal)
```
source venv/bin/activate
python scooper.py --once   #for single run
python scooper.py --watch --poll-interval 10   #for cyclic run, it watches when crawler has completed a cycle.
```
