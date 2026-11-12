import sys
SESSION = sys.argv[1]
import json, os, subprocess
pane = int(subprocess.run(["tmux","list-panes","-t",SESSION,"-F","#{pane_pid}"],capture_output=True,text=True).stdout.split()[0])
rows = []
for pid in os.listdir("/proc"):
    if not pid.isdigit(): continue
    try:
        stat = open(f"/proc/{pid}/stat").read(); ppid = int(stat.rsplit(")",1)[1].split()[1])
        cmd = open(f"/proc/{pid}/cmdline","rb").read().replace(b"\0",b" ").decode("latin1").strip()
    except OSError: continue
    rows.append({"pid":int(pid),"ppid":ppid,"command":cmd})
kids = {}
for r in rows: kids.setdefault(r["ppid"],[]).append(r)
out, q = [], [r for r in rows if r["pid"]==pane]
while q:
    r = q.pop(0); out.append(r); q += kids.get(r["pid"],[])
for r in out:
    if r["command"].split(" ")[0].endswith("/claude"): r["command"] = r["command"].split(" ")[0] + " [args omitted]"
    if r["command"].startswith("/bin/bash -c source"): r["command"] = r["command"][:400]
print(json.dumps({"session":SESSION,"panePid":pane,"rows":sorted(out,key=lambda r:r["pid"])},indent=1))
