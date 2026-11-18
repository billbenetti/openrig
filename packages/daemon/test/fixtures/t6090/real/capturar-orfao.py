import json, os, subprocess, sys
# Processes whose program is suite-flock or congelar-eval and that no longer descend from any tmux pane (their parent
# chain reaches pid 1 or the user's systemd without a pane pid), with the TMUX_PANE they inherited and the session that
# owns that pane now. Only pid, ppid, command (cut) and TMUX_PANE: no other environment variable.
panes = {}
for line in subprocess.run(["tmux", "list-panes", "-a", "-F", "#{pane_id} #{pane_pid} #{session_name}"], capture_output=True, text=True).stdout.splitlines():
    pid_, ppid_, sess = line.split(" ", 2)
    panes[int(ppid_)] = (pid_, sess)
pane_by_id = {v[0]: v[1] for v in panes.values()}
rows = {}
for pid in os.listdir("/proc"):
    if not pid.isdigit(): continue
    try:
        stat = open(f"/proc/{pid}/stat").read(); ppid = int(stat.rsplit(")", 1)[1].split()[1])
        cmd = open(f"/proc/{pid}/cmdline", "rb").read().replace(b"\0", b" ").decode("latin1").strip()
    except OSError: continue
    rows[int(pid)] = {"pid": int(pid), "ppid": ppid, "command": cmd[:200]}
def under_pane(pid):
    seen = set()
    while pid in rows and pid not in seen:
        if pid in panes: return True
        seen.add(pid); pid = rows[pid]["ppid"]
    return False
out = []
for r in rows.values():
    toks = r["command"].split()
    if not any(t.endswith("/suite-flock") or t.endswith("/congelar-eval") for t in toks[:3]): continue
    if under_pane(r["pid"]): continue
    try:
        env = open(f"/proc/{r['pid']}/environ", "rb").read().decode("latin1").split("\0")
    except OSError: continue
    tp = next((e.split("=", 1)[1] for e in env if e.startswith("TMUX_PANE=")), None)
    chain, p = [], r["ppid"]
    while p in rows and len(chain) < 6:
        chain.append(rows[p]); p = rows[p]["ppid"]
    out.append({"process": r, "parents": chain, "tmuxPane": tp, "paneSessionNow": pane_by_id.get(tp)})
print(json.dumps(out[:3], indent=1))
