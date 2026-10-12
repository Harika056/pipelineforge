import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _run(*args):
    return subprocess.run([sys.executable, *args], cwd=ROOT, capture_output=True, text=True, timeout=300)


def test_demo_runs_end_to_end(tmp_path):
    r = _run("-m", "pipelineforge.run", "demo", "--dir", str(tmp_path), "--orders", "300")
    assert r.returncode == 0, r.stderr
    assert "batch 1 (full load)" in r.stdout and "batch 2 (incremental)" in r.stdout
    assert "top customers by revenue" in r.stdout


def test_batch_and_cdc_commands(tmp_path):
    from pipelineforge.cdc import make_event
    import json
    from pipelineforge.generate import generate
    oltp, wh = str(tmp_path / "o.db"), str(tmp_path / "w.db")
    generate(oltp, orders=50, dirty_rate=0.0)
    r = _run("-m", "pipelineforge.run", "batch", "--oltp", oltp, "--warehouse", wh)
    assert r.returncode == 0, r.stderr
    ev = tmp_path / "events.jsonl"
    row = {"order_id": 1, "customer_id": 1, "status": "PAID", "order_ts": "2025-01-02T03:04:05",
           "updated_at": "2031-01-01T00:00:00"}
    ev.write_text(json.dumps(make_event("orders", "u", None, row, 999)) + "\n")
    r = _run("-m", "pipelineforge.run", "cdc", "--events", str(ev), "--warehouse", wh)
    assert r.returncode == 0, r.stderr
    assert '"applied": 1' in r.stdout


def test_benchmark_script_small(tmp_path):
    r = _run("-m", "scripts.benchmark", "--orders", "300", "--repeats", "1")
    assert r.returncode == 0, r.stderr
    assert "median throughput" in r.stdout
