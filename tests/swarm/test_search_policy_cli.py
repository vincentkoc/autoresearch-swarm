"""Exercise the configured portfolio through the real queue and worker CLI."""

import json
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


def test_hung_model_keeps_cli_dispatching_and_selecting_replicates(tmp_path: Path) -> None:
    contacted = threading.Event()
    release = threading.Event()

    class StuckModel(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            self.rfile.read(int(self.headers["Content-Length"]))
            contacted.set()
            release.wait()

    server = ThreadingHTTPServer(("127.0.0.1", 0), StuckModel)
    serving = threading.Thread(target=server.serve_forever, daemon=True)
    serving.start()
    command = (
        "import json,os; from pathlib import Path;"
        "trial=json.loads(Path(os.environ['SWARM_TRIAL']).read_text());"
        "x=int(trial['candidate']['params']['x']);"
        "Path(os.environ['SWARM_METRICS']).write_text(json.dumps({'accuracy': x}))"
    )
    spec = {
        "campaign_id": "timeout",
        "metric": "accuracy",
        "direction": "maximize",
        "evaluation_mode": "smoke",
        "command": [sys.executable, "-c", command],
        "candidates": [{"candidate_id": "initial", "params": {"x": "0"}}],
        "max_trials": 10,
        "max_workers": 3,
        "proposal_timeout_seconds": 0.75,
        "search_space": {"x": ["0", "1", "2", "3", "4", "5"]},
        "proposer": "research_swarm.swarm.operators.config:from_spec",
        "selection_policy": "research_swarm.swarm.operators.config:selection_from_spec",
        "options": {
            "search": {
                "operators": ["llm-choice-rewrite", "param-mutate"],
                "model": "fixture",
                "port": server.server_port,
                "objective": "maximize accuracy",
                "min_replicates": 2,
            }
        },
    }
    path = tmp_path / "campaign.yaml"
    path.write_text(json.dumps(spec), encoding="utf-8")
    try:
        cli = str(Path(sys.executable).parent / "research")
        process = subprocess.run(
            [cli, "swarm", "run", str(path)], timeout=20, text=True, capture_output=True
        )
        assert process.returncode == 0, process.stdout + process.stderr
        assert contacted.is_set()
        result = json.loads(process.stdout)
        assert result["trials"] == result["attempts"] == spec["max_trials"]
        assert result["failed"] == 0 and result["selected"]["replicates"] >= 2
        assert result["selected"]["score"] > 0
    finally:
        release.set()
        server.shutdown()
        server.server_close()
        serving.join()
