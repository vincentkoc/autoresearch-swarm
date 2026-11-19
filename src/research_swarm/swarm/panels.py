"""Calibration, exposure accounting and held-out decisions for one campaign."""

from __future__ import annotations

import json
import math
import sqlite3
import statistics
import time
import warnings
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import Literal

from filelock import FileLock

Role = Literal["search", "selection", "confirmation", "calibration_proxy", "calibration_target"]
Evaluate = Callable[[str, Role, tuple[str, ...], int], float]


@dataclass(frozen=True)
class PanelSplit:
    """Use stable subject or example IDs from the actual dataset manifest."""

    train: tuple[str, ...]
    search: tuple[str, ...]
    selection: tuple[str, ...]
    confirmation: tuple[str, ...]
    calibration_proxy: tuple[str, ...]
    calibration_target: tuple[str, ...]

    def __post_init__(self) -> None:
        seen: dict[str, str] = {}
        for field in fields(self):
            members = getattr(self, field.name)
            if not isinstance(members, tuple) or (not members and field.name != "train"):
                raise ValueError(f"{field.name} requires a tuple of member IDs")
            for member in members:
                if not isinstance(member, str) or not member.strip():
                    raise ValueError(f"invalid member ID in {field.name}")
                if member in seen:
                    raise ValueError(
                        f"member {member!r} occurs in both {seen[member]} and {field.name}"
                    )
                seen[member] = field.name

    def members(self, role: Role) -> tuple[str, ...]:
        members: tuple[str, ...] = getattr(self, role)
        return members


@dataclass(frozen=True)
class Calibration:
    slope: float | None
    intercept: float | None
    seed_noise: float
    trials: int
    seconds_per_trial: float
    trials_per_second: float


@dataclass(frozen=True)
class Decision:
    promoted: bool
    reason: str
    selection_gain: float | None = None
    confirmation_gain: float | None = None
    error: str | None = None


class PanelEvaluationError(ValueError):
    """A reserved panel look failed to produce a usable score."""


class PanelStudy:
    """Use one SQLite file for a study. Reopen it to resume the same panel protocol."""

    def __init__(
        self,
        path: Path,
        *,
        panels: PanelSplit,
        variants: Sequence[str],
        seeds: Sequence[int],
        evaluate: Evaluate,
        direction: Literal["maximize", "minimize"],
        revision: str,
        look_limits: Mapping[Role, int],
        min_gain: float = 0.0,
        noise_multiplier: float = 1.0,
    ) -> None:
        self.path = Path(path)
        self.panels = panels
        self.variants = tuple(variants)
        self.seeds = tuple(seeds)
        self.evaluate = evaluate
        self.direction = direction
        self.look_limits = dict(look_limits)
        self.min_gain = min_gain
        self.noise_multiplier = noise_multiplier
        if len(self.variants) < 3 or len(set(self.variants)) != len(self.variants):
            raise ValueError("calibrate the baseline and at least two distinct variants")
        if len(self.seeds) < 2 or len(set(self.seeds)) != len(self.seeds):
            raise ValueError("calibration needs distinct replicate seeds")
        if direction not in ("maximize", "minimize") or not revision.strip():
            raise ValueError("set the metric direction and scoring revision")
        if not all(math.isfinite(x) and x >= 0 for x in (min_gain, noise_multiplier)):
            raise ValueError("gain and noise thresholds must be finite and nonnegative")
        roles = {field.name for field in fields(panels)} - {"train"}
        if any(k not in roles or type(v) is not int or v < 1 for k, v in self.look_limits.items()):
            raise ValueError("look limits require a panel role and a positive count")
        config = json.dumps(
            {
                "panels": asdict(panels),
                "variants": self.variants,
                "seeds": self.seeds,
                "direction": direction,
                "revision": revision,
                "look_limits": self.look_limits,
                "min_gain": min_gain,
                "noise_multiplier": noise_multiplier,
            },
            sort_keys=True,
        )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = FileLock(str(self.path) + ".lock")
        with self._db() as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS panel_state (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
            )
            db.execute("""CREATE TABLE IF NOT EXISTS panel_looks (
                id INTEGER PRIMARY KEY, role TEXT NOT NULL, candidate TEXT NOT NULL,
                seed INTEGER NOT NULL, score REAL)""")
            db.execute("""CREATE UNIQUE INDEX IF NOT EXISTS confirm_once ON panel_looks(candidate)
                          WHERE role = 'confirmation'""")
            db.execute("INSERT OR IGNORE INTO panel_state VALUES ('config', ?)", (config,))
            if (
                db.execute("SELECT value FROM panel_state WHERE key = 'config'").fetchone()[0]
                != config
            ):
                raise ValueError("study configuration changed; use a new study file and calibrate")

    @contextmanager
    def _db(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(self.path, timeout=30)
        try:
            with db:
                yield db
        finally:
            db.close()

    def _state(self, key: str) -> str | None:
        with self._db() as db:
            row = db.execute("SELECT value FROM panel_state WHERE key = ?", (key,)).fetchone()
            return str(row[0]) if row is not None else None

    def _put(self, key: str, value: Calibration | Decision) -> None:
        with self._db() as db:
            db.execute(
                "INSERT INTO panel_state VALUES (?, ?)",
                (key, json.dumps(asdict(value), allow_nan=False)),
            )

    def _reserve(self, role: Role, key: str, seed: int, replay_key: str | None = None) -> int:
        with self._db() as db:
            row = db.execute(
                "INSERT INTO panel_looks(role, candidate, seed) VALUES (?, ?, ?) RETURNING id",
                (role, key, seed),
            ).fetchone()
            assert row is not None
            look_id = int(row[0])
            if replay_key is not None:
                db.execute("INSERT INTO panel_state VALUES (?, ?)", (replay_key, str(look_id)))
            limit = self.look_limits.get(role)
            if limit is not None:
                count = db.execute(
                    "SELECT count(*) FROM panel_looks WHERE role = ?", (role,)
                ).fetchone()[0]
        if limit is not None and count == limit + 1:
            warnings.warn(
                f"{role} panel exceeded its {limit}-look budget", UserWarning, stacklevel=3
            )
        return look_id

    def _measure(
        self, role: Role, key: str, seed: int, *, comparison: tuple[str, str] | None = None
    ) -> float:
        replay_key = None
        if comparison is not None:
            # The comparison lock spans lookup, reservation and evaluation. Store the
            # reservation with the look so a restart can distinguish it from a new pair.
            replay_key = json.dumps([role, *comparison, key, seed])
            with self._db() as db:
                row = db.execute(
                    """SELECT l.score FROM panel_state s JOIN panel_looks l ON l.id = s.value
                       WHERE s.key = ?""",
                    (replay_key,),
                ).fetchone()
            if row is not None:
                if row[0] is None:
                    raise PanelEvaluationError(f"{role} look consumed without a result")
                return float(row[0])
        look_id = self._reserve(role, key, seed, replay_key)
        try:
            score = float(self.evaluate(key, role, self.panels.members(role), seed))
        except Exception as exc:
            raise PanelEvaluationError(f"{role}: {type(exc).__name__}: {exc}") from exc
        if not math.isfinite(score):
            raise PanelEvaluationError(f"non-finite {role} score")
        with self._db() as db:
            db.execute("UPDATE panel_looks SET score = ? WHERE id = ?", (score, look_id))
        return score

    def look_counts(self) -> dict[str, int]:
        with self._db() as db:
            return dict(db.execute("SELECT role, count(*) FROM panel_looks GROUP BY role"))

    def calibrate(self) -> Calibration:
        """Pair proxy and target measurements by recipe and seed before dispatch."""
        with self.lock:
            previous = self._state("calibration")
            if previous is not None:
                return Calibration(**json.loads(previous))
            start = time.perf_counter()
            proxy_means, target_means, target_variances = [], [], []
            for key in self.variants:
                proxy, target = [], []
                for seed in self.seeds:
                    proxy.append(self._measure("calibration_proxy", key, seed))
                    target.append(self._measure("calibration_target", key, seed))
                proxy_means.append(statistics.mean(proxy))
                target_means.append(statistics.mean(target))
                target_variances.append(statistics.variance(target))
            trials = 2 * len(self.variants) * len(self.seeds)
            elapsed = time.perf_counter() - start
            slope = intercept = None
            if len(set(proxy_means)) > 1:
                slope, intercept = statistics.linear_regression(proxy_means, target_means)
            result = Calibration(
                slope,
                intercept,
                math.sqrt(statistics.mean(target_variances)),
                trials,
                elapsed / trials,
                trials / elapsed,
            )
            self._put("calibration", result)
            return result

    def _calibration(self) -> Calibration:
        value = self._state("calibration")
        if value is None:
            raise RuntimeError("calibrate before search or selection")
        return Calibration(**json.loads(value))

    def record_search(self, candidate_key: str, seed: int) -> None:
        """Reserve an exposure before handing search cases to an external worker."""
        self._calibration()
        self._reserve("search", candidate_key, seed)

    def search(self, candidate_key: str, seed: int) -> float:
        self._calibration()
        return self._measure("search", candidate_key, seed)

    def confirmed_score(self, key: str) -> float | None:
        """Read a previously measured score without opening the confirmation panel."""
        with self._db() as db:
            row = db.execute(
                "SELECT score FROM panel_looks WHERE role = 'confirmation' AND candidate = ?",
                (key,),
            ).fetchone()
        return float(row[0]) if row is not None and row[0] is not None else None

    def _confirmed(self, key: str) -> float:
        with self._db() as db:
            row = db.execute(
                "SELECT score FROM panel_looks WHERE role = 'confirmation' AND candidate = ?",
                (key,),
            ).fetchone()
        if row is None:
            return self._measure("confirmation", key, self.seeds[0])
        if row[0] is None:
            raise PanelEvaluationError("confirmation look consumed without a result")
        return float(row[0])

    def consider(self, challenger_key: str, incumbent_key: str | None) -> Decision:
        """Only a gain on selection and confirmation can replace the reference."""
        calibration = self._calibration()
        reference = self.variants[0] if incumbent_key is None else incumbent_key
        decision_key = json.dumps(["consider", challenger_key, incumbent_key])
        sign = 1 if self.direction == "maximize" else -1
        with self.lock:
            previous = self._state(decision_key)
            if previous is not None:
                return Decision(**json.loads(previous))
            try:
                if challenger_key == reference:
                    self._confirmed(reference)
                    result = Decision(incumbent_key is None, "reference")
                else:
                    result = self._compare(challenger_key, reference, sign, calibration)
            except PanelEvaluationError as exc:
                result = Decision(False, "evaluation_failed", error=str(exc))
            self._put(decision_key, result)
            return result

    def _compare(self, key: str, reference: str, sign: int, calibration: Calibration) -> Decision:
        pair = (key, reference)
        # Older interrupted studies may have consumed confirmation before they
        # recorded which comparison opened the selection looks.
        with self._db() as db:
            unfinished = db.execute(
                """SELECT 1 FROM panel_looks WHERE role = 'confirmation' AND score IS NULL
                   AND candidate IN (?, ?) LIMIT 1""",
                pair,
            ).fetchone()
        if unfinished is not None:
            raise PanelEvaluationError("confirmation look consumed without a result")
        gains = [
            sign
            * (
                self._measure("selection", key, seed, comparison=pair)
                - self._measure("selection", reference, seed, comparison=pair)
            )
            for seed in self.seeds
        ]
        selection_gain = statistics.mean(gains)
        threshold = max(
            self.min_gain,
            self.noise_multiplier * calibration.seed_noise * math.sqrt(2 / len(gains)),
        )
        if selection_gain <= threshold:
            return Decision(False, "selection", selection_gain)
        gain = sign * (self._confirmed(key) - self._confirmed(reference))
        return Decision(gain > self.min_gain, "confirmation", selection_gain, gain)
