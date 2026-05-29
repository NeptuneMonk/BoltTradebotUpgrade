"""
Simple rule-based pattern classifier for new Pump.fun launches.
Inputs: early-window metrics (first ~10 seconds)
Output: action + risk score (0..100, lower = safer)
"""
from typing import Literal

Action = Literal["exit_early", "hold_briefly", "abort_trade"]


def default_rules() -> dict:
    return {
        # If curve fills >X% in 10s --> exit_early (it's a fast pump, likely dump soon)
        "fast_curve_fill_pct": 30.0,
        "fast_curve_window_s": 10,
        # If unique buyers > X in 5s --> hold_briefly (real interest)
        "many_buyers_count": 15,
        "many_buyers_window_s": 5,
        # If SOL inflow < X in 8s --> abort_trade
        "low_inflow_sol": 0.5,
        "low_inflow_window_s": 8,
        # If creator wallet has > X rugs in history --> abort
        "creator_rug_threshold": 1,
    }


def classify(metrics: dict, rules: dict) -> dict:
    """
    metrics keys:
      - curve_fill_pct: float
      - elapsed_s: float
      - unique_buyers: int
      - sol_inflow: float
      - creator_rugs: int (defaults 0 if unknown)
      - social_score: int (0..100; optional)
    """
    elapsed = metrics.get("elapsed_s", 0)
    curve_pct = metrics.get("curve_fill_pct", 0)
    buyers = metrics.get("unique_buyers", 0)
    inflow = metrics.get("sol_inflow", 0)
    rugs = metrics.get("creator_rugs", 0)
    social = metrics.get("social_score", 0)

    reasons: list[str] = []
    action: Action = "hold_briefly"
    risk = 50  # baseline

    # Abort conditions
    # NOTE: rugs > 0 guard is critical — without it, a misconfigured
    # `creator_rug_threshold = 0` would abort EVERY trade because 0 >= 0.
    # The semantic is "creator has rugged BEFORE", so 0 rugs must never abort
    # regardless of how the threshold is set in the UI.
    if rugs > 0 and rugs >= max(1, rules["creator_rug_threshold"]):
        action = "abort_trade"
        risk = 95
        reasons.append(f"creator has {rugs} prior rugs")
        return {"action": action, "risk": risk, "reasons": reasons}

    # Social trending gate (only if rule enabled, i.e. > 0)
    social_min = rules.get("social_score_min", 0)
    if social_min and social < social_min:
        action = "abort_trade"
        risk = 85
        reasons.append(f"social score {social} < min {social_min} (not trending)")
        return {"action": action, "risk": risk, "reasons": reasons}

    if elapsed >= rules["low_inflow_window_s"] and inflow < rules["low_inflow_sol"]:
        action = "abort_trade"
        risk = 80
        reasons.append(f"low SOL inflow ({inflow:.2f} SOL in {elapsed:.0f}s)")
        return {"action": action, "risk": risk, "reasons": reasons}

    # Exit-early: fast curve fill
    if (
        elapsed <= rules["fast_curve_window_s"]
        and curve_pct >= rules["fast_curve_fill_pct"]
    ):
        action = "exit_early"
        risk = 70
        reasons.append(f"curve filled {curve_pct:.1f}% in {elapsed:.0f}s (fast pump)")
        return {"action": action, "risk": risk, "reasons": reasons}

    # Hold briefly: many buyers
    if (
        elapsed <= rules["many_buyers_window_s"]
        and buyers >= rules["many_buyers_count"]
    ):
        action = "hold_briefly"
        risk = 35
        reasons.append(f"{buyers} unique buyers in {elapsed:.0f}s")
        return {"action": action, "risk": risk, "reasons": reasons}

    # Default = hold briefly with neutral risk
    reasons.append("baseline rules — no strong signal")
    # Adjust risk by inflow + social
    if inflow > 1.0:
        risk = max(20, risk - 15)
    elif inflow < 0.2:
        risk = min(85, risk + 15)
    if social >= 50:
        risk = max(15, risk - 10)
    return {"action": action, "risk": risk, "reasons": reasons}
