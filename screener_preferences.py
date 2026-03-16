#!/usr/bin/env python3
"""
Investment Preferences Screener
================================
Multi-step wizard that maps investor preferences to a model ETF portfolio.

Exports:
  SCREENER_HTML             – the wizard page (served at /screener/preferences)
  build_portfolio(prefs, db_path) → dict  – portfolio construction logic
"""

import sqlite3
import json

# ─────────────────────────────────────────────────────────────────────────────
#  Model Portfolios  (base allocations by risk level 1=very conservative … 5=aggressive)
# ─────────────────────────────────────────────────────────────────────────────

_BASE = {
    1: {"Cash": 20, "Fixed Income": 55, "Australian Equities": 5,
        "International Equities": 5, "Property": 10, "Infrastructure": 5},
    2: {"Cash": 5, "Fixed Income": 45, "Australian Equities": 15,
        "International Equities": 15, "Property": 10, "Infrastructure": 10},
    3: {"Fixed Income": 30, "Australian Equities": 20,
        "International Equities": 25, "Property": 10, "Infrastructure": 10,
        "Commodities": 5},
    4: {"Fixed Income": 20, "Australian Equities": 30,
        "International Equities": 35, "Property": 10, "Infrastructure": 5},
    5: {"Fixed Income": 10, "Australian Equities": 35,
        "International Equities": 45, "Commodities": 5, "Digital Assets": 5},
}

_RISK_LABELS = {
    1: "Very Conservative",
    2: "Conservative",
    3: "Balanced",
    4: "Growth",
    5: "Aggressive Growth",
}

_RISK_DESC = {
    1: "Capital preservation with minimal volatility. Heavily weighted to cash and bonds with very limited equity exposure.",
    2: "Modest growth with low volatility. Predominantly bonds and defensive assets with some equity exposure.",
    3: "A balanced blend of growth and stability. Equal parts equities and defensive assets for medium-term investors.",
    4: "Long-term wealth accumulation. Majority in equities with some defensive exposure to manage volatility.",
    5: "Maximum long-term growth. Predominantly equities across all markets, suited to investors comfortable with significant volatility.",
}

_AC_COLORS = {
    "Australian Equities":    "#3b82f6",
    "International Equities": "#8b5cf6",
    "Fixed Income":           "#10b981",
    "Property":               "#f59e0b",
    "Infrastructure":         "#06b6d4",
    "Cash":                   "#6b7280",
    "Commodities":            "#ef4444",
    "Digital Assets":         "#f97316",
    "Diversified":            "#84cc16",
}


# ─────────────────────────────────────────────────────────────────────────────
#  Portfolio construction
# ─────────────────────────────────────────────────────────────────────────────

def _calc_risk(prefs: dict) -> int:
    score = 0
    score += {"growth": 2, "balanced": 1, "income": 0, "preservation": -1}.get(prefs.get("goal", "balanced"), 0)
    score += {"long": 2, "medium": 1, "short": -1}.get(prefs.get("horizon", "medium"), 0)
    score += {"aggressive": 2, "moderate": 1, "conservative": -1}.get(prefs.get("risk", "moderate"), 0)
    if score <= 0:  return 1
    if score <= 2:  return 2
    if score <= 4:  return 3
    if score == 5:  return 4
    return 5


def _adjust_allocations(allocs: dict, prefs: dict) -> dict:
    a = dict(allocs)

    # Geography tilt
    geo = prefs.get("geography", "mix")
    au = a.get("Australian Equities", 0)
    intl = a.get("International Equities", 0)
    if geo == "australia" and intl > 0:
        shift = intl // 2
        a["International Equities"] = intl - shift
        a["Australian Equities"] = au + shift
    elif geo == "global" and au > 0:
        shift = au // 2
        a["Australian Equities"] = au - shift
        a["International Equities"] = intl + shift

    # Income tilt: bump Fixed Income, trim equities
    if prefs.get("income") == "high":
        fi = a.get("Fixed Income", 0)
        shift = 10
        a["Fixed Income"] = fi + shift
        total_eq = a.get("Australian Equities", 0) + a.get("International Equities", 0)
        if total_eq > 0:
            au_frac = a.get("Australian Equities", 0) / max(total_eq, 1)
            a["Australian Equities"] = max(0, a.get("Australian Equities", 0) - round(shift * au_frac))
            a["International Equities"] = max(0, a.get("International Equities", 0) - round(shift * (1 - au_frac)))

    return {k: v for k, v in a.items() if v > 0}


def _sub_where(asset_class: str, prefs: dict) -> tuple:
    """Return (extra_where_clause, extra_params) for sub_category preference."""
    esg  = prefs.get("esg", "none")
    inc  = prefs.get("income", "none")
    geo  = prefs.get("geography", "mix")

    if esg in ("prefer", "essential"):
        return "AND sub_category = 'ESG & Responsible'", []

    if asset_class == "Australian Equities":
        if inc in ("moderate", "high"):
            return "AND sub_category IN ('Broad Market','Dividend & Income')", []
        return "AND sub_category = 'Broad Market'", []

    if asset_class == "International Equities":
        return "AND sub_category IN ('Developed Markets','US Market')", []

    if asset_class == "Fixed Income":
        return "AND sub_category IN ('Australian Diversified','Global Diversified','Australian Government','Global Government')", []

    if asset_class == "Property":
        if geo == "australia":
            return "AND sub_category = 'Australian REITs'", []
        if geo == "global":
            return "AND sub_category = 'Global REITs'", []
        return "", []

    return "", []


def _select_etfs(asset_class: str, prefs: dict, conn) -> list:
    sub_clause, sub_params = _sub_where(asset_class, prefs)
    fee_clause = ""
    if prefs.get("fees") == "high":   # high = minimize fees
        fee_clause = "AND (expense_ratio IS NULL OR expense_ratio < 0.5)"

    sql = f"""
        SELECT code, name, expense_ratio, fund_size_aud_millions,
               distribution_yield, sub_category, summary
        FROM etfs
        WHERE asset_class = ?
          AND (fund_size_aud_millions IS NULL OR fund_size_aud_millions > 50)
          {sub_clause}
          {fee_clause}
        ORDER BY COALESCE(fund_size_aud_millions, 0) DESC
        LIMIT 3
    """
    try:
        rows = conn.execute(sql, [asset_class] + sub_params).fetchall()
    except Exception:
        rows = []

    # Fallback: ignore sub_category filter
    if not rows:
        sql2 = f"""
            SELECT code, name, expense_ratio, fund_size_aud_millions,
                   distribution_yield, sub_category, summary
            FROM etfs
            WHERE asset_class = ? {fee_clause}
            ORDER BY COALESCE(fund_size_aud_millions, 0) DESC
            LIMIT 3
        """
        try:
            rows = conn.execute(sql2, [asset_class]).fetchall()
        except Exception:
            rows = []

    return [dict(r) for r in rows[:2]]


def build_portfolio(prefs: dict, db_path: str) -> dict:
    risk_level = _calc_risk(prefs)
    allocs = _adjust_allocations(_BASE[risk_level], prefs)

    # Normalise to exactly 100 %
    total = sum(allocs.values())
    if total != 100 and total > 0:
        allocs = {k: round(v * 100 / total) for k, v in allocs.items()}
        diff = 100 - sum(allocs.values())
        if diff and allocs:
            biggest = max(allocs, key=lambda k: allocs[k])
            allocs[biggest] += diff

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    holdings = []
    try:
        for asset_class, alloc_pct in sorted(allocs.items(), key=lambda x: -x[1]):
            if alloc_pct <= 0:
                continue
            etfs = _select_etfs(asset_class, prefs, conn)
            if not etfs:
                continue

            # Split alloc across up to 2 ETFs (60/40)
            if len(etfs) == 1:
                splits = [alloc_pct]
            else:
                primary = round(alloc_pct * 0.6)
                splits = [primary, alloc_pct - primary]

            for i, etf in enumerate(etfs[:len(splits)]):
                summary = {}
                if etf.get("summary"):
                    try:
                        summary = json.loads(etf["summary"])
                    except Exception:
                        pass
                holdings.append({
                    "asset_class": asset_class,
                    "allocation_pct": splits[i],
                    "code": etf["code"],
                    "name": etf["name"],
                    "expense_ratio": etf.get("expense_ratio"),
                    "fund_size_aud_millions": etf.get("fund_size_aud_millions"),
                    "distribution_yield": etf.get("distribution_yield"),
                    "sub_category": etf.get("sub_category"),
                    "description": summary.get("description", ""),
                    "suitable_for": summary.get("suitable_for", ""),
                    "color": _AC_COLORS.get(asset_class, "#94a3b8"),
                })
    finally:
        conn.close()

    # Re-sum actual allocations (some may have been skipped if no ETFs found)
    actual_total = sum(h["allocation_pct"] for h in holdings)

    return {
        "risk_level": risk_level,
        "risk_label": _RISK_LABELS[risk_level],
        "risk_description": _RISK_DESC[risk_level],
        "allocations": allocs,
        "holdings": holdings,
        "total_pct": actual_total,
        "preferences": prefs,
    }


# ─────────────────────────────────────────────────────────────────────────────
#  Wizard HTML
# ─────────────────────────────────────────────────────────────────────────────

SCREENER_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Portfolio Builder — Australian ETF Platform</title>
<script src="https://cdn.tailwindcss.com"></script>
<style>
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; }
  .step { display: none; }
  .step.active { display: block; animation: fadeIn .25s ease; }
  @keyframes fadeIn { from { opacity:0; transform:translateY(8px) } to { opacity:1; transform:translateY(0) } }
  .opt-card {
    border: 2px solid #e5e7eb; border-radius: 12px; padding: 16px 20px;
    cursor: pointer; transition: all .15s; background: #fff;
    display: flex; align-items: flex-start; gap: 14px;
  }
  .opt-card:hover { border-color: #93c5fd; background: #eff6ff; }
  .opt-card.selected { border-color: #2563eb; background: #eff6ff; }
  .opt-icon { font-size: 24px; line-height: 1; flex-shrink: 0; margin-top: 2px; }
  .opt-title { font-weight: 600; color: #111827; font-size: 15px; }
  .opt-desc { color: #6b7280; font-size: 13px; margin-top: 2px; }
  .progress-step {
    width: 28px; height: 28px; border-radius: 50%; border: 2px solid #e5e7eb;
    display: flex; align-items: center; justify-content: center;
    font-size: 12px; font-weight: 600; color: #9ca3af; background: #fff;
    transition: all .2s;
  }
  .progress-step.done { background: #2563eb; border-color: #2563eb; color: #fff; }
  .progress-step.current { border-color: #2563eb; color: #2563eb; }
  .progress-line { flex: 1; height: 2px; background: #e5e7eb; transition: background .2s; }
  .progress-line.done { background: #2563eb; }
  .alloc-bar { height: 12px; border-radius: 6px; transition: width .5s ease; }
  .etf-card {
    background: #fff; border: 1px solid #e5e7eb; border-radius: 12px;
    padding: 16px; transition: box-shadow .15s;
  }
  .etf-card:hover { box-shadow: 0 4px 12px rgba(0,0,0,.08); }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 20px; font-size: 11px; font-weight: 600; }
</style>
</head>
<body class="bg-slate-50 min-h-screen">

<!-- Header -->
<header class="bg-gradient-to-r from-slate-900 to-slate-800 text-white py-3 px-5 flex items-center justify-between sticky top-0 z-10 shadow-lg">
  <div class="flex items-center gap-3">
    <a href="/dashboard" class="text-white font-bold text-lg tracking-tight hover:text-blue-300 transition-colors">
      <span class="text-blue-400">AU</span>ETF
    </a>
    <span class="text-white/30">|</span>
    <span class="text-white/80 text-sm">Portfolio Builder</span>
  </div>
  <div class="flex items-center gap-4 text-sm">
    <a href="/dashboard" class="text-white/70 hover:text-white transition-colors">Dashboard</a>
    <a href="/articles" class="text-white/70 hover:text-white transition-colors">Articles</a>
  </div>
</header>

<main class="max-w-2xl mx-auto px-4 py-8">

  <!-- Intro / Hero -->
  <div id="intro" class="text-center mb-8">
    <div class="inline-flex items-center gap-2 bg-blue-50 text-blue-700 px-3 py-1 rounded-full text-xs font-semibold mb-4">
      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>
      Personalised ETF Recommendations
    </div>
    <h1 class="text-3xl font-bold text-gray-900 mb-3">Build Your ETF Portfolio</h1>
    <p class="text-gray-500 text-base max-w-lg mx-auto">Answer 7 quick questions and we'll recommend a diversified ETF portfolio tailored to your goals and risk profile.</p>
  </div>

  <!-- Progress indicator -->
  <div id="progress-bar" class="flex items-center mb-8 hidden">
    <div class="progress-step current" id="ps-1">1</div>
    <div class="progress-line" id="pl-1"></div>
    <div class="progress-step" id="ps-2">2</div>
    <div class="progress-line" id="pl-2"></div>
    <div class="progress-step" id="ps-3">3</div>
    <div class="progress-line" id="pl-3"></div>
    <div class="progress-step" id="ps-4">4</div>
    <div class="progress-line" id="pl-4"></div>
    <div class="progress-step" id="ps-5">5</div>
    <div class="progress-line" id="pl-5"></div>
    <div class="progress-step" id="ps-6">6</div>
    <div class="progress-line" id="pl-6"></div>
    <div class="progress-step" id="ps-7">7</div>
  </div>

  <!-- ── STEP 1: Goal ── -->
  <div id="step-1" class="step active">
    <div class="mb-6">
      <p class="text-xs font-semibold text-blue-600 uppercase tracking-wider mb-1">Step 1 of 7</p>
      <h2 class="text-xl font-bold text-gray-900">What are you investing for?</h2>
      <p class="text-gray-500 text-sm mt-1">Your primary investment goal shapes the entire portfolio.</p>
    </div>
    <div class="grid gap-3">
      <div class="opt-card" onclick="pick('goal','growth',this)">
        <div class="opt-icon">🚀</div>
        <div><div class="opt-title">Build long-term wealth</div><div class="opt-desc">Maximise growth over many years — happy to ride out market cycles</div></div>
      </div>
      <div class="opt-card" onclick="pick('goal','balanced',this)">
        <div class="opt-icon">⚖️</div>
        <div><div class="opt-title">Balanced growth and income</div><div class="opt-desc">A mix of capital growth and regular income distributions</div></div>
      </div>
      <div class="opt-card" onclick="pick('goal','income',this)">
        <div class="opt-icon">💰</div>
        <div><div class="opt-title">Generate regular income</div><div class="opt-desc">Prioritise consistent dividend or distribution income</div></div>
      </div>
      <div class="opt-card" onclick="pick('goal','preservation',this)">
        <div class="opt-icon">🛡️</div>
        <div><div class="opt-title">Preserve capital</div><div class="opt-desc">Protect savings from inflation with minimal risk of loss</div></div>
      </div>
    </div>
    <div class="flex justify-end mt-6">
      <button onclick="nextStep()" id="btn-next-1" class="px-6 py-2.5 bg-blue-600 text-white rounded-lg font-semibold text-sm disabled:opacity-40 disabled:cursor-not-allowed hover:bg-blue-700 transition-colors" disabled>Next →</button>
    </div>
  </div>

  <!-- ── STEP 2: Horizon ── -->
  <div id="step-2" class="step">
    <div class="mb-6">
      <p class="text-xs font-semibold text-blue-600 uppercase tracking-wider mb-1">Step 2 of 7</p>
      <h2 class="text-xl font-bold text-gray-900">When do you plan to use this money?</h2>
      <p class="text-gray-500 text-sm mt-1">Longer time horizons allow for more growth-oriented allocations.</p>
    </div>
    <div class="grid gap-3">
      <div class="opt-card" onclick="pick('horizon','long',this)">
        <div class="opt-icon">🌳</div>
        <div><div class="opt-title">More than 7 years</div><div class="opt-desc">Long-term investing — retirement, wealth accumulation, next generation</div></div>
      </div>
      <div class="opt-card" onclick="pick('horizon','medium',this)">
        <div class="opt-icon">📅</div>
        <div><div class="opt-title">3 to 7 years</div><div class="opt-desc">Medium-term — home purchase, education funding, major milestone</div></div>
      </div>
      <div class="opt-card" onclick="pick('horizon','short',this)">
        <div class="opt-icon">⏱️</div>
        <div><div class="opt-title">Less than 3 years</div><div class="opt-desc">Short-term — need access to funds relatively soon</div></div>
      </div>
    </div>
    <div class="flex justify-between mt-6">
      <button onclick="prevStep()" class="px-5 py-2.5 text-gray-600 rounded-lg font-semibold text-sm hover:bg-gray-100 transition-colors">← Back</button>
      <button onclick="nextStep()" id="btn-next-2" class="px-6 py-2.5 bg-blue-600 text-white rounded-lg font-semibold text-sm disabled:opacity-40 disabled:cursor-not-allowed hover:bg-blue-700 transition-colors" disabled>Next →</button>
    </div>
  </div>

  <!-- ── STEP 3: Risk ── -->
  <div id="step-3" class="step">
    <div class="mb-6">
      <p class="text-xs font-semibold text-blue-600 uppercase tracking-wider mb-1">Step 3 of 7</p>
      <h2 class="text-xl font-bold text-gray-900">How would you react if your portfolio fell 20%?</h2>
      <p class="text-gray-500 text-sm mt-1">Be honest — your emotional response to losses matters more than your theoretical risk tolerance.</p>
    </div>
    <div class="grid gap-3">
      <div class="opt-card" onclick="pick('risk','aggressive',this)">
        <div class="opt-icon">💎</div>
        <div><div class="opt-title">I'd buy more — great buying opportunity</div><div class="opt-desc">Comfortable with high volatility and significant drawdowns</div></div>
      </div>
      <div class="opt-card" onclick="pick('risk','moderate',this)">
        <div class="opt-icon">😐</div>
        <div><div class="opt-title">I'd be concerned but stay the course</div><div class="opt-desc">Accepting of moderate volatility in pursuit of long-term returns</div></div>
      </div>
      <div class="opt-card" onclick="pick('risk','conservative',this)">
        <div class="opt-icon">😟</div>
        <div><div class="opt-title">I'd be very worried and consider selling</div><div class="opt-desc">Low tolerance for market volatility — capital protection is important</div></div>
      </div>
    </div>
    <div class="flex justify-between mt-6">
      <button onclick="prevStep()" class="px-5 py-2.5 text-gray-600 rounded-lg font-semibold text-sm hover:bg-gray-100 transition-colors">← Back</button>
      <button onclick="nextStep()" id="btn-next-3" class="px-6 py-2.5 bg-blue-600 text-white rounded-lg font-semibold text-sm disabled:opacity-40 disabled:cursor-not-allowed hover:bg-blue-700 transition-colors" disabled>Next →</button>
    </div>
  </div>

  <!-- ── STEP 4: Income ── -->
  <div id="step-4" class="step">
    <div class="mb-6">
      <p class="text-xs font-semibold text-blue-600 uppercase tracking-wider mb-1">Step 4 of 7</p>
      <h2 class="text-xl font-bold text-gray-900">How important is regular income?</h2>
      <p class="text-gray-500 text-sm mt-1">Regular distributions vs. reinvesting for growth — different strategies suit different needs.</p>
    </div>
    <div class="grid gap-3">
      <div class="opt-card" onclick="pick('income','high',this)">
        <div class="opt-icon">💵</div>
        <div><div class="opt-title">Very important — I need regular distributions</div><div class="opt-desc">Portfolio should prioritise income-generating ETFs with consistent yield</div></div>
      </div>
      <div class="opt-card" onclick="pick('income','moderate',this)">
        <div class="opt-icon">🔄</div>
        <div><div class="opt-title">Somewhat important — a nice bonus</div><div class="opt-desc">Income is welcome but not the primary objective</div></div>
      </div>
      <div class="opt-card" onclick="pick('income','none',this)">
        <div class="opt-icon">📈</div>
        <div><div class="opt-title">Not important — I prefer growth</div><div class="opt-desc">Reinvest everything for maximum long-term compounding</div></div>
      </div>
    </div>
    <div class="flex justify-between mt-6">
      <button onclick="prevStep()" class="px-5 py-2.5 text-gray-600 rounded-lg font-semibold text-sm hover:bg-gray-100 transition-colors">← Back</button>
      <button onclick="nextStep()" id="btn-next-4" class="px-6 py-2.5 bg-blue-600 text-white rounded-lg font-semibold text-sm disabled:opacity-40 disabled:cursor-not-allowed hover:bg-blue-700 transition-colors" disabled>Next →</button>
    </div>
  </div>

  <!-- ── STEP 5: ESG ── -->
  <div id="step-5" class="step">
    <div class="mb-6">
      <p class="text-xs font-semibold text-blue-600 uppercase tracking-wider mb-1">Step 5 of 7</p>
      <h2 class="text-xl font-bold text-gray-900">Do you want to invest responsibly?</h2>
      <p class="text-gray-500 text-sm mt-1">ESG and ethical ETFs screen out companies based on environmental, social and governance criteria.</p>
    </div>
    <div class="grid gap-3">
      <div class="opt-card" onclick="pick('esg','none',this)">
        <div class="opt-icon">📊</div>
        <div><div class="opt-title">No preference</div><div class="opt-desc">Standard index ETFs — broadest selection, typically lowest fees</div></div>
      </div>
      <div class="opt-card" onclick="pick('esg','prefer',this)">
        <div class="opt-icon">🌱</div>
        <div><div class="opt-title">I prefer ESG / ethical investments</div><div class="opt-desc">Favour responsible investing options where available</div></div>
      </div>
      <div class="opt-card" onclick="pick('esg','essential',this)">
        <div class="opt-icon">🌍</div>
        <div><div class="opt-title">ESG is essential to me</div><div class="opt-desc">Only include ETFs that screen for ESG or responsible criteria</div></div>
      </div>
    </div>
    <div class="flex justify-between mt-6">
      <button onclick="prevStep()" class="px-5 py-2.5 text-gray-600 rounded-lg font-semibold text-sm hover:bg-gray-100 transition-colors">← Back</button>
      <button onclick="nextStep()" id="btn-next-5" class="px-6 py-2.5 bg-blue-600 text-white rounded-lg font-semibold text-sm disabled:opacity-40 disabled:cursor-not-allowed hover:bg-blue-700 transition-colors" disabled>Next →</button>
    </div>
  </div>

  <!-- ── STEP 6: Geography ── -->
  <div id="step-6" class="step">
    <div class="mb-6">
      <p class="text-xs font-semibold text-blue-600 uppercase tracking-wider mb-1">Step 6 of 7</p>
      <h2 class="text-xl font-bold text-gray-900">Where do you want to invest?</h2>
      <p class="text-gray-500 text-sm mt-1">Geographic focus affects currency exposure and diversification.</p>
    </div>
    <div class="grid gap-3">
      <div class="opt-card" onclick="pick('geography','mix',this)">
        <div class="opt-icon">🌐</div>
        <div><div class="opt-title">A mix of Australia and global markets</div><div class="opt-desc">Balanced exposure — home bias with meaningful international diversification</div></div>
      </div>
      <div class="opt-card" onclick="pick('geography','australia',this)">
        <div class="opt-icon">🦘</div>
        <div><div class="opt-title">Mainly Australia</div><div class="opt-desc">Focus on ASX-listed companies — franking credits, no currency risk</div></div>
      </div>
      <div class="opt-card" onclick="pick('geography','global',this)">
        <div class="opt-icon">✈️</div>
        <div><div class="opt-title">Mainly global markets</div><div class="opt-desc">Broad global diversification — US, Europe, Asia and beyond</div></div>
      </div>
    </div>
    <div class="flex justify-between mt-6">
      <button onclick="prevStep()" class="px-5 py-2.5 text-gray-600 rounded-lg font-semibold text-sm hover:bg-gray-100 transition-colors">← Back</button>
      <button onclick="nextStep()" id="btn-next-6" class="px-6 py-2.5 bg-blue-600 text-white rounded-lg font-semibold text-sm disabled:opacity-40 disabled:cursor-not-allowed hover:bg-blue-700 transition-colors" disabled>Next →</button>
    </div>
  </div>

  <!-- ── STEP 7: Fees ── -->
  <div id="step-7" class="step">
    <div class="mb-6">
      <p class="text-xs font-semibold text-blue-600 uppercase tracking-wider mb-1">Step 7 of 7</p>
      <h2 class="text-xl font-bold text-gray-900">How important are low fees?</h2>
      <p class="text-gray-500 text-sm mt-1">Management expense ratios (MERs) compound over time — even 0.1% differences matter over decades.</p>
    </div>
    <div class="grid gap-3">
      <div class="opt-card" onclick="pick('fees','high',this)">
        <div class="opt-icon">✂️</div>
        <div><div class="opt-title">Very important — minimise fees</div><div class="opt-desc">Prefer the lowest-cost ETFs available, even if it limits choice</div></div>
      </div>
      <div class="opt-card" onclick="pick('fees','moderate',this)">
        <div class="opt-icon">🎯</div>
        <div><div class="opt-title">Moderately important</div><div class="opt-desc">Balance cost with quality — reasonable fees for a good product</div></div>
      </div>
      <div class="opt-card" onclick="pick('fees','low',this)">
        <div class="opt-icon">🏆</div>
        <div><div class="opt-title">Less important — quality over cost</div><div class="opt-desc">Willing to pay more for active management or specialist exposure</div></div>
      </div>
    </div>
    <div class="flex justify-between mt-6">
      <button onclick="prevStep()" class="px-5 py-2.5 text-gray-600 rounded-lg font-semibold text-sm hover:bg-gray-100 transition-colors">← Back</button>
      <button onclick="submitPreferences()" id="btn-submit" class="px-6 py-2.5 bg-blue-600 text-white rounded-lg font-semibold text-sm disabled:opacity-40 disabled:cursor-not-allowed hover:bg-blue-700 transition-colors" disabled>
        Build My Portfolio →
      </button>
    </div>
  </div>

  <!-- ── LOADING ── -->
  <div id="loading" class="hidden text-center py-16">
    <div class="inline-block w-12 h-12 border-4 border-blue-100 border-t-blue-600 rounded-full animate-spin mb-4"></div>
    <p class="text-gray-600 font-medium">Building your personalised portfolio…</p>
    <p class="text-gray-400 text-sm mt-1">Analysing 470+ ETFs to find your best matches</p>
  </div>

  <!-- ── RESULTS ── -->
  <div id="results" class="hidden">
    <!-- Risk profile badge -->
    <div class="bg-white rounded-2xl border border-gray-100 shadow-sm p-6 mb-5">
      <div class="flex items-start gap-4">
        <div id="risk-icon" class="text-4xl">📊</div>
        <div class="flex-1 min-w-0">
          <p class="text-xs font-semibold text-blue-600 uppercase tracking-wider mb-1">Your Risk Profile</p>
          <h2 id="risk-label" class="text-2xl font-bold text-gray-900 mb-2"></h2>
          <p id="risk-desc" class="text-gray-600 text-sm leading-relaxed"></p>
        </div>
      </div>
    </div>

    <!-- Allocation visual -->
    <div class="bg-white rounded-2xl border border-gray-100 shadow-sm p-6 mb-5">
      <h3 class="font-bold text-gray-900 mb-4">Asset Allocation</h3>
      <div id="alloc-bars" class="space-y-3 mb-5"></div>
      <!-- Stacked bar -->
      <div class="flex rounded-lg overflow-hidden h-4 gap-px" id="alloc-stacked"></div>
    </div>

    <!-- ETF Holdings -->
    <div class="mb-5">
      <h3 class="font-bold text-gray-900 mb-3">Recommended ETFs</h3>
      <div id="etf-cards" class="grid gap-3"></div>
    </div>

    <!-- Disclaimer -->
    <div class="bg-amber-50 border border-amber-200 rounded-xl p-4 text-xs text-amber-800 mb-5">
      <strong>Important:</strong> This is a model portfolio for educational purposes only and does not constitute financial advice. ETF recommendations are based on fund size and category fit. Always consider your personal circumstances and consult a licensed financial adviser before investing.
    </div>

    <!-- Actions -->
    <div class="flex gap-3 flex-wrap">
      <button onclick="startOver()" class="px-5 py-2.5 bg-white border border-gray-200 text-gray-700 rounded-lg font-semibold text-sm hover:bg-gray-50 transition-colors">
        ← Start Over
      </button>
      <a href="/dashboard" class="px-5 py-2.5 bg-blue-600 text-white rounded-lg font-semibold text-sm hover:bg-blue-700 transition-colors text-center">
        Explore ETFs on Dashboard →
      </a>
    </div>
  </div>

</main>

<script>
const TOTAL_STEPS = 7;
let currentStep = 1;
const answers = {};

const STEP_KEYS = ['goal','horizon','risk','income','esg','geography','fees'];

const RISK_ICONS = { 1:'🛡️', 2:'⚖️', 3:'🎯', 4:'🚀', 5:'⚡' };

function pick(key, value, el) {
  // Deselect siblings
  el.closest('.grid').querySelectorAll('.opt-card').forEach(c => c.classList.remove('selected'));
  el.classList.add('selected');
  answers[key] = value;
  // Enable next button
  const btn = document.getElementById('btn-next-' + currentStep) || document.getElementById('btn-submit');
  if (btn) btn.disabled = false;
}

function updateProgress() {
  for (let i = 1; i <= TOTAL_STEPS; i++) {
    const ps = document.getElementById('ps-' + i);
    if (!ps) continue;
    ps.className = 'progress-step';
    if (i < currentStep) { ps.classList.add('done'); ps.innerHTML = '✓'; }
    else if (i === currentStep) { ps.classList.add('current'); ps.textContent = i; }
    else { ps.textContent = i; }

    const pl = document.getElementById('pl-' + i);
    if (pl) pl.className = 'progress-line' + (i < currentStep ? ' done' : '');
  }
}

function showStep(n) {
  document.querySelectorAll('.step').forEach(s => s.classList.remove('active'));
  const step = document.getElementById('step-' + n);
  if (step) step.classList.add('active');
  document.getElementById('progress-bar').classList.remove('hidden');
  document.getElementById('intro').classList.add('hidden');
  updateProgress();
  window.scrollTo({ top: 0, behavior: 'smooth' });
}

function nextStep() {
  const key = STEP_KEYS[currentStep - 1];
  if (!answers[key]) return;
  if (currentStep < TOTAL_STEPS) {
    currentStep++;
    showStep(currentStep);
  }
}

function prevStep() {
  if (currentStep > 1) {
    currentStep--;
    showStep(currentStep);
    // Re-enable next button if already answered
    const key = STEP_KEYS[currentStep - 1];
    if (answers[key]) {
      const btn = document.getElementById('btn-next-' + currentStep) || document.getElementById('btn-submit');
      if (btn) btn.disabled = false;
    }
  }
}

async function submitPreferences() {
  // Show loading
  document.querySelectorAll('.step').forEach(s => s.classList.remove('active'));
  document.getElementById('progress-bar').classList.add('hidden');
  document.getElementById('loading').classList.remove('hidden');
  window.scrollTo({ top: 0, behavior: 'smooth' });

  try {
    const resp = await fetch('/api/v1/screener/preferences', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(answers),
    });
    const data = await resp.json();
    document.getElementById('loading').classList.add('hidden');
    renderResults(data);
  } catch(e) {
    document.getElementById('loading').classList.add('hidden');
    alert('Something went wrong. Please try again.');
    startOver();
  }
}

function fmt(v, suffix='') {
  if (v === null || v === undefined) return '—';
  return parseFloat(v).toFixed(2) + suffix;
}

function fmtM(v) {
  if (!v) return '—';
  if (v >= 1000) return '$' + (v / 1000).toFixed(1) + 'b';
  return '$' + Math.round(v) + 'm';
}

function renderResults(data) {
  const riskIcons = { 1:'🛡️', 2:'⚖️', 3:'🎯', 4:'🚀', 5:'⚡' };
  document.getElementById('risk-icon').textContent = riskIcons[data.risk_level] || '📊';
  document.getElementById('risk-label').textContent = data.risk_label;
  document.getElementById('risk-desc').textContent = data.risk_description;

  // Allocation bars
  const allocs = data.allocations;
  const colors = ${JSON_AC_COLORS};

  const barsEl = document.getElementById('alloc-bars');
  barsEl.innerHTML = Object.entries(allocs).sort((a,b) => b[1]-a[1]).map(([ac, pct]) => {
    const color = colors[ac] || '#94a3b8';
    return `<div>
      <div class="flex justify-between text-xs mb-1">
        <span class="font-medium text-gray-700">${ac}</span>
        <span class="text-gray-500 font-semibold">${pct}%</span>
      </div>
      <div class="bg-gray-100 rounded-full h-2.5">
        <div class="alloc-bar rounded-full h-2.5" style="width:${pct}%;background:${color}"></div>
      </div>
    </div>`;
  }).join('');

  // Stacked bar
  const stackedEl = document.getElementById('alloc-stacked');
  stackedEl.innerHTML = Object.entries(allocs).sort((a,b) => b[1]-a[1]).map(([ac, pct]) => {
    const color = colors[ac] || '#94a3b8';
    return `<div title="${ac}: ${pct}%" style="width:${pct}%;background:${color};min-width:2px"></div>`;
  }).join('');

  // ETF cards
  const cardsEl = document.getElementById('etf-cards');
  cardsEl.innerHTML = data.holdings.map(h => {
    const color = h.color || '#94a3b8';
    const mer = h.expense_ratio ? fmt(h.expense_ratio, '%') : '—';
    const yld = h.distribution_yield ? fmt(h.distribution_yield, '%') : '—';
    const fum = fmtM(h.fund_size_aud_millions);
    const desc = h.description ? `<p class="text-gray-500 text-xs mt-2 leading-relaxed">${h.description.slice(0,140)}${h.description.length>140?'…':''}</p>` : '';
    return `
    <div class="etf-card">
      <div class="flex items-start gap-3">
        <div class="flex-shrink-0 w-1.5 self-stretch rounded-full" style="background:${color}"></div>
        <div class="flex-1 min-w-0">
          <div class="flex items-start justify-between gap-2 mb-1">
            <div>
              <span class="font-bold text-gray-900 text-base">${h.code}</span>
              <span class="text-gray-500 text-xs ml-2">${h.sub_category || h.asset_class}</span>
            </div>
            <div class="flex-shrink-0 text-right">
              <span class="badge text-white text-xs font-bold" style="background:${color}">${h.allocation_pct}%</span>
            </div>
          </div>
          <p class="text-gray-700 text-sm font-medium leading-snug">${h.name}</p>
          ${desc}
          <div class="flex gap-4 mt-2.5">
            <div class="text-center">
              <div class="text-xs text-gray-400">MER</div>
              <div class="text-xs font-semibold text-gray-700">${mer}</div>
            </div>
            <div class="text-center">
              <div class="text-xs text-gray-400">Yield</div>
              <div class="text-xs font-semibold text-gray-700">${yld}</div>
            </div>
            <div class="text-center">
              <div class="text-xs text-gray-400">Fund Size</div>
              <div class="text-xs font-semibold text-gray-700">${fum}</div>
            </div>
          </div>
        </div>
      </div>
    </div>`;
  }).join('');

  document.getElementById('results').classList.remove('hidden');
  window.scrollTo({ top: 0, behavior: 'smooth' });
}

function startOver() {
  Object.keys(answers).forEach(k => delete answers[k]);
  currentStep = 1;
  document.getElementById('results').classList.add('hidden');
  document.getElementById('loading').classList.add('hidden');
  document.getElementById('progress-bar').classList.add('hidden');
  document.getElementById('intro').classList.remove('hidden');
  // Reset step 1
  document.querySelectorAll('.step').forEach(s => s.classList.remove('active'));
  document.getElementById('step-1').classList.add('active');
  document.querySelectorAll('.opt-card').forEach(c => c.classList.remove('selected'));
  document.querySelectorAll('[id^=btn-next],[id=btn-submit]').forEach(b => b.disabled = true);
  window.scrollTo({ top: 0, behavior: 'smooth' });
}
</script>
</body>
</html>"""

# Inject the AC colours dict as JSON for the template
import json as _json
SCREENER_HTML = SCREENER_HTML.replace(
    '${JSON_AC_COLORS}',
    _json.dumps(_AC_COLORS)
)
