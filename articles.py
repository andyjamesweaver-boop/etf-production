"""
Articles for the Australian ETF Market platform.
Each article is a dict with: slug, title, subtitle, date, category, summary, body (HTML).
"""

ARTICLES = [

    # ── Weekly Wrap ────────────────────────────────────────────────────────────

    {
        "slug": "weekly-wrap-16-march-2026",
        "title": "Weekly Wrap: Gold Miners Retreat, Asia Rallies",
        "subtitle": "The market fell 0.81% in the week to 16 March. Gold miners gave back double digits while Asian equities outperformed. Bear ETFs had their best week in months.",
        "date": "2026-03-20",
        "category": "Market Trends",
        "summary": "The Australian ETF market fell 0.81% in the week to 16 March 2026, as a sharp commodity selloff dominated headlines. Gold miners — which have been among the best performers over the past two years — gave back 10–12% in a single week. Offsetting that, Asian equities were broadly positive, with South Korean and China tech ETFs leading. Fixed income barely moved. Inverse ETFs rose. The week reinforced a familiar pattern: after extraordinary runs, mean reversion tends to be fast.",
        "body": """
<h2>Overview</h2>
<p>The Australian ETF market fell <strong>0.81%</strong> on an equal-weighted basis in the week to 16 March 2026, with 396 ETFs included in the calculation. The result masked a sharp divergence between asset classes: commodities fell 4.8%, Australian equities fell 1.4%, and international equities fell 0.7%, while cash, digital assets, and alternatives were marginally positive. Fixed income was the relative safe haven, declining just 0.2%.</p>

<p>The week's story was told in two acts. In the first, gold miners — products that had been among the strongest performers across the prior twelve months — gave back substantial ground in a single session-cluster, with MNRS and GDX both losing more than 11%. In the second, Asia-Pacific equities attracted buying interest, with South Korea, China tech, and broader Asia ex-Japan all advancing meaningfully.</p>

<div class="chart-box">
  <h3>Weekly Return by Asset Class — Week to 16 March 2026</h3>
  <div style="position:relative;height:240px"><canvas id="chart-ac-weekly"></canvas></div>
</div>
<script>
(function() {
  const labels = ['Cash','Digital Assets','Alternatives','Fixed Income','Thematic','Property','Diversified','Intl Equities','Aust Equities','Commodities'];
  const data   = [+0.1,+1.2,+0.1,-0.2,-0.5,-0.4,-0.6,-0.7,-1.4,-4.8];
  const colors = data.map(v => v >= 0 ? '#4ade8080' : '#f8717180');
  const borders = data.map(v => v >= 0 ? '#4ade80' : '#f87171');
  new Chart(document.getElementById('chart-ac-weekly'), {
    type: 'bar',
    data: { labels, datasets: [{ label: 'Return %', data, backgroundColor: colors, borderColor: borders, borderWidth: 1, borderRadius: 3, borderSkipped: false }] },
    options: { indexAxis: 'y', responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false }, tooltip: { callbacks: { label: c => c.parsed.x.toFixed(1) + '%' } } },
      scales: {
        x: { ticks: { font: { size: 10 }, callback: v => v + '%' }, grid: { color: '#1e3860' } },
        y: { ticks: { font: { size: 10 } }, grid: { display: false } }
      }
    }
  });
})();
</script>

<h2>The Commodity Selloff: Gold Miners' Sharpest Week in Months</h2>
<p>Gold miners had the worst week across the entire ETF market. VanEck's GDX (Global Gold Miners) fell <strong>11.3%</strong> and BetaShares' MNRS (Global Gold Miners Currency Hedged) fell <strong>11.0%</strong>. Silver miners were worse: Global X's SLVM dropped <strong>12.2%</strong> and BetaShares' XMET (Global Uranium and Copper Miners) fell <strong>10.8%</strong>. Uranium also sold off, with URNM declining 7.2%.</p>

<p>Context matters here. GDX has returned <strong>+137.9%</strong> over the trailing twelve months. MNRS has returned <strong>+189.9%</strong>. SLVM launched in January 2026 and has already drawn $31M in assets. These are products that have attracted significant inflows precisely because of their performance, which makes the week's selloff interpretable as profit-taking rather than a fundamental reversal — though the two are not mutually exclusive.</p>

<p>Physical gold ETFs also fell, but far less sharply. GOLD (Global X) and QAU (BetaShares) both declined around 5%, consistent with underlying gold price weakness. The amplified miner losses reflect the operating leverage inherent in mining equities: when gold falls, miner margins compress faster than the metal price, driving equity selloffs that exceed the commodity move.</p>

<p>WIRE (Global X Global Infrastructure & Utilities), which has a significant exposure to copper-intensive electrification infrastructure, also fell 7.8%. Copper prices weakened on the week amid renewed concern about global industrial demand, particularly from Europe.</p>

<div class="chart-box">
  <h3>Worst Performers — Week to 16 March (FUM ≥ $10M)</h3>
  <div style="position:relative;height:200px"><canvas id="chart-bottom"></canvas></div>
</div>
<script>
(function() {
  const labels = ['SLVM','GDX','MNRS','XMET','GMTL','WIRE','URNM','ETPMAG','ATOM','QAU'];
  const data   = [-12.2,-11.3,-11.0,-10.8,-8.6,-7.8,-7.2,-7.0,-5.7,-5.0];
  new Chart(document.getElementById('chart-bottom'), {
    type: 'bar',
    data: { labels, datasets: [{ label: 'Return %', data, backgroundColor: '#f8717166', borderColor: '#f87171', borderWidth: 1, borderRadius: 3, borderSkipped: false }] },
    options: { responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false }, tooltip: { callbacks: { label: c => c.parsed.y.toFixed(1) + '%' } } },
      scales: {
        x: { ticks: { font: { size: 10 } }, grid: { display: false } },
        y: { ticks: { font: { size: 10 }, callback: v => v + '%' }, grid: { color: '#1e3860' } }
      }
    }
  });
})();
</script>

<h2>Asian Equities: The Week's Clear Outperformer</h2>
<p>While commodities sold off, Asian equities were the standout positive theme. iShares' IKO (MSCI South Korea ETF) led all ETFs with FUM above $10M, rising <strong>+5.8%</strong> on the week. South Korea's equity market has been a beneficiary of renewed investor interest in AI hardware and semiconductor exposure, with Samsung Electronics and SK Hynix representing significant weights in the benchmark.</p>

<p>BetaShares' ASIA (Asia Technology Tigers) rose <strong>+3.5%</strong>, and iShares' IAA (Asia 50) gained <strong>+2.7%</strong>. Both are products with meaningful China and Taiwan technology exposure. Global X's DRGN (China Tech) added 2.4%. The pattern suggests rotation from US tech — which had a relatively modest week — toward Asian technology names.</p>

<p>Vanguard's VAE (FTSE Asia ex Japan) gained 1.3% and Platinum's PAXX (Platinum Asia Fund) added 1.2%. The Asia ex-Japan category collectively outperformed global developed markets by a meaningful margin. For investors who have been waiting for a sustained Asian equity outperformance cycle, the week provided encouraging data — though one week is insufficient to call a trend.</p>

<h2>Broad Equity Benchmarks: Modest Declines Across the Board</h2>
<p>The large Australian and global equity benchmarks declined modestly. VAS (Vanguard Australian Shares, $22.5B) fell <strong>1.43%</strong>, A200 (BetaShares, $9.1B) fell <strong>1.30%</strong>, and STW (SPDR, $6.2B) fell <strong>1.35%</strong>, all consistent with weakness in ASX200 large caps driven partly by resource sector exposure. VHY (Vanguard High Yield, $6.8B) was the exception, rising <strong>+0.27%</strong>, benefiting from defensive yield-seeking in an uncertain week.</p>

<p>On the global side, IVV (iShares S&amp;P 500, $12.2B) fell <strong>0.59%</strong> and NDQ (BetaShares Nasdaq 100, $7.1B) declined just <strong>0.21%</strong> — the Nasdaq's relative resilience suggesting technology continued to hold ground even as sentiment was broadly cautious. VGS (Vanguard Global Shares, $13.9B) fell 0.63%. QUAL (VanEck Quality Factor, $7.7B) fell 1.16%, its quality-factor screen not providing meaningful protection in this week's environment.</p>

<h2>Fixed Income: The Quiet Achiever</h2>
<p>Fixed income was remarkably stable. VBND (Vanguard Global Aggregate Bond, $3.9B) fell just 0.07%, as did VAF (Vanguard Australian Fixed Interest, $3.4B). Floating rate and hybrid products were marginally positive: QPON (BetaShares Senior Floating Rate, $1.9B) gained 0.11% and HBRD (BetaShares Hybrids, $2.6B) gained 0.10%. AAA (BetaShares Cash Plus, $5.0B) rose 0.06% as cash yields continued to accrue.</p>

<p>The near-zero movement in fixed income is notable in context. With commodities down sharply and equities under pressure, bonds functioned as expected — neither selling off in a risk-off move nor rallying as a flight-to-quality asset. This is consistent with a market interpreting the week as sector rotation rather than systemic concern.</p>

<h2>Inverse and Bear ETFs: A Good Week for Hedgers</h2>
<p>Investors who held inverse products were rewarded. BBOZ (BetaShares Australian Equities Strong Bear) rose <strong>+3.3%</strong> and BBUS (US Equities Strong Bear) gained <strong>+2.3%</strong>, both reflecting the underlying index declines. SNAS (Global X Ultra Short Nasdaq 100) gained 1.5%. OOO (BetaShares Crude Oil, which tracks crude oil futures) added 2.5% as energy prices moved against the broader commodity trend.</p>

<p>The geared long products experienced the inverse: GEAR (BetaShares Geared Australian Equities) fell 3.1%, GGUS (BetaShares Geared US Equity) fell 2.4%, and LEVR (First Sentier Geared Australian Share) fell 4.3%. These products amplify market moves in both directions, and this week that amplification cut against holders.</p>

<h2>New Products: Vanguard's Ultra-Low-Cost S&P 500 ETFs Find Early Traction</h2>
<p>Three products listed on 4 March are now in their first weeks of trading. Vanguard's <strong>V500</strong> (S&amp;P 500, MER 0.07%) has already attracted $24M in assets, and its hedged sibling <strong>V5AH</strong> (MER 0.09%) has gathered $5M. These are among the cheapest S&amp;P 500 products available to Australian retail investors — V500 undercuts IVV's 0.04% MER only marginally, but the Vanguard brand and in-specie creation structure make it a natural fit for existing Vanguard investors diversifying their portfolios.</p>

<p>VanEck's <strong>MONY</strong> (Cash Plus Active, MER 0.15%), which listed on 4 February, has now crossed $100M in assets — a notable milestone for a cash-management product in its first six weeks. The fund targets returns above the RBA cash rate, sitting between AAA (pure cash) and HBRD (hybrid credit) on the risk spectrum.</p>

<h2>NAV Premium/Discount</h2>
<p>The market-wide average premium/discount to NAV was <strong>-1.07%</strong> as of 20 March, with 57 of 318 ETFs trading at a premium. The broad discount reflects the lag between NAV calculations (typically struck at market close) and intraday price movements — particularly pronounced for products with underlying assets in different time zones. For investors in diversified equity products, the discount is unlikely to be economically significant at these levels.</p>

<h2>What to Watch</h2>
<p>The week's sharp commodity reversal will draw attention to whether the gold miners selloff represents a temporary correction within a continuing bull trend, or the beginning of a more sustained mean reversion. GDX has returned 138% over twelve months; a 10–15% correction is modest in that context. The more important signal will come from gold price direction — if the metal stabilises above $3,000/oz, miner equities are likely to recover quickly.</p>

<p>The Asia outperformance theme is worth monitoring. If China stimulus expectations accelerate and AI semiconductor demand remains elevated, products like ASIA, IAA, and IKO could sustain gains that have been intermittent rather than persistent over the past year.</p>

<p>Fixed income investors will be watching the Reserve Bank of Australia's next communication carefully. With cash ETFs like AAA returning approximately 3.9% annually and investment-grade credit (CRED) at 5%, the fixed income carry trade remains attractively priced relative to equity volatility.</p>
""",
    },

    # ── Thematic / Performance Articles ───────────────────────────────────────

    {
        "slug": "gold-miners-leverage-mnrs-2026",
        "title": "Miners vs Metal: How MNRS Turned Gold's Rally Into a 363% Return",
        "subtitle": "Physical gold ETFs returned 137% over the past two years. Gold miners returned more than twice that. Here's why the gap exists — and why it can reverse.",
        "date": "2026-03-19",
        "category": "Performance",
        "summary": "Physical gold ETFs returned 137% from January 2024 to March 2026. Over the same period, BetaShares' Gold Miners Currency Hedged ETF (MNRS) returned 263%. The gap is not a mistake — it is operating leverage. But the same mechanism that amplifies gains also amplifies losses, and investors who chased MNRS in early 2024 endured two months of underperformance before the trade delivered.",
        "body": """
<p>When gold goes up, gold miners tend to go up more. And when gold falls, they tend to fall harder. This relationship — operating leverage — has been on vivid display over the past two years on the ASX, as BetaShares' Global Gold Miners ETF (MNRS) delivered returns that dwarfed those of the physical gold ETFs it nominally tracks alongside.</p>

<p>From January 2024 to March 2026, MNRS rose from $5.01 to $18.21 — a gain of <strong>263%</strong>. Over the same period, the VanEck Gold Bullion ETF (NUGG) rose from $30.45 to $72.28 — a gain of <strong>137%</strong>. Both products benefited from the same underlying commodity. One returned nearly twice the other.</p>

<p>One important structural note: MNRS is currency hedged, meaning it removes the AUD/USD exchange rate from its returns. Investors receive the equity performance of global gold miners in AUD terms, without the overlay of currency movements. This makes the comparison with NUGG (which holds physical gold priced in USD and is unhedged) a cleaner test of the operating leverage thesis — the MNRS outperformance reflects equity amplification of gold's move, not a currency tailwind.</p>

<h2>Performance at a glance</h2>
<table>
  <thead><tr><th>ETF</th><th>Type</th><th>1Y Return</th><th>2Y Return (est.)</th><th>FUM</th><th>Fee</th></tr></thead>
  <tbody>
    <tr><td>MNRS</td><td>Gold miners equity (AUD hedged)</td><td class="pos">+190%</td><td class="pos">+263%</td><td>$285M</td><td>0.57%</td></tr>
    <tr><td>GDX</td><td>Gold miners equity (large-cap)</td><td class="pos">+138%</td><td>—</td><td>$1,575M</td><td>0.53%</td></tr>
    <tr><td>NUGG</td><td>Physical gold bullion</td><td class="pos">+65%</td><td class="pos">+137%</td><td>$259M</td><td>0.25%</td></tr>
    <tr><td>GLDN</td><td>Physical gold bullion</td><td class="pos">+65%</td><td>—</td><td>$394M</td><td>0.18%</td></tr>
  </tbody>
</table>

<div class="chart-box">
  <h3>MNRS vs NUGG — Indexed to 100 (January 2024)</h3>
  <div style="position:relative;height:260px"><canvas id="chart-gold-leverage"></canvas></div>
</div>
<script>
(function() {
  const labels = ["2024-01","2024-02","2024-03","2024-04","2024-05","2024-06","2024-07","2024-08","2024-09","2024-10","2024-11","2024-12","2025-01","2025-02","2025-03","2025-04","2025-05","2025-06","2025-07","2025-08","2025-09","2025-10","2025-11","2025-12","2026-01","2026-02","2026-03"];
  const mnrs  = [100.0,92.7,98.2,109.9,120.5,121.6,119.3,119.9,123.1,135.4,137.3,137.0,131.8,146.3,145.7,165.6,167.2,184.9,184.7,208.4,231.1,269.1,253.6,290.0,326.7,325.7,363.2];
  const nugg  = [100.0,102.8,106.4,113.6,116.6,116.5,114.7,120.3,122.5,128.2,132.5,133.9,141.0,149.2,151.1,168.3,169.2,169.3,165.4,169.8,177.8,197.1,201.5,207.4,217.4,226.3,237.4];
  new Chart(document.getElementById('chart-gold-leverage'), {
    type: 'line',
    data: {
      labels,
      datasets: [
        { label: 'MNRS (Gold Miners)', data: mnrs, borderColor: '#f59e0b', backgroundColor: '#f59e0b18', fill: true, tension: 0.3, pointRadius: 2, borderWidth: 2 },
        { label: 'NUGG (Physical Gold)', data: nugg, borderColor: '#6b7280', backgroundColor: '#6b728018', fill: true, tension: 0.3, pointRadius: 2, borderWidth: 2 }
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: { legend: { position: 'bottom', labels: { boxWidth: 10, font: { size: 10 } } } },
      scales: {
        x: { ticks: { font: { size: 10 } }, grid: { display: false } },
        y: { ticks: { font: { size: 10 }, callback: v => v + '' }, grid: { color: '#1e3860' }, title: { display: true, text: 'Indexed (Jan 2024 = 100)', font: { size: 10 } } }
      }
    }
  });
})();
</script>

<h2>Why miners amplify gold's moves</h2>

<p>A gold mining company's revenue is determined by the gold price, but its costs — labour, energy, equipment, capital — are largely fixed in the short term. When gold rises from, say, $2,000 to $2,600 per ounce (a 30% gain), a miner whose all-in sustaining cost is $1,400 per ounce sees its profit margin expand from $600 to $1,200 per ounce — a 100% increase. The equity market prices that profit expansion, not just the commodity gain.</p>

<p>This is operating leverage in action. The same mechanism works in reverse: if gold falls 20%, a high-cost miner may see its margin halved or eliminated entirely, causing the stock to fall 50% or more. Gold miners carry more risk than the metal itself.</p>

<h2>The first half of 2024: a painful lesson</h2>

<p>The leverage story did not play out cleanly from the start. In January and February 2024, gold was rising (NUGG climbed from $30.45 to $31.29, a 2.8% gain) while MNRS fell — from $5.01 to $4.65, a drop of 7.2%. This apparent paradox reflects that gold miners are affected by more than just the gold price: input cost inflation, operational disruptions, currency movements, and general equity market sentiment all play a role. In early 2024, equity markets were cautious about mining stocks even as the metal itself appreciated.</p>

<p>By April 2024, the leverage kicked in properly. Gold broke above $2,300 per ounce for the first time, and MNRS moved from $4.65 to $5.51 in a month — erasing its early underperformance and then some. From that point, the divergence between the two ETFs began to widen meaningfully.</p>

<h2>The acceleration phase: mid-2025 onward</h2>

<p>The most dramatic divergence emerged in the second half of 2025. From July to October 2025, NUGG rose from $50.35 to $60.02 — a solid 19% over four months. MNRS over the same period rose from $9.26 to $13.49 — a 46% gain. The leverage ratio in this period was approximately 2.4×, consistent with the leverage effect as gold approached and then exceeded $3,000 per ounce.</p>

<p>By March 2026, MNRS had returned 263% from its January 2024 starting point, compared with NUGG's 137%. In absolute unit price terms, MNRS moved from $5.01 to $18.21 and NUGG from $30.45 to $72.28 — both extraordinary outcomes, but with very different risk profiles along the way.</p>

<h2>What investors should understand</h2>

<p>MNRS (BetaShares) and GDX (VanEck) are equity products, not commodity products. They are affected by company-specific risks, management quality, hedging policies, and equity market valuations, in addition to the gold price. Investors who want pure commodity exposure with lower volatility are better served by NUGG or GLDN (iShares Physical Gold ETF, $394M, fee 0.18%), both of which hold physical gold directly.</p>

<p>The fee difference is also meaningful over time. NUGG charges 0.25% and GLDN 0.18%, while MNRS charges 0.57%. In a flat gold environment, that spread compounds unfavourably for miners holders. The case for MNRS rests entirely on the gold price rising — and rising enough to justify the additional volatility, equity risk, and fee cost.</p>

<p>Over the past two years, that case was emphatically made. But investors considering adding gold miners exposure today are buying at prices that already reflect a 263% gain. The leverage that worked so powerfully on the way up will work equally powerfully on the way down.</p>
""",
    },

    {
        "slug": "hjpn-japan-currency-hedging-2026",
        "title": "The Yen Trade: How Currency Hedging Added 39 Percentage Points to Japan Returns",
        "subtitle": "Two Japan ETFs, the same underlying market, a 39-percentage-point performance gap. The difference was whether you hedged the yen.",
        "date": "2026-03-19",
        "category": "Performance",
        "summary": "HJPN, BetaShares' currency-hedged Japan ETF, returned 74% from January 2024 to March 2026. IJP, the iShares unhedged equivalent, returned 40% over the same period. Both hold the same Japanese equities. The gap — 34 percentage points over two years, 39 percentage points over one year — reflects the cost of holding unhedged yen exposure as Japan's currency weakened against the Australian dollar.",
        "body": """
<p>Japan's equity market has been one of the world's better-performing major markets over the past two years. The Nikkei 225 broke above 40,000 for the first time in March 2024, a milestone that resonated globally as Japanese equities shook off three decades of stagnation. Corporate governance reforms, the end of negative interest rates, and the return of domestic inflation all contributed to a genuine re-rating of Japanese stocks.</p>

<p>Yet two Australia-listed Japan ETFs that both hold Japanese equities produced very different outcomes for investors. BetaShares' HJPN (Japan ETF — Currency Hedged) returned <strong>+55.5%</strong> over the past twelve months. The iShares IJP (MSCI Japan ETF, unhedged) returned <strong>+15.7%</strong> over the same period. Both track Japanese equities. The difference is almost entirely currency.</p>

<h2>Performance comparison</h2>
<table>
  <thead><tr><th>ETF</th><th>Currency Hedging</th><th>1Y Return</th><th>3Y Return</th><th>FUM</th><th>Fee</th></tr></thead>
  <tbody>
    <tr><td>HJPN</td><td>AUD hedged (removes JPY risk)</td><td class="pos">+55.5%</td><td class="pos">+33.2%</td><td>$244M</td><td>0.56%</td></tr>
    <tr><td>IJP</td><td>Unhedged (full JPY exposure)</td><td class="pos">+15.7%</td><td class="pos">+59.2%</td><td>$1,229M</td><td>0.50%</td></tr>
  </tbody>
</table>

<div class="chart-box">
  <h3>HJPN vs IJP — Indexed to 100 (January 2024)</h3>
  <div style="position:relative;height:260px"><canvas id="chart-japan-hedge"></canvas></div>
</div>
<script>
(function() {
  const labels = ["2024-01","2024-02","2024-03","2024-04","2024-05","2024-06","2024-07","2024-08","2024-09","2024-10","2024-11","2024-12","2025-01","2025-02","2025-03","2025-04","2025-05","2025-06","2025-07","2025-08","2025-09","2025-10","2025-11","2025-12","2026-01","2026-02","2026-03"];
  const hjpn = [100.0,110.6,119.2,118.3,119.4,121.2,121.2,108.0,111.6,120.4,121.6,121.7,123.5,123.2,120.6,106.9,121.4,124.1,126.3,133.5,137.4,145.6,150.0,154.4,162.7,172.4,173.6];
  const ijp  = [100.0,109.0,115.2,112.7,109.8,110.7,110.7,105.8,111.6,112.9,112.4,117.5,115.4,119.5,120.5,114.5,122.6,123.2,120.8,130.2,132.5,134.6,139.8,138.2,140.4,142.2,140.1];
  new Chart(document.getElementById('chart-japan-hedge'), {
    type: 'line',
    data: {
      labels,
      datasets: [
        { label: 'HJPN (hedged)', data: hjpn, borderColor: '#ef4444', backgroundColor: '#ef444418', fill: true, tension: 0.3, pointRadius: 2, borderWidth: 2 },
        { label: 'IJP (unhedged)', data: ijp,  borderColor: '#3b82f6', backgroundColor: '#3b82f618', fill: true, tension: 0.3, pointRadius: 2, borderWidth: 2 }
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: { legend: { position: 'bottom', labels: { boxWidth: 10, font: { size: 10 } } } },
      scales: {
        x: { ticks: { font: { size: 10 } }, grid: { display: false } },
        y: { ticks: { font: { size: 10 } }, grid: { color: '#1e3860' }, title: { display: true, text: 'Indexed (Jan 2024 = 100)', font: { size: 10 } } }
      }
    }
  });
})();
</script>

<h2>How currency hedging works</h2>

<p>When an Australian investor buys IJP, they are exposed to two things: the performance of Japanese equities, and the AUD/JPY exchange rate. If Japanese stocks rise 20% in yen terms but the yen weakens 15% against the Australian dollar over the same period, the Australian investor pockets only around 5% — the currency move has eaten most of the equity gain.</p>

<p>HJPN eliminates that second exposure. It enters into forward contracts to lock in the exchange rate, so that Australian investors receive the equity return in full, regardless of what the yen does. The cost of this hedge is embedded in the product's returns — roughly the interest rate differential between Australian and Japanese rates.</p>

<p>The key question is always the direction of the currency. Hedging helps when your home currency (AUD) strengthens against the foreign currency (JPY). It hurts when your home currency weakens.</p>

<h2>The yen's structural weakness</h2>

<p>The yen has been under sustained pressure since 2021, driven by the Bank of Japan's commitment to yield curve control and ultra-loose monetary policy at a time when most other central banks were aggressively raising rates. The resulting interest rate differential drove investors out of yen and into higher-yielding currencies, including the Australian dollar.</p>

<p>By mid-2024, the AUD had appreciated meaningfully against the JPY from historical norms. Unhedged Japanese equity investors — those holding IJP — saw a significant portion of their equity gains eroded by the currency. HJPN holders experienced no such erosion.</p>

<p>The chart tells the story cleanly. Both ETFs moved largely in tandem through early 2024 as both the yen weakness and equity gains were still developing. The divergence became pronounced from mid-2025 onward, with HJPN accelerating to a 73.6% total return from the January 2024 start point versus IJP's 40.1%.</p>

<h2>August 2024: a sharp reminder of hedge risk</h2>

<p>The hedged strategy was not uniformly superior. In August 2024, HJPN fell sharply — from $19.62 to $17.49, a drop of 10.9% in a single month — while IJP held up better, declining only 5.4%. This divergence came when the Bank of Japan surprised markets with a rate hike in late July 2024, causing the yen to rapidly strengthen. Unhedged investors benefited from that yen bounce; hedged investors did not.</p>

<p>That episode illustrated that hedging is not a free lunch. When the yen rallies — as it did briefly in August 2024 — HJPN underperforms. The bet embedded in HJPN is that the yen will remain weak relative to the AUD, or at least not significantly strengthen. Over the past two years, that bet has paid off handsomely. It does not always.</p>

<h2>Three-year returns tell a different story</h2>

<p>The three-year return figures reveal an interesting reversal. IJP's three-year return of <strong>59.2%</strong> significantly exceeds HJPN's <strong>33.2%</strong>. This is because Japanese equities delivered strong gains over 2022–2024, but the yen was more stable in the earlier part of that period — meaning unhedged investors captured more of the equity return before the yen depreciation became severe.</p>

<p>The lesson is that the value of currency hedging is time-dependent and direction-dependent. Investors who held IJP for the full three years were rewarded by earlier periods when the yen was not a meaningful drag. Those who added in 2024–2025 found the yen to be a significant headwind that HJPN's hedge removed.</p>

<h2>Which product is right for you?</h2>

<p>HJPN charges 0.56% per annum versus IJP's 0.50%. The cost difference is modest. The more important question is your view on the yen. If you believe the Bank of Japan will continue normalising rates and the yen will structurally recover against the AUD, IJP's unhedged exposure may be more attractive. If you are agnostic about currency direction and simply want clean Japanese equity exposure, HJPN removes one significant source of uncertainty.</p>

<p>IJP is also the far larger product at $1.23 billion versus HJPN's $247 million, reflecting its longer track record and broader institutional ownership. Both products are liquid and well-run. The choice between them is fundamentally a view on currency, not a view on the quality of the products themselves.</p>

<p>Over the past twelve months, having a view on the yen — and acting on it — was worth 39 percentage points.</p>
""",
    },

    # ── Annual Year-in-Review Reports ─────────────────────────────────────────

    {
        "slug": "etf-year-review-2025",
        "title": "Australian ETF Year in Review: 2025",
        "subtitle": "2025 was the year the Australian ETF industry crossed $300 billion in assets under management, closing at a record $320.7 billion.",
        "date": "2025-12-31",
        "category": "Annual Report",
        "summary": "2025 was the year the Australian ETF industry crossed $300 billion in assets under management, closing at a record $320.7 billion. It was also a year that defied easy characterisation: global equity returns were positive but more modest than the prior two years, yet ETF flows reached an unprecedented $51.5 billion — nearly 50% above 2024's record level.",
        "body": """
<h2>The $300 Billion Milestone — and a Year of Gold and Flows</h2>
<p>2025 was the year the Australian ETF industry crossed $300 billion in assets under management, closing at a record $320.7 billion. It was also a year that defied easy characterisation: global equity returns were positive but more modest than the prior two years, yet ETF flows reached an unprecedented $51.5 billion — nearly 50% above 2024's record level. Gold delivered extraordinary gains, crypto held a significant place in the product shelf, and the industry's product count surpassed 400 for the first time. Beneath the headline numbers, a story of Vanguard's continued dominance and BetaShares' rapid ascent continued to reshape the competitive landscape.</p>

<hr class="my-6 border-gray-200">

<div class="chart-box">
  <h3>Monthly AUM &amp; Net Flows &#8212; 2025</h3>
  <div style="position:relative;height:260px"><canvas id="chart-monthly-2025"></canvas></div>
</div>
<script>
(function() {
  const labels = ["2025-01", "2025-02", "2025-03", "2025-04", "2025-05", "2025-06", "2025-07", "2025-08", "2025-09", "2025-10", "2025-11", "2025-12"];
  const aums   = [250.48, 247.74, 242.52, 250.89, 265.23, 272.18, 280.66, 290.35, 300.03, 312.24, 315.24, 320.73];
  const flows  = [4631.0, 3103.0, 3381.0, 4518.0, 3136.0, 2128.0, 5552.0, 4883.0, 5250.0, 5672.0, 4074.0, 5127.0];
  new Chart(document.getElementById('chart-monthly-2025'), {
    data: {
      labels,
      datasets: [
        { type: 'line',  label: 'AUM ($B)',         data: aums,  borderColor: '#3b82f6', backgroundColor: '#3b82f620', fill: true,  tension: 0.3, pointRadius: 2, borderWidth: 2, yAxisID: 'yAUM' },
        { type: 'bar',   label: 'Net Flows ($M)',    data: flows, backgroundColor: flows.map(v => v >= 0 ? '#10b98180' : '#ef444480'), borderRadius: 2, yAxisID: 'yFlow' }
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: { legend: { position: 'bottom', labels: { boxWidth: 10, font: { size: 10 } } } },
      scales: {
        x:     { ticks: { font: { size: 10 } }, grid: { display: false } },
        yAUM:  { type: 'linear', position: 'left',  ticks: { font: { size: 10 }, callback: v => '$' + v + 'B' }, grid: { color: '#1e3860' } },
        yFlow: { type: 'linear', position: 'right', ticks: { font: { size: 10 }, callback: v => '$' + v + 'M' }, grid: { display: false } }
      }
    }
  });
})();
</script>


<h2>Industry AUM: $81.6 Billion in a Single Year</h2>
<p>The industry grew from $239.1 billion to $320.7 billion — an increase of $81.6 billion, the largest dollar gain in history by a substantial margin. Net flows of $51.5 billion drove the bulk of that growth, with market appreciation contributing approximately $30 billion. The dominance of flows over price appreciation was notable: in prior bull years (2021, 2023, 2024), price gains had contributed at least as much as flows. In 2025, investors were adding money faster than markets were rising — a powerful statement about structural demand. Total traded value reached $196.6 billion, up 39% from 2024's $141.2 billion, as larger AUM and higher market turnover drove exceptional exchange activity.</p>

<hr class="my-6 border-gray-200">

<h2>Net Flows: $51.5 Billion — More Than Double 2023's Record</h2>
<p>The $51.5 billion in net flows (excluding admission months) was the most remarkable single statistic of the year. It exceeded the prior record of $34.0 billion by more than 50%, and it exceeded the cumulative total flows from any two-year period before 2024. The flows were spread broadly across equity, fixed income, and other asset classes.</p>
<p>In equities, VAS attracted $3.1 billion — its largest ever annual inflow — followed by VGS at $2.6 billion and A200 at $2.1 billion. VHY (Vanguard Australian Shares High Yield) drew $1.6 billion, reflecting ongoing demand for income as yields remained attractive. VGAD (Vanguard International Shares - AUD Hedged) attracted $1.4 billion and BGBL (BetaShares Global Shares) drew $1.4 billion, reinforcing the shift toward diversified international equity across multiple products.</p>
<p>In fixed income, VBND attracted $1.6 billion and SUBD drew $1.2 billion — a continuation of the category's structural growth. IVV brought in $1.2 billion and IOZ $1.1 billion, completing a top-ten that was strikingly broad by asset class and geography.</p>

<hr class="my-6 border-gray-200">

<h2>Top Performing ETFs: Gold's Extraordinary Year</h2>
<p>2025 was the year of precious metals. Among all products with at least $500 million in AUM, GDX (VanEck Gold Miners) returned an extraordinary 143.8% — reflecting not just gold's price appreciation but the operational leverage that mining equities provide to the underlying metal. ETPMAG (Australian bullion ETF for silver) returned 132.8%. QAU (BetaShares Gold Bullion Currency Hedged) returned 64.4%. Gold ETFs broadly had their best year in at least a decade, as geopolitical uncertainty, dollar weakness, and central bank buying drove the metal to successive record highs.</p>
<p>Other strong performers included WIRE (81.0%), reflecting infrastructure themes linked to electrification and the energy transition, and ACDC (59.6%, BetaShares Electric Vehicles and Future Mobility), which benefited from accelerating EV adoption globally.</p>
<p>By contrast, the worst performers among sizable products were concentrated in interest-rate-sensitive or slow-growth categories. IJR (iShares US Small Cap) returned -0.8%, while several newer products showed flat returns reflecting their early-stage nature. The broad equity products — VAS, VGS, A200, IVV — delivered solid but unspectacular 9–13% returns, suggesting that global markets were consolidating after three years of strong gains.</p>

<hr class="my-6 border-gray-200">

<h2>Fund Flows Leaders: Magellan's Long Goodbye</h2>
<p>The top inflow story was the continued, broad-based dominance of Vanguard and BetaShares core products, with no single ETF capturing an outsized share of the market's new money. VAS's $3.1 billion led all products, but the top-ten collectively spread flows across eight distinct products and multiple issuers — a healthy competitive outcome.</p>
<p>The largest outflow was MGOC (Magellan Global Open Class) at -$1.3 billion, continuing the multi-year redemption cycle that had seen the product shrink from over $14 billion at peak to $6.4 billion at year-end 2025. XALG (Alphinity Global Equity Fund) shed $236 million and FRGG lost $203 million, reflecting continued rationalisation of higher-fee active products as investors compared costs.</p>

<hr class="my-6 border-gray-200">

<h2>Asset Class Trends: Commodities and "Other" Surge</h2>
<p>The most dramatic structural shift in 2025 was the surge in commodity and "Other" category AUM. Commodities reached $12.4 billion (up from $6.3 billion at the end of 2024), largely driven by gold's exceptional performance. The "Other" category — encompassing crypto ETFs, listed private credit, and innovative active products — grew to $20.1 billion from $5.4 billion the prior year, as bitcoin and other crypto ETFs attracted significant assets and the broader alternative investment category expanded.</p>
<p>Equity remained dominant at $245.4 billion (76.5% of total AUM), while fixed income reached $40.0 billion — crossing the $40 billion threshold for the first time. Money market products grew to $2.1 billion.</p>

<hr class="my-6 border-gray-200">


<div class="chart-grid">
  <div class="chart-box" style="margin:0">
    <h3>Asset Class Breakdown</h3>
    <div style="position:relative;height:220px"><canvas id="chart-ac-2025"></canvas></div>
  </div>
  <div class="chart-box" style="margin:0">
    <h3>Issuer Market Share</h3>
    <table>
      <thead><tr><th>Issuer</th><th>AUM</th><th>ETFs</th><th>Share</th></tr></thead>
      <tbody>
        <tr><td>Vanguard</td><td>$89.7B</td><td>31</td><td>30.9%</td></tr>
        <tr><td>Betashares</td><td>$61.9B</td><td>100</td><td>21.3%</td></tr>
        <tr><td>iShares</td><td>$54.9B</td><td>51</td><td>18.9%</td></tr>
        <tr><td>VanEck</td><td>$31.5B</td><td>48</td><td>10.9%</td></tr>
        <tr><td>Other</td><td>$18.2B</td><td>6</td><td>6.3%</td></tr>
        <tr><td>Global X</td><td>$15.2B</td><td>44</td><td>5.2%</td></tr>
        <tr><td>StateStreet</td><td>$11.4B</td><td>17</td><td>3.9%</td></tr>
        <tr><td>Magellan</td><td>$7.4B</td><td>4</td><td>2.5%</td></tr>
      </tbody>
    </table>
  </div>
</div>
<script>
(function() {
  const acLabels = ["International Equities", "Australian Equities", "Fixed Income", "Other", "Commodity", "Money Market", "Alternative", "Mixed Allocation", "Specialty"];
  const acData   = [168.1, 77.3, 40.0, 20.1, 12.4, 2.1, 0.6, 0.1, 0.1];
  const PALETTE  = ['#3b82f6','#f59e0b','#10b981','#ef4444','#8b5cf6','#ec4899','#06b6d4','#f97316','#84cc16','#6366f1'];
  new Chart(document.getElementById('chart-ac-2025'), {
    type: 'doughnut',
    data: { labels: acLabels, datasets: [{ data: acData, backgroundColor: PALETTE, borderWidth: 2, borderColor: '#fff' }] },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: 'right', labels: { boxWidth: 10, font: { size: 10 } } } }
    }
  });
})();
</script>

<div class="chart-box">
  <h3>Top 10 ETFs by AUM &#8212; 2025-12-31</h3>
  <table>
    <thead><tr><th>#</th><th>Code</th><th>Name</th><th>Issuer</th><th>AUM ($M)</th><th>1Y Return</th><th>Year Flows ($M)</th></tr></thead>
    <tbody>
      <tr><td>1</td><td><strong>VAS</strong></td><td>Vanguard Australian Shares Index ETF</td><td>Vanguard</td><td>$22,585M</td><td><span class="pos">+11.9%</span></td><td><span class="pos">+$3,078M</span></td></tr>
      <tr><td>2</td><td><strong>VGS</strong></td><td>Vanguard MSCI Index International Shares ETF</td><td>Vanguard</td><td>$14,192M</td><td><span class="pos">+12.9%</span></td><td><span class="pos">+$2,619M</span></td></tr>
      <tr><td>3</td><td><strong>IVV</strong></td><td>iShares S&P 500 ETF</td><td>iShares</td><td>$13,110M</td><td><span class="pos">+9.4%</span></td><td><span class="pos">+$1,167M</span></td></tr>
      <tr><td>4</td><td><strong>A200</strong></td><td>BetaShares Australia 200 ETF</td><td>Betashares</td><td>$8,880M</td><td><span class="pos">+11.5%</span></td><td><span class="pos">+$2,122M</span></td></tr>
      <tr><td>5</td><td><strong>QUAL</strong></td><td>VanEck MSCI International Quality ETF</td><td>VanEck</td><td>$8,070M</td><td><span class="pos">+8.1%</span></td><td><span class="pos">+$474M</span></td></tr>
      <tr><td>6</td><td><strong>IOZ</strong></td><td>iShares Core S&P/ASX 200 ETF</td><td>iShares</td><td>$7,798M</td><td><span class="pos">+11.4%</span></td><td><span class="pos">+$1,113M</span></td></tr>
      <tr><td>7</td><td><strong>NDQ</strong></td><td>BetaShares NASDAQ 100 ETF</td><td>Betashares</td><td>$7,690M</td><td><span class="pos">+11.4%</span></td><td><span class="pos">+$927M</span></td></tr>
      <tr><td>8</td><td><strong>DACE</strong></td><td>Dimensional Australian Core Equity Trust</td><td>Other</td><td>$6,434M</td><td><span class="pos">+17.5%</span></td><td><span class="pos">+$293M</span></td></tr>
      <tr><td>9</td><td><strong>MGOC</strong></td><td>Magellan Global Fund - Open Class Units</td><td>Magellan</td><td>$6,372M</td><td><span class="pos">+3.4%</span></td><td><span class="neg">$-1,301M</span></td></tr>
      <tr><td>10</td><td><strong>VTS</strong></td><td>Vanguard US Total Market Shares Index ETF</td><td>Vanguard</td><td>$6,361M</td><td><span class="pos">+9.0%</span></td><td><span class="pos">+$377M</span></td></tr>
    </tbody>
  </table>
</div>

<div class="chart-grid">
  <div class="chart-box" style="margin:0">
    <h3>Top 5 Performers (min $500M AUM)</h3>
    <table>
      <thead><tr><th>Code</th><th>Name</th><th>Asset Class</th><th>1Y Return</th></tr></thead>
      <tbody>
      <tr><td><strong>GDX</strong></td><td>VanEck Gold Miners ETF</td><td>Equity</td><td><span class="pos">+143.8%</span></td></tr>
      <tr><td><strong>ETPMAG</strong></td><td>Global X Physical Silver</td><td>Commodity</td><td><span class="pos">+132.8%</span></td></tr>
      <tr><td><strong>WIRE</strong></td><td>Global X Copper Miners ETF</td><td>Equity</td><td><span class="pos">+81.0%</span></td></tr>
      <tr><td><strong>QAU</strong></td><td>BetaShares Gold Bullion ETF (Currency Hedged)</td><td>Commodity</td><td><span class="pos">+64.4%</span></td></tr>
      <tr><td><strong>ACDC</strong></td><td>Global X Battery Tech & Lithium ETF</td><td>Equity</td><td><span class="pos">+59.6%</span></td></tr>
      </tbody>
    </table>
  </div>
  <div class="chart-box" style="margin:0">
    <h3>Worst 5 Performers (min $500M AUM)</h3>
    <table>
      <thead><tr><th>Code</th><th>Name</th><th>Asset Class</th><th>1Y Return</th></tr></thead>
      <tbody>
      <tr><td><strong>IJR</strong></td><td>iShares S&P Small-Cap ETF</td><td>Equity</td><td><span class="neg">-0.8%</span></td></tr>
      <tr><td><strong>DIVI</strong></td><td>Ausbil Active Dividend Income Fund</td><td>—</td><td>0.0%</td></tr>
      <tr><td><strong>CIIH</strong></td><td>Clearbridge Global Infrastructure Income (Hedged)</td><td>—</td><td>0.0%</td></tr>
      <tr><td><strong>CUIV</strong></td><td>Clearbridge Global Infrastructure Value Active ETF</td><td>—</td><td>0.0%</td></tr>
      <tr><td><strong>CIVH</strong></td><td>Clearbridge Global Infrastructure Value (Hedged)</td><td>—</td><td>0.0%</td></tr>
      </tbody>
    </table>
  </div>
</div>

<h2>New Launches: 62 Products — Another Record</h2>
<p>Sixty-two new ETPs launched in 2025, extending the industry's consecutive record-breaking year of product development. Notable arrivals included QBTC (BetaShares Bitcoin ETF) and QETH (BetaShares Ether ETF), deepening the crypto product shelf. Vanguard launched VDIF (Diversified Income ETF) and VDAL (Diversified All Growth ETF), extending its multi-asset product range. Macquarie entered the active ETF space with MQSD and MQYM, adding a major institutional name to the issuer roster. VanEck's ALFA (Australian Long Short) brought complex alternatives strategies to the ETF format.</p>

<hr class="my-6 border-gray-200">

<h2>Issuer Landscape: Vanguard and BetaShares Dominate</h2>
<p>Vanguard grew to $89.7 billion — more than a quarter of total industry AUM — confirming its structural dominance built on low-cost, high-quality index products. BetaShares grew to $61.9 billion, closing in on iShares ($54.9 billion) and cementing its position as the most dynamic domestic issuer. VanEck reached $31.5 billion, driven by QUAL, QSML, and a growing fixed income franchise. Global X grew to $15.2 billion as its commodity and thematic products benefited from the gold rally and broader investor interest.</p>
<p>Magellan fell to $7.4 billion, now comfortably below State Street ($11.4 billion). The month of December's flows told the competitive story with precision: Vanguard attracted $1.4 billion, BetaShares $1.7 billion, and iShares $715 million — the three leading issuers collectively capturing the majority of the industry's monthly incremental flows.</p>

<hr class="my-6 border-gray-200">

<h2>Fee Trends: The Cheapest Market in History</h2>
<p>The AUM-weighted MER fell to 0.327% — the lowest on record — as the industry's largest products remained among its cheapest. The simple average MER was 0.528%, reflecting the growing population of specialist, active, and thematic products that command higher fees. The 0.201 percentage point gap between the weighted and simple average MER was the widest ever recorded, illustrating the bifurcation of the market: a high-volume, low-fee core, and a growing long tail of specialist products where issuers can still charge meaningful fees for differentiated exposures.</p>

<hr class="my-6 border-gray-200">

<h2>Looking Back</h2>
<p>2025 cemented the Australian ETF market's position as one of the fastest-growing and most innovative in the Asia-Pacific region. The combination of record flows, a $320 billion milestone, 434 products, and the integration of crypto and alternative assets into the mainstream ETF wrapper marked the industry's coming-of-age. Gold's extraordinary performance rewarded the investors who had maintained commodity exposure through quieter years, while the resilience of core equity flows through a more muted return environment confirmed that Australian ETF investors had adopted a genuinely long-term, systematic investment approach.</p>
""",
    },

    {
        "slug": "etf-year-review-2024",
        "title": "Australian ETF Year in Review: 2024",
        "subtitle": "2024 was the Australian ETF industry's most remarkable year to date, with total assets crossing $200 billion to close at $239.1 billion.",
        "date": "2024-12-31",
        "category": "Annual Report",
        "summary": "2024 was the Australian ETF industry's most remarkable year to date. Equity markets delivered strong returns for the second consecutive year, investor flows hit record levels, and the industry's total assets under management crossed $200 billion to close at $239.1 billion.",
        "body": """
<h2>The Industry's Breakout Year</h2>
<p>2024 was the Australian ETF industry's most remarkable year to date. Equity markets delivered strong returns for the second consecutive year, investor flows hit record levels, and the industry's total assets under management crossed $200 billion to close at $239.1 billion. The breadth of demand was striking: strong inflows came from international equities, domestic equities, and fixed income simultaneously — a sign of a maturing investor base rather than a momentum-driven crowd. Meanwhile, 54 new products launched, including Australia's first Bitcoin ETFs.</p>

<hr class="my-6 border-gray-200">

<div class="chart-box">
  <h3>Monthly AUM &amp; Net Flows &#8212; 2024</h3>
  <div style="position:relative;height:260px"><canvas id="chart-monthly-2024"></canvas></div>
</div>
<script>
(function() {
  const labels = ["2024-01", "2024-02", "2024-03", "2024-04", "2024-05", "2024-06", "2024-07", "2024-08", "2024-09", "2024-10", "2024-11", "2024-12"];
  const aums   = [178.51, 184.48, 191.87, 190.27, 193.38, 199.59, 208.75, 213.2, 219.87, 225.45, 234.33, 239.09];
  const flows  = [1716.0, 1341.0, 1790.0, 1130.0, 1925.0, 2514.0, 6737.0, 3188.0, 2780.0, 3169.0, 3851.0, 3844.0];
  new Chart(document.getElementById('chart-monthly-2024'), {
    data: {
      labels,
      datasets: [
        { type: 'line',  label: 'AUM ($B)',         data: aums,  borderColor: '#3b82f6', backgroundColor: '#3b82f620', fill: true,  tension: 0.3, pointRadius: 2, borderWidth: 2, yAxisID: 'yAUM' },
        { type: 'bar',   label: 'Net Flows ($M)',    data: flows, backgroundColor: flows.map(v => v >= 0 ? '#10b98180' : '#ef444480'), borderRadius: 2, yAxisID: 'yFlow' }
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: { legend: { position: 'bottom', labels: { boxWidth: 10, font: { size: 10 } } } },
      scales: {
        x:     { ticks: { font: { size: 10 } }, grid: { display: false } },
        yAUM:  { type: 'linear', position: 'left',  ticks: { font: { size: 10 }, callback: v => '$' + v + 'B' }, grid: { color: '#1e3860' } },
        yFlow: { type: 'linear', position: 'right', ticks: { font: { size: 10 }, callback: v => '$' + v + 'M' }, grid: { display: false } }
      }
    }
  });
})();
</script>


<h2>Industry AUM: $66 Billion Added in a Single Year</h2>
<p>Beginning 2024 at $173.0 billion, the industry closed at $239.1 billion — a $66.1 billion increase, or 38.2%, the largest single-year dollar gain in the industry's history. Net flows contributed $34.0 billion (excluding admission months), the highest annual figure ever recorded. Market appreciation accounted for the remaining $32.1 billion, meaning both flows and performance made roughly equal contributions to the growth. Total traded value reached $141.2 billion, up from $114.6 billion in 2023, as higher AUM drove larger turnover volumes. The ETF count grew to 379 products.</p>

<hr class="my-6 border-gray-200">

<h2>Net Flows: $34.0 Billion — A Record by a Wide Margin</h2>
<p>The $34.0 billion in net flows represented a near-doubling of the prior year's $13.9 billion and shattered all previous records. The flows were notably broad-based. International equity ETFs collectively dominated: IVV led all products with $2.1 billion in inflows, VGS attracted $1.9 billion, QUAL drew $1.5 billion, and IOZ received $1.0 billion. Domestic equity also remained strong: VAS pulled in $2.3 billion and A200 received $1.9 billion. The domestic-vs-international split was closer to parity than in prior years, reflecting growing comfort among Australian investors with global equity exposure.</p>
<p>Fixed income had another strong year. VBND attracted $1.0 billion, SUBD drew $910 million, and the category as a whole collected multi-billion-dollar inflows — cementing fixed income ETFs as a permanent feature of Australian portfolios rather than a rate-cycle novelty. QSML (VanEck MSCI International Small Companies Quality) attracted $913 million, reflecting new interest in small-cap quality international exposure. BGBL (BetaShares Global Shares ETF) drew $859 million in its ongoing ascent as a low-cost global equities vehicle.</p>

<hr class="my-6 border-gray-200">

<h2>Top Performing ETFs: US Tech Surges Again</h2>
<p>Return dispersion was wide in 2024. Among products with at least $500 million in AUM, FANG (Global X FANG+ ETF) delivered 65.1% — powered by the continued concentration of US market returns in a handful of mega-cap technology companies. HYGG (Hyperion Global Growth) returned 52.9% for its second consecutive strong year. LPGD returned 40.9%, IOO (iShares Global 100) delivered 39.1%, and PMGOLD (Perth Mint Physical Gold) returned 38.0% as gold reached all-time highs on geopolitical risk and central bank buying.</p>
<p>IVV (S&P 500) returned 37.0% and NDQ (Nasdaq 100) returned 38.0%, while VGS (global developed markets) returned 30.6% and QUAL returned 30.4% — an exceptional cohort of returns from broad global equity products.</p>
<p>At the bottom, bond and interest-rate-sensitive products lagged. USTB returned -1.1% and ILB (iShares Government Inflation) fell -0.9% as longer-duration fixed income struggled in the persistent higher-rate environment.</p>

<hr class="my-6 border-gray-200">

<h2>Fund Flows Leaders: Diversification at Scale</h2>
<p>For the first time, no single product captured a dominant share of flows. VAS led with $2.3 billion, but the ten largest flow recipients were spread across Australian equities (VAS, A200, IOZ), international equities (IVV, VGS, QUAL, BGBL), fixed income (VBND, SUBD), and small-cap (QSML). This diversification of flows signalled an industry that had moved beyond the early phase of a few dominant "go-to" products and into a more sophisticated multi-product allocation framework.</p>
<p>On the outflow side, FAIR (BetaShares Australian Sustainability Leaders) shed $422 million, and XARO (Alphinity Global Equity) lost $377 million. The ESG product category showed signs of flow pressure as investor enthusiasm for sustainability-labelled products moderated.</p>

<hr class="my-6 border-gray-200">

<h2>Asset Class Trends: Equities Dominant, Fixed Income Consolidates</h2>
<p>Equity ETFs grew to $195.2 billion (81.6% of total AUM), while fixed income reached $30.3 billion — a 24.7% increase from 2023's $24.3 billion. The "Other" category (primarily active and alternative ETPs) reached $5.4 billion, reflecting the growing scale of products outside traditional equity and bond categories, including the new crypto ETF segment. Money market products grew to $1.2 billion. Commodities rose to $6.3 billion, driven by gold's strong performance.</p>

<hr class="my-6 border-gray-200">


<div class="chart-grid">
  <div class="chart-box" style="margin:0">
    <h3>Asset Class Breakdown</h3>
    <div style="position:relative;height:220px"><canvas id="chart-ac-2024"></canvas></div>
  </div>
  <div class="chart-box" style="margin:0">
    <h3>Issuer Market Share</h3>
    <table>
      <thead><tr><th>Issuer</th><th>AUM</th><th>ETFs</th><th>Share</th></tr></thead>
      <tbody>
        <tr><td>Vanguard</td><td>$67.2B</td><td>29</td><td>30.4%</td></tr>
        <tr><td>Betashares</td><td>$44.5B</td><td>94</td><td>20.1%</td></tr>
        <tr><td>iShares</td><td>$42.4B</td><td>47</td><td>19.2%</td></tr>
        <tr><td>VanEck</td><td>$23.6B</td><td>43</td><td>10.7%</td></tr>
        <tr><td>Other</td><td>$15.4B</td><td>6</td><td>7.0%</td></tr>
        <tr><td>StateStreet</td><td>$9.7B</td><td>17</td><td>4.4%</td></tr>
        <tr><td>Magellan</td><td>$9.6B</td><td>4</td><td>4.3%</td></tr>
        <tr><td>Global X</td><td>$8.7B</td><td>37</td><td>3.9%</td></tr>
      </tbody>
    </table>
  </div>
</div>
<script>
(function() {
  const acLabels = ["International Equities", "Australian Equities", "Fixed Income", "Commodity", "Other", "Money Market", "Alternative", "Specialty", "Mixed Allocation"];
  const acData   = [134.7, 60.5, 30.3, 6.3, 5.4, 1.2, 0.5, 0.1, 0.1];
  const PALETTE  = ['#3b82f6','#f59e0b','#10b981','#ef4444','#8b5cf6','#ec4899','#06b6d4','#f97316','#84cc16','#6366f1'];
  new Chart(document.getElementById('chart-ac-2024'), {
    type: 'doughnut',
    data: { labels: acLabels, datasets: [{ data: acData, backgroundColor: PALETTE, borderWidth: 2, borderColor: '#fff' }] },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: 'right', labels: { boxWidth: 10, font: { size: 10 } } } }
    }
  });
})();
</script>

<div class="chart-box">
  <h3>Top 10 ETFs by AUM &#8212; 2024-12-31</h3>
  <table>
    <thead><tr><th>#</th><th>Code</th><th>Name</th><th>Issuer</th><th>AUM ($M)</th><th>1Y Return</th><th>Year Flows ($M)</th></tr></thead>
    <tbody>
      <tr><td>1</td><td><strong>VAS</strong></td><td>Vanguard Australian Shares Index ETF</td><td>Vanguard</td><td>$17,854M</td><td><span class="pos">+12.5%</span></td><td><span class="pos">+$2,299M</span></td></tr>
      <tr><td>2</td><td><strong>IVV</strong></td><td>iShares S&P 500 ETF</td><td>iShares</td><td>$11,036M</td><td><span class="pos">+37.0%</span></td><td><span class="pos">+$2,070M</span></td></tr>
      <tr><td>3</td><td><strong>VGS</strong></td><td>Vanguard MSCI Index International Shares ETF</td><td>Vanguard</td><td>$10,377M</td><td><span class="pos">+30.6%</span></td><td><span class="pos">+$1,924M</span></td></tr>
      <tr><td>4</td><td><strong>MGOC</strong></td><td>Magellan Global Fund - Open Class Units</td><td>Magellan</td><td>$8,380M</td><td><span class="pos">+29.7%</span></td><td><span class="pos">+$516M</span></td></tr>
      <tr><td>5</td><td><strong>QUAL</strong></td><td>VanEck MSCI International Quality ETF</td><td>VanEck</td><td>$7,139M</td><td><span class="pos">+30.4%</span></td><td><span class="pos">+$1,462M</span></td></tr>
      <tr><td>6</td><td><strong>A200</strong></td><td>BetaShares Australia 200 ETF</td><td>Betashares</td><td>$6,277M</td><td><span class="pos">+12.6%</span></td><td><span class="pos">+$1,895M</span></td></tr>
      <tr><td>7</td><td><strong>IOZ</strong></td><td>iShares Core S&P/ASX 200 ETF</td><td>iShares</td><td>$6,245M</td><td><span class="pos">+12.5%</span></td><td><span class="pos">+$1,015M</span></td></tr>
      <tr><td>8</td><td><strong>NDQ</strong></td><td>BetaShares NASDAQ 100 ETF</td><td>Betashares</td><td>$6,073M</td><td><span class="pos">+38.0%</span></td><td><span class="pos">+$825M</span></td></tr>
      <tr><td>9</td><td><strong>VTS</strong></td><td>Vanguard US Total Market Shares Index ETF</td><td>Vanguard</td><td>$5,548M</td><td><span class="pos">+35.8%</span></td><td><span class="pos">+$422M</span></td></tr>
      <tr><td>10</td><td><strong>STW</strong></td><td>SPDR S&P/ASX 200 Fund</td><td>StateStreet</td><td>$5,462M</td><td><span class="pos">+12.6%</span></td><td><span class="pos">+$149M</span></td></tr>
    </tbody>
  </table>
</div>

<div class="chart-grid">
  <div class="chart-box" style="margin:0">
    <h3>Top 5 Performers (min $500M AUM)</h3>
    <table>
      <thead><tr><th>Code</th><th>Name</th><th>Asset Class</th><th>1Y Return</th></tr></thead>
      <tbody>
      <tr><td><strong>FANG</strong></td><td>Global X FANG+ ETF</td><td>Equity</td><td><span class="pos">+65.1%</span></td></tr>
      <tr><td><strong>HYGG</strong></td><td>Hyperion Global Growth Companies Fund</td><td>Equity</td><td><span class="pos">+52.9%</span></td></tr>
      <tr><td><strong>LPGD</strong></td><td>Loftus Peak Global Disruption Fund</td><td>Equity</td><td><span class="pos">+40.9%</span></td></tr>
      <tr><td><strong>IOO</strong></td><td>iShares Global 100 ETF</td><td>Equity</td><td><span class="pos">+39.1%</span></td></tr>
      <tr><td><strong>PMGOLD</strong></td><td>Perth Mint Gold</td><td>Commodity</td><td><span class="pos">+38.0%</span></td></tr>
      </tbody>
    </table>
  </div>
  <div class="chart-box" style="margin:0">
    <h3>Worst 5 Performers (min $500M AUM)</h3>
    <table>
      <thead><tr><th>Code</th><th>Name</th><th>Asset Class</th><th>1Y Return</th></tr></thead>
      <tbody>
      <tr><td><strong>USTB</strong></td><td>Global X US Treasury Bond ETF (Currency Hedged)</td><td>Fixed Income</td><td><span class="neg">-1.1%</span></td></tr>
      <tr><td><strong>ILB</strong></td><td>iShares Government Inflation ETF</td><td>Fixed Income</td><td><span class="neg">-0.9%</span></td></tr>
      <tr><td><strong>DAVA</strong></td><td>Dimensional Australian Value Trust</td><td>—</td><td>0.0%</td></tr>
      <tr><td><strong>DGSM</strong></td><td>Dimensional Global Small Company Trust</td><td>—</td><td>0.0%</td></tr>
      <tr><td><strong>DGVA</strong></td><td>Dimensional Global Value Trust</td><td>—</td><td>0.0%</td></tr>
      </tbody>
    </table>
  </div>
</div>

<h2>New Launches: 54 Products Including Bitcoin ETFs</h2>
<p>Fifty-four new ETPs launched in 2024, sustaining the record-breaking pace of product development. The most significant category breakthrough was crypto: BTXX (DigitalX Bitcoin ETF) and several other bitcoin-related products launched, giving Australian investors regulated access to cryptocurrency through their brokerage accounts for the first time. Other notable launches included LEND (VanEck Global Listed Private Credit), reflecting interest in private credit as an asset class, and a range of currency-hedged variants of existing popular products.</p>

<hr class="my-6 border-gray-200">

<h2>Issuer Landscape: The Big Four Pull Away</h2>
<p>Vanguard grew to $67.2 billion, BetaShares to $44.5 billion, and iShares to $42.4 billion — three issuers now commanding over 60% of industry AUM. VanEck grew to $23.6 billion, driven by strong flows into QUAL, QSML, and fixed income products. The "Other" category grew to $15.4 billion, reflecting the continued expansion of the specialist and active management segment. Magellan stabilised at $9.6 billion — the first year since 2020 that its AUM did not decline sharply — while Global X grew to $8.7 billion.</p>
<p>December's monthly flow figures told the competitive story clearly: Vanguard attracted $1.3 billion in a single month, BetaShares $899 million, and iShares $760 million.</p>

<hr class="my-6 border-gray-200">

<h2>Fee Trends: Relentless Compression</h2>
<p>The AUM-weighted MER fell to 0.339% — down from 0.346% in 2023 and 0.389% in 2022. The simple average MER was 0.538%. The divergence between these two metrics (0.199 percentage points) was the widest yet, reflecting the continued concentration of assets in the industry's lowest-cost products. Vanguard's flagship ETFs — with MERs as low as 0.03–0.10% — were growing fastest, pulling the weighted average down even as the overall product shelf became more expensive on a simple-average basis.</p>

<hr class="my-6 border-gray-200">

<h2>Looking Back</h2>
<p>2024 was the year Australian ETF investing became definitively mainstream. With $239 billion in AUM, over 379 products, and $34 billion in new money entering the market, the ETF structure had effectively won the competition for Australian investors' new savings. The year also confirmed that Australian investors were not deterred by either elevated valuations or the memory of 2022 — they were buying systematically and broadly, a hallmark of a structurally committed investor base.</p>
""",
    },

    {
        "slug": "etf-year-review-2023",
        "title": "Australian ETF Year in Review: 2023",
        "subtitle": "After the bruising conditions of 2022, 2023 delivered a powerful recovery for most risk assets, with the Australian ETF industry growing from $130.4 billion to $173.0 billion.",
        "date": "2023-12-31",
        "category": "Annual Report",
        "summary": "After the bruising conditions of 2022, 2023 delivered a powerful recovery for most risk assets. US equities surged — led by the \"Magnificent Seven\" technology stocks — AI-related enthusiasm swept through markets, and the Australian ETF industry grew from $130.4 billion to $173.0 billion, a gain of 32.7%.",
        "body": """
<h2>Rebound, Resilience, and the Rise of Fixed Income</h2>
<p>After the bruising conditions of 2022, 2023 delivered a powerful recovery for most risk assets. US equities surged — led by the "Magnificent Seven" technology stocks — AI-related enthusiasm swept through markets, and bond yields, while elevated, stabilised enough to allow fixed income to attract meaningful flows. The Australian ETF industry grew from $130.4 billion to $173.0 billion, a gain of 32.7%, and net flows of $13.9 billion confirmed that investor appetite remained structurally robust even after the prior year's losses.</p>

<hr class="my-6 border-gray-200">

<div class="chart-box">
  <h3>Monthly AUM &amp; Net Flows &#8212; 2023</h3>
  <div style="position:relative;height:260px"><canvas id="chart-monthly-2023"></canvas></div>
</div>
<script>
(function() {
  const labels = ["2023-01", "2023-02", "2023-03", "2023-04", "2023-05", "2023-06", "2023-07", "2023-08", "2023-09", "2023-10", "2023-11", "2023-12"];
  const aums   = [135.05, 136.2, 138.79, 142.13, 143.46, 145.93, 149.42, 151.84, 148.15, 145.83, 165.08, 172.98];
  const flows  = [487.0, 797.0, 533.0, 716.0, 874.0, 735.0, 1032.0, 2090.0, 1643.0, 1652.0, 1964.0, 1419.0];
  new Chart(document.getElementById('chart-monthly-2023'), {
    data: {
      labels,
      datasets: [
        { type: 'line',  label: 'AUM ($B)',         data: aums,  borderColor: '#3b82f6', backgroundColor: '#3b82f620', fill: true,  tension: 0.3, pointRadius: 2, borderWidth: 2, yAxisID: 'yAUM' },
        { type: 'bar',   label: 'Net Flows ($M)',    data: flows, backgroundColor: flows.map(v => v >= 0 ? '#10b98180' : '#ef444480'), borderRadius: 2, yAxisID: 'yFlow' }
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: { legend: { position: 'bottom', labels: { boxWidth: 10, font: { size: 10 } } } },
      scales: {
        x:     { ticks: { font: { size: 10 } }, grid: { display: false } },
        yAUM:  { type: 'linear', position: 'left',  ticks: { font: { size: 10 }, callback: v => '$' + v + 'B' }, grid: { color: '#1e3860' } },
        yFlow: { type: 'linear', position: 'right', ticks: { font: { size: 10 }, callback: v => '$' + v + 'M' }, grid: { display: false } }
      }
    }
  });
})();
</script>


<h2>Industry AUM: A $42.6 Billion Recovery</h2>
<p>The industry began 2023 at $130.4 billion — below its 2021 year-end level — and closed at $173.0 billion, surpassing the prior peak by nearly $40 billion. Net flows contributed $13.9 billion of the increase, with the remaining $28.7 billion driven by market appreciation, particularly in US and global equity ETFs. A record 52 new ETPs launched during the year, expanding the product universe to 328 funds. Total traded value was $114.6 billion, slightly below the prior year's elevated level.</p>

<hr class="my-6 border-gray-200">

<h2>Net Flows: $13.9 Billion — Fixed Income Breaks Through</h2>
<p>While equities remained the dominant flow recipient, 2023 was the year fixed income ETFs truly arrived. With short-term interest rates offering yields not seen in over a decade, investors rotated meaningful capital into bond and hybrid products. SUBD (VanEck) attracted $772 million, AAA (BetaShares Australian High Interest Cash) drew $730 million, USTB (VanEck US Treasury Bond) received $538 million, and VBND (Vanguard Australian Fixed Interest Index) attracted $529 million. These figures represented a step-change from prior years, when fixed income flows had been a fraction of equity flows.</p>
<p>In equities, VAS again led the field with $1.5 billion, followed by A200 ($1.1 billion) and IOZ ($938 million). The Australian equity trio dominated domestic flows, while IVV added $511 million in international exposure. Notably, VHY (Vanguard Australian Shares High Yield) attracted $606 million, reflecting investors' continued preference for income in a higher-yield environment.</p>

<hr class="my-6 border-gray-200">

<h2>Top Performing ETFs: Tech's Revenge</h2>
<p>The 2023 return environment was almost a mirror image of 2022. Products that had fallen hardest bounced hardest. HYGG (Hyperion Global Growth), which had declined 43% in 2022, surged 70.1% to become the best-performing major ETF of the year among products with at least $500 million in AUM. NDQ (BetaShares NASDAQ 100) returned 53.6% as the technology sector roared back. HACK (BetaShares Global Cybersecurity) returned 39.0%, QUAL returned 30.6%, and MOAT (VanEck Morningstar Wide Moat) returned 30.6%.</p>
<p>The worst performers were at the other extreme: infrastructure-oriented products and Asia-focused ETFs lagged, with IFRA (iShares Global Infrastructure) returning -0.9%, and IAA (iShares Asia 50) returning just 1.4% as China's anticipated post-COVID recovery disappointed.</p>

<hr class="my-6 border-gray-200">

<h2>Fund Flows Leaders: Domestic Equities, Bonds — and Magellan Continues to Bleed</h2>
<p>The three major Australian equity ETFs (VAS, A200, and IOZ) collectively attracted $3.6 billion, demonstrating the enduring appeal of home-bias investing among Australian self-directed investors. SUBD and USTB captured the new fixed income interest, as higher yields made duration-light and short-maturity products attractive.</p>
<p>Magellan's outflows continued, albeit at a reduced pace: MGOC shed $2.5 billion, extending a multi-year redemption cycle that had seen the fund shrink from over $14 billion to $6.1 billion by year-end. GOLD (Global X) also experienced $512 million in outflows as gold's relative underperformance compared to surging equities prompted rotation. HYGG itself saw $264 million in outflows despite its exceptional return — a pattern consistent with investors locking in gains or reducing concentration in a single active manager.</p>

<hr class="my-6 border-gray-200">

<h2>Asset Class Trends: Fixed Income's Structural Moment</h2>
<p>The most significant structural development of 2023 was the acceleration in fixed income ETF growth. The asset class ended the year at $24.3 billion in AUM — up from $17.8 billion at the end of 2022, a 36.5% increase that outpaced equity AUM growth (12.0% from $107.0B to $142.1B, though note equity also benefited from strong performance). Fixed income's share of industry AUM rose to approximately 14.0%. Money market products also grew, reaching $1.1 billion as investors sought capital-stable yield.</p>
<p>Equity AUM recovered to $142.1 billion (82.1% of total), commodities reached $4.6 billion, and the alternative category grew to $500 million.</p>

<hr class="my-6 border-gray-200">


<div class="chart-grid">
  <div class="chart-box" style="margin:0">
    <h3>Asset Class Breakdown</h3>
    <div style="position:relative;height:220px"><canvas id="chart-ac-2023"></canvas></div>
  </div>
  <div class="chart-box" style="margin:0">
    <h3>Issuer Market Share</h3>
    <table>
      <thead><tr><th>Issuer</th><th>AUM</th><th>ETFs</th><th>Share</th></tr></thead>
      <tbody>
        <tr><td>Vanguard</td><td>$50.4B</td><td>29</td><td>31.4%</td></tr>
        <tr><td>Betashares</td><td>$31.7B</td><td>84</td><td>19.8%</td></tr>
        <tr><td>iShares</td><td>$29.9B</td><td>44</td><td>18.6%</td></tr>
        <tr><td>VanEck</td><td>$15.4B</td><td>38</td><td>9.6%</td></tr>
        <tr><td>Other</td><td>$11.0B</td><td>3</td><td>6.9%</td></tr>
        <tr><td>StateStreet</td><td>$8.7B</td><td>17</td><td>5.4%</td></tr>
        <tr><td>Magellan</td><td>$7.3B</td><td>4</td><td>4.5%</td></tr>
        <tr><td>Global X</td><td>$6.1B</td><td>32</td><td>3.8%</td></tr>
      </tbody>
    </table>
  </div>
</div>
<script>
(function() {
  const acLabels = ["International Equities", "Australian Equities", "Fixed Income", "Commodity", "Money Market", "Alternative", "Other", "Specialty", "Mixed Allocation"];
  const acData   = [92.8, 49.4, 24.4, 4.6, 1.1, 0.5, 0.2, 0.1, 0.1];
  const PALETTE  = ['#3b82f6','#f59e0b','#10b981','#ef4444','#8b5cf6','#ec4899','#06b6d4','#f97316','#84cc16','#6366f1'];
  new Chart(document.getElementById('chart-ac-2023'), {
    type: 'doughnut',
    data: { labels: acLabels, datasets: [{ data: acData, backgroundColor: PALETTE, borderWidth: 2, borderColor: '#fff' }] },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: 'right', labels: { boxWidth: 10, font: { size: 10 } } } }
    }
  });
})();
</script>

<div class="chart-box">
  <h3>Top 10 ETFs by AUM &#8212; 2023-12-29</h3>
  <table>
    <thead><tr><th>#</th><th>Code</th><th>Name</th><th>Issuer</th><th>AUM ($M)</th><th>1Y Return</th><th>Year Flows ($M)</th></tr></thead>
    <tbody>
      <tr><td>1</td><td><strong>VAS</strong></td><td>Vanguard Australian Shares Index ETF</td><td>Vanguard</td><td>$14,384M</td><td><span class="pos">+13.7%</span></td><td><span class="pos">+$1,542M</span></td></tr>
      <tr><td>2</td><td><strong>VGS</strong></td><td>Vanguard MSCI Index International Shares ETF</td><td>Vanguard</td><td>$6,490M</td><td><span class="pos">+23.0%</span></td><td><span class="pos">+$414M</span></td></tr>
      <tr><td>3</td><td><strong>IVV</strong></td><td>iShares S&P 500 ETF</td><td>iShares</td><td>$6,458M</td><td><span class="pos">+25.0%</span></td><td><span class="pos">+$511M</span></td></tr>
      <tr><td>4</td><td><strong>MGOC</strong></td><td>Magellan Global Fund - Open Class Units</td><td>Magellan</td><td>$6,058M</td><td><span class="pos">+20.8%</span></td><td><span class="neg">$-2,523M</span></td></tr>
      <tr><td>5</td><td><strong>STW</strong></td><td>SPDR S&P/ASX 200 Fund</td><td>StateStreet</td><td>$4,919M</td><td><span class="pos">+13.8%</span></td><td><span class="neg">$-22M</span></td></tr>
      <tr><td>6</td><td><strong>DACE</strong></td><td>Dimensional Australian Core Equity Trust</td><td>Other</td><td>$4,845M</td><td>&#8212;</td><td><span class="neg">$-13M</span></td></tr>
      <tr><td>7</td><td><strong>IOZ</strong></td><td>iShares Core S&P/ASX 200 ETF</td><td>iShares</td><td>$4,827M</td><td><span class="pos">+13.8%</span></td><td><span class="pos">+$938M</span></td></tr>
      <tr><td>8</td><td><strong>QUAL</strong></td><td>VanEck MSCI International Quality ETF</td><td>VanEck</td><td>$4,467M</td><td><span class="pos">+30.6%</span></td><td><span class="pos">+$600M</span></td></tr>
      <tr><td>9</td><td><strong>A200</strong></td><td>BetaShares Australia 200 ETF</td><td>Betashares</td><td>$3,978M</td><td><span class="pos">+13.7%</span></td><td><span class="pos">+$1,072M</span></td></tr>
      <tr><td>10</td><td><strong>NDQ</strong></td><td>BetaShares NASDAQ 100 ETF</td><td>Betashares</td><td>$3,781M</td><td><span class="pos">+53.6%</span></td><td><span class="pos">+$296M</span></td></tr>
    </tbody>
  </table>
</div>

<div class="chart-grid">
  <div class="chart-box" style="margin:0">
    <h3>Top 5 Performers (min $500M AUM)</h3>
    <table>
      <thead><tr><th>Code</th><th>Name</th><th>Asset Class</th><th>1Y Return</th></tr></thead>
      <tbody>
      <tr><td><strong>HYGG</strong></td><td>Hyperion Global Growth Companies Fund</td><td>Equity</td><td><span class="pos">+70.1%</span></td></tr>
      <tr><td><strong>NDQ</strong></td><td>BetaShares NASDAQ 100 ETF</td><td>Equity</td><td><span class="pos">+53.6%</span></td></tr>
      <tr><td><strong>HACK</strong></td><td>BetaShares Global Cybersecurity ETF</td><td>Equity</td><td><span class="pos">+39.0%</span></td></tr>
      <tr><td><strong>QUAL</strong></td><td>VanEck MSCI International Quality ETF</td><td>Equity</td><td><span class="pos">+30.6%</span></td></tr>
      <tr><td><strong>MOAT</strong></td><td>VanEck Morningstar Wide Moat ETF</td><td>Equity</td><td><span class="pos">+30.6%</span></td></tr>
      </tbody>
    </table>
  </div>
  <div class="chart-box" style="margin:0">
    <h3>Worst 5 Performers (min $500M AUM)</h3>
    <table>
      <thead><tr><th>Code</th><th>Name</th><th>Asset Class</th><th>1Y Return</th></tr></thead>
      <tbody>
      <tr><td><strong>IFRA</strong></td><td>VanEck FTSE Global Infrastructure (Hedged) ETF</td><td>Equity</td><td><span class="neg">-0.9%</span></td></tr>
      <tr><td><strong>DACE</strong></td><td>Dimensional Australian Core Equity Trust</td><td>Equity</td><td>0.0%</td></tr>
      <tr><td><strong>DFGH</strong></td><td>Dimensional Global Core Equity Trust - Hedged</td><td>Equity</td><td>0.0%</td></tr>
      <tr><td><strong>DGCE</strong></td><td>Dimensional Global Core Equity Trust - Unhedged</td><td>Equity</td><td>0.0%</td></tr>
      <tr><td><strong>IAA</strong></td><td>iShares Asia 50 ETF</td><td>Equity</td><td><span class="pos">+1.4%</span></td></tr>
      </tbody>
    </table>
  </div>
</div>

<h2>New Launches: 52 Products — A New Record</h2>
<p>Fifty-two new ETPs launched in 2023, setting a new annual record. The year saw significant innovation in covered-call products — AYLD, UYLD, and QYLD from Global X offered yield-enhancement strategies on Australian, US, and Nasdaq 100 indices respectively. Active management within ETF wrappers continued to expand, with ESG-focused credit (GOOD from Janus Henderson) and other specialist strategies debuting. The growing diversity of the product shelf reflected both issuer innovation and an investor base willing to engage with more complex strategies.</p>

<hr class="my-6 border-gray-200">

<h2>Issuer Landscape: VanEck's Rise, Magellan's Continued Slide</h2>
<p>Vanguard extended its lead with $50.4 billion in AUM. BetaShares overtook iShares to take second place with $31.7 billion versus iShares' $29.9 billion — a competitive milestone that confirmed BetaShares' status as the industry's most dynamic domestic player. VanEck grew to $15.4 billion (up from $10.9 billion), driven by strong flows into QUAL, SUBD, and USTB. Magellan fell further to $7.3 billion, barely above State Street's $8.7 billion. The "Other" category — comprising smaller and newer issuers — reached $11.0 billion, reflecting the growing long tail of specialist and active managers entering the ETF market.</p>

<hr class="my-6 border-gray-200">

<h2>Fee Trends: AUM-Weighted MER Falls Further</h2>
<p>The simple average MER was 0.547%, broadly stable year-on-year. The AUM-weighted MER fell to 0.346% — a notable compression from 0.389% in 2022 — as the largest, cheapest products (particularly at Vanguard and iShares) continued to grow faster than the average. This fee pressure was being felt most acutely by mid-tier issuers with products charging 0.40–0.70% competing against Vanguard's flagship sub-0.10% offerings.</p>

<hr class="my-6 border-gray-200">

<h2>Looking Back</h2>
<p>2023 confirmed that Australian ETF investors were not deterred by the prior year's losses — flows were positive even in the depths of the market decline, and when conditions improved, the industry recovered sharply. In retrospect, the year also marked the arrival of fixed income as a genuine growth category within Australian ETFs, a shift that would continue and accelerate as yields remained elevated into 2024.</p>
""",
    },

    {
        "slug": "etf-year-review-2022",
        "title": "Australian ETF Year in Review: 2022",
        "subtitle": "2022 was the most challenging year for investment returns since the Global Financial Crisis, yet Australian ETF investors continued to add $12.8 billion in net new flows.",
        "date": "2022-12-31",
        "category": "Annual Report",
        "summary": "2022 was the most challenging year for investment returns since the Global Financial Crisis. Central banks around the world launched aggressive interest rate hiking cycles to combat inflation, global equities fell sharply, bonds collapsed in tandem — yet Australian ETF investors continued to add money, demonstrating a structural shift in how Australians invest.",
        "body": """
<h2>The Year the Bull Market Ended</h2>
<p>2022 was the most challenging year for investment returns since the Global Financial Crisis. Central banks around the world — including the Reserve Bank of Australia — launched aggressive interest rate hiking cycles to combat inflation that had reached multi-decade highs. Global equities fell sharply, bonds collapsed in tandem (removing the traditional diversification benefit), and growth-oriented ETFs that had led the 2020–2021 rally suffered some of their worst-ever drawdowns. Yet despite the adverse conditions, Australian ETF investors continued to add money — $12.8 billion in net new flows — demonstrating a structural shift in how Australians invest.</p>

<hr class="my-6 border-gray-200">

<div class="chart-box">
  <h3>Monthly AUM &amp; Net Flows &#8212; 2022</h3>
  <div style="position:relative;height:260px"><canvas id="chart-monthly-2022"></canvas></div>
</div>
<script>
(function() {
  const labels = ["2022-01", "2022-02", "2022-03", "2022-04", "2022-05", "2022-06", "2022-07", "2022-08", "2022-09", "2022-10", "2022-11", "2022-12"];
  const aums   = [128.96, 127.16, 132.34, 130.36, 128.33, 121.45, 127.24, 126.94, 121.47, 128.51, 132.85, 130.44];
  const flows  = [1359.0, 156.0, 1236.0, 1157.0, 1428.0, 533.0, 2224.0, 547.0, 705.0, 1446.0, 997.0, 982.0];
  new Chart(document.getElementById('chart-monthly-2022'), {
    data: {
      labels,
      datasets: [
        { type: 'line',  label: 'AUM ($B)',         data: aums,  borderColor: '#3b82f6', backgroundColor: '#3b82f620', fill: true,  tension: 0.3, pointRadius: 2, borderWidth: 2, yAxisID: 'yAUM' },
        { type: 'bar',   label: 'Net Flows ($M)',    data: flows, backgroundColor: flows.map(v => v >= 0 ? '#10b98180' : '#ef444480'), borderRadius: 2, yAxisID: 'yFlow' }
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: { legend: { position: 'bottom', labels: { boxWidth: 10, font: { size: 10 } } } },
      scales: {
        x:     { ticks: { font: { size: 10 } }, grid: { display: false } },
        yAUM:  { type: 'linear', position: 'left',  ticks: { font: { size: 10 }, callback: v => '$' + v + 'B' }, grid: { color: '#1e3860' } },
        yFlow: { type: 'linear', position: 'right', ticks: { font: { size: 10 }, callback: v => '$' + v + 'M' }, grid: { display: false } }
      }
    }
  });
})();
</script>


<h2>Industry AUM: Flat Despite Strong Flows</h2>
<p>The Australian ETP industry began 2022 at $134.0 billion and ended the year at $130.4 billion — a decline of $3.6 billion despite $12.8 billion in net new flows. This means that market depreciation erased roughly $16.4 billion in value during the year. It was the first calendar-year decline in industry AUM in the modern ETF era. Forty-one new products launched during the year, bringing the total ETF count to 278. Total traded value rose to $117.3 billion — the highest since 2020's crisis-elevated level — as investors actively repositioned through a volatile year.</p>

<hr class="my-6 border-gray-200">

<h2>Net Flows: $12.8 Billion Despite the Bear Market</h2>
<p>The resilience of flows was perhaps the most significant story of 2022. Net flows of $12.8 billion (excluding admission months) represented a continuation of strong structural demand for ETFs despite a difficult return environment. Equity ETFs continued to dominate, with VAS attracting $2.6 billion — its highest annual intake to that point — as investors continued to buy Australian equities through the volatility. International equity also drew strong flows: VGS received $989 million, QUAL $666 million, VGAD $543 million, and VHY $535 million.</p>
<p>Notably, fixed income began attracting more serious attention, with HBRD (BetaShares Australian Major Bank Hybrids) drawing $405 million as rising yields made fixed income more attractive than it had been in years.</p>

<hr class="my-6 border-gray-200">

<h2>Top Performing ETFs: Value and Commodities Shine</h2>
<p>In a year when almost everything fell, the best performers were those with value, commodity, or domestic income tilts. Among products with at least $500 million in AUM, VHY (Vanguard Australian Shares High Yield) returned +11.3%, PMGOLD (Perth Mint Physical Gold) returned +7.0%, GOLD (Global X Physical Gold) returned +6.5%, and SFY (SPDR S&P/ASX 50) returned +4.1%. Energy exposure and domestic value stocks — heavily weighted in Australian large-cap — held up well as commodity prices surged.</p>
<p>The worst performers were the products that had soared in 2021. HYGG (Hyperion Global Growth) fell -43.0%, NDQ dropped -28.5%, QHAL (iShares Hedged International Equity) fell -23.0%, and HACK (BetaShares Global Cybersecurity) declined -22.1%. These were exactly the high-multiple, long-duration growth funds that were most exposed to the rise in discount rates. Investors who had poured money into these products in 2021 faced significant paper losses.</p>

<hr class="my-6 border-gray-200">

<h2>Fund Flows Leaders: VAS Dominant, MGOC in Freefall</h2>
<p>VAS continued its reign atop the flow tables with $2.6 billion for the year — suggesting that market declines were read as buying opportunities by Australian investors with a long-term perspective. A200 (BetaShares Australia 200) attracted $768 million and VGS $989 million.</p>
<p>The year's defining outflow story was MGOC (Magellan Global Open Class), which suffered $4.1 billion in net redemptions — by far the largest single-year outflow of any ETF in the industry's history at that point. The fund's underperformance relative to passive benchmarks, combined with Magellan's well-publicised corporate governance problems including the departure of founder Hamish Douglass, triggered a wave of institutional and retail redemptions. IOZ also saw $1.0 billion in outflows as some investors rotated out of the product in favour of lower-cost alternatives or chose different international exposures.</p>

<hr class="my-6 border-gray-200">

<h2>Asset Class Trends: Fixed Income Reclaims Relevance</h2>
<p>The most notable structural shift of 2022 was in fixed income. After years of negligible yields making bonds unattractive, the rapid rise in rates finally began drawing investors to fixed income ETFs. The asset class ended the year with $17.8 billion in AUM — up from $15.4 billion at the end of 2021 — and December's monthly flow of $881 million was the strongest single-month showing for fixed income in years. Equity ETFs fell to $107.0 billion (82.1% of total AUM), reflecting both price depreciation and Magellan's mass redemptions.</p>
<p>Commodities reached $4.2 billion, supported by gold's relative resilience.</p>

<hr class="my-6 border-gray-200">


<div class="chart-grid">
  <div class="chart-box" style="margin:0">
    <h3>Asset Class Breakdown</h3>
    <div style="position:relative;height:220px"><canvas id="chart-ac-2022"></canvas></div>
  </div>
  <div class="chart-box" style="margin:0">
    <h3>Issuer Market Share</h3>
    <table>
      <thead><tr><th>Issuer</th><th>AUM</th><th>ETFs</th><th>Share</th></tr></thead>
      <tbody>
        <tr><td>Vanguard</td><td>$41.2B</td><td>29</td><td>33.6%</td></tr>
        <tr><td>iShares</td><td>$23.8B</td><td>40</td><td>19.4%</td></tr>
        <tr><td>Betashares</td><td>$23.4B</td><td>75</td><td>19.1%</td></tr>
        <tr><td>VanEck</td><td>$10.9B</td><td>31</td><td>8.9%</td></tr>
        <tr><td>Magellan</td><td>$8.9B</td><td>4</td><td>7.2%</td></tr>
        <tr><td>StateStreet</td><td>$8.2B</td><td>17</td><td>6.7%</td></tr>
        <tr><td>Global X</td><td>$4.8B</td><td>24</td><td>3.9%</td></tr>
        <tr><td>Other</td><td>$1.6B</td><td>1</td><td>1.3%</td></tr>
      </tbody>
    </table>
  </div>
</div>
<script>
(function() {
  const acLabels = ["International Equities", "Australian Equities", "Fixed Income", "Commodity", "Money Market", "Alternative", "Specialty", "Other", "Mixed Allocation"];
  const acData   = [70.5, 36.5, 17.8, 4.2, 0.8, 0.4, 0.2, 0.1, 0.1];
  const PALETTE  = ['#3b82f6','#f59e0b','#10b981','#ef4444','#8b5cf6','#ec4899','#06b6d4','#f97316','#84cc16','#6366f1'];
  new Chart(document.getElementById('chart-ac-2022'), {
    type: 'doughnut',
    data: { labels: acLabels, datasets: [{ data: acData, backgroundColor: PALETTE, borderWidth: 2, borderColor: '#fff' }] },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: 'right', labels: { boxWidth: 10, font: { size: 10 } } } }
    }
  });
})();
</script>

<div class="chart-box">
  <h3>Top 10 ETFs by AUM &#8212; 2022-12-30</h3>
  <table>
    <thead><tr><th>#</th><th>Code</th><th>Name</th><th>Issuer</th><th>AUM ($M)</th><th>1Y Return</th><th>Year Flows ($M)</th></tr></thead>
    <tbody>
      <tr><td>1</td><td><strong>VAS</strong></td><td>Vanguard Australian Shares Index ETF</td><td>Vanguard</td><td>$11,844M</td><td><span class="neg">-0.3%</span></td><td><span class="pos">+$2,619M</span></td></tr>
      <tr><td>2</td><td><strong>MGOC</strong></td><td>Magellan Global Fund - Open Class Units</td><td>Magellan</td><td>$7,536M</td><td><span class="neg">-15.5%</span></td><td><span class="neg">$-4,134M</span></td></tr>
      <tr><td>3</td><td><strong>VGS</strong></td><td>Vanguard MSCI Index International Shares ETF</td><td>Vanguard</td><td>$5,023M</td><td><span class="neg">-12.4%</span></td><td><span class="pos">+$989M</span></td></tr>
      <tr><td>4</td><td><strong>IVV</strong></td><td>iShares S&P 500 ETF</td><td>iShares</td><td>$4,794M</td><td><span class="neg">-12.6%</span></td><td><span class="neg">$-16M</span></td></tr>
      <tr><td>5</td><td><strong>STW</strong></td><td>SPDR S&P/ASX 200 Fund</td><td>StateStreet</td><td>$4,612M</td><td><span class="pos">+0.9%</span></td><td><span class="pos">+$171M</span></td></tr>
      <tr><td>6</td><td><strong>IOZ</strong></td><td>iShares Core S&P/ASX 200 ETF</td><td>iShares</td><td>$3,561M</td><td><span class="pos">+1.1%</span></td><td><span class="neg">$-1,038M</span></td></tr>
      <tr><td>7</td><td><strong>QUAL</strong></td><td>VanEck MSCI International Quality ETF</td><td>VanEck</td><td>$2,969M</td><td><span class="neg">-17.0%</span></td><td><span class="pos">+$666M</span></td></tr>
      <tr><td>8</td><td><strong>VTS</strong></td><td>Vanguard US Total Market Shares Index ETF</td><td>Vanguard</td><td>$2,897M</td><td><span class="neg">-13.9%</span></td><td><span class="pos">+$178M</span></td></tr>
      <tr><td>9</td><td><strong>AAA</strong></td><td>BetaShares Australian High Interest Cash ETF</td><td>Betashares</td><td>$2,700M</td><td><span class="pos">+1.4%</span></td><td><span class="pos">+$65M</span></td></tr>
      <tr><td>10</td><td><strong>VHY</strong></td><td>Vanguard Australian Shares High Yield ETF</td><td>Vanguard</td><td>$2,643M</td><td><span class="pos">+11.3%</span></td><td><span class="pos">+$535M</span></td></tr>
    </tbody>
  </table>
</div>

<div class="chart-grid">
  <div class="chart-box" style="margin:0">
    <h3>Top 5 Performers (min $500M AUM)</h3>
    <table>
      <thead><tr><th>Code</th><th>Name</th><th>Asset Class</th><th>1Y Return</th></tr></thead>
      <tbody>
      <tr><td><strong>VHY</strong></td><td>Vanguard Australian Shares High Yield ETF</td><td>Equity</td><td><span class="pos">+11.3%</span></td></tr>
      <tr><td><strong>PMGOLD</strong></td><td>Perth Mint Gold</td><td>Commodity</td><td><span class="pos">+7.0%</span></td></tr>
      <tr><td><strong>GOLD</strong></td><td>Global X Physical Gold</td><td>Commodity</td><td><span class="pos">+6.5%</span></td></tr>
      <tr><td><strong>SFY</strong></td><td>SPDR S&P/ASX 50 Fund</td><td>Equity</td><td><span class="pos">+4.1%</span></td></tr>
      <tr><td><strong>HBRD</strong></td><td>BetaShares Active Australian Hybrids Fund</td><td>Fixed Income</td><td><span class="pos">+2.7%</span></td></tr>
      </tbody>
    </table>
  </div>
  <div class="chart-box" style="margin:0">
    <h3>Worst 5 Performers (min $500M AUM)</h3>
    <table>
      <thead><tr><th>Code</th><th>Name</th><th>Asset Class</th><th>1Y Return</th></tr></thead>
      <tbody>
      <tr><td><strong>HYGG</strong></td><td>Hyperion Global Growth Companies Fund</td><td>Equity</td><td><span class="neg">-43.0%</span></td></tr>
      <tr><td><strong>NDQ</strong></td><td>BetaShares NASDAQ 100 ETF</td><td>Equity</td><td><span class="neg">-28.5%</span></td></tr>
      <tr><td><strong>QHAL</strong></td><td>VanEck MSCI World Ex-Australia Quality (Hedged)</td><td>Equity</td><td><span class="neg">-23.0%</span></td></tr>
      <tr><td><strong>HACK</strong></td><td>BetaShares Global Cybersecurity ETF</td><td>Equity</td><td><span class="neg">-22.1%</span></td></tr>
      <tr><td><strong>IHVV</strong></td><td>iShares S&P 500 AUD Hedged ETF</td><td>Equity</td><td><span class="neg">-20.8%</span></td></tr>
      </tbody>
    </table>
  </div>
</div>

<h2>New Launches: A Record 41 Products</h2>
<p>Despite the difficult market environment, 41 new ETPs were admitted to the ASX — nearly double the prior year's tally. The wave included niche thematic products (GAME for video games and e-sports, IBUY for e-commerce) as well as ESG-focused and alternative products. The large number of launches reflected issuers' long product development pipelines and confidence in the structural growth of the ETF market even during a cyclical downturn.</p>

<hr class="my-6 border-gray-200">

<h2>Issuer Landscape: Magellan's Fall Reshapes the Rankings</h2>
<p>Vanguard consolidated its leadership position with $41.2 billion in AUM, growing despite the market decline through strong flows. BetaShares ($23.4 billion) pulled effectively level with iShares ($23.8 billion) in a significant competitive milestone. VanEck grew to $10.9 billion. Magellan fell from a peak of over $16 billion to $8.9 billion by year-end — a dramatic contraction driven almost entirely by MGOC redemptions rather than market performance alone. State Street held $8.2 billion.</p>

<hr class="my-6 border-gray-200">

<h2>Fee Trends: MER Creep from New Products</h2>
<p>The simple average MER rose to 0.555% as the wave of specialist and thematic product launches pushed the unweighted average higher. However, the AUM-weighted MER fell to 0.389% — a meaningful compression from 0.444% the prior year — as the largest products remained predominantly low-cost index funds. The growing divergence between the two metrics captured the dual-speed nature of the market: cheap core products growing rapidly at scale, and more expensive specialist products multiplying in number but remaining small.</p>

<hr class="my-6 border-gray-200">

<h2>Looking Back</h2>
<p>In retrospect, 2022 was a year that tested the conviction of Australian ETF investors — and found it solid. Flows never turned negative despite some of the worst return environments in decades. The year also completed Magellan's fall from dominance: what had been Australia's largest ETF in early 2022 would close the year having lost more than half its AUM, a cautionary tale about the vulnerability of high-profile active management products to performance and reputational risk.</p>
""",
    },

    {
        "slug": "etf-year-review-2021",
        "title": "Australian ETF Year in Review: 2021",
        "subtitle": "2021 was a year of extraordinary performance for global equity markets, with the Australian ETF industry growing from $94.4 billion to $134.0 billion — crossing $100 billion for the first time.",
        "date": "2021-12-31",
        "category": "Annual Report",
        "summary": "2021 was a year of extraordinary performance for global equity markets. Vaccine rollouts, fiscal stimulus, and surging corporate earnings pushed major indices to record highs. The Australian ETF industry mirrored this euphoria, growing from $94.4 billion to $134.0 billion in assets — a 42% expansion.",
        "body": """
<h2>The Bull Market Matures — and Magellan Cracks</h2>
<p>2021 was a year of extraordinary performance for global equity markets. Vaccine rollouts, fiscal stimulus, and surging corporate earnings pushed major indices to record highs. The Australian ETF industry mirrored this euphoria, growing from $94.4 billion to $134.0 billion in assets — a 42% expansion. Yet the year also contained the seeds of a major structural shift: Magellan Financial Group's flagship product began suffering significant outflows that would reshape the industry's competitive landscape for years to come.</p>

<hr class="my-6 border-gray-200">

<div class="chart-box">
  <h3>Monthly AUM &amp; Net Flows &#8212; 2021</h3>
  <div style="position:relative;height:260px"><canvas id="chart-monthly-2021"></canvas></div>
</div>
<script>
(function() {
  const labels = ["2021-01", "2021-02", "2021-03", "2021-04", "2021-05", "2021-06", "2021-07", "2021-08", "2021-09", "2021-10", "2021-11", "2021-12"];
  const aums   = [96.04, 96.57, 102.07, 106.67, 109.49, 113.52, 116.5, 122.78, 122.95, 124.64, 129.93, 133.97];
  const flows  = [1592.0, 1420.0, 1218.0, 1964.0, 1465.0, 976.0, 2738.0, 2433.0, 2855.0, 2357.0, 1726.0, 2617.0];
  new Chart(document.getElementById('chart-monthly-2021'), {
    data: {
      labels,
      datasets: [
        { type: 'line',  label: 'AUM ($B)',         data: aums,  borderColor: '#3b82f6', backgroundColor: '#3b82f620', fill: true,  tension: 0.3, pointRadius: 2, borderWidth: 2, yAxisID: 'yAUM' },
        { type: 'bar',   label: 'Net Flows ($M)',    data: flows, backgroundColor: flows.map(v => v >= 0 ? '#10b98180' : '#ef444480'), borderRadius: 2, yAxisID: 'yFlow' }
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: { legend: { position: 'bottom', labels: { boxWidth: 10, font: { size: 10 } } } },
      scales: {
        x:     { ticks: { font: { size: 10 } }, grid: { display: false } },
        yAUM:  { type: 'linear', position: 'left',  ticks: { font: { size: 10 }, callback: v => '$' + v + 'B' }, grid: { color: '#1e3860' } },
        yFlow: { type: 'linear', position: 'right', ticks: { font: { size: 10 }, callback: v => '$' + v + 'M' }, grid: { display: false } }
      }
    }
  });
})();
</script>


<h2>Industry AUM: $39.6 Billion Added in a Single Year</h2>
<p>Beginning the year at $94.4 billion, the industry closed 2021 at $134.0 billion — an increase of $39.6 billion. Net flows contributed $23.4 billion of that growth (excluding admission-month distortions), while market appreciation accounted for roughly $16 billion. It was a near-ideal environment: equities rose sharply, flows were strong, and new product launches kept the product universe expanding. Total traded value for the year was $95.7 billion, down slightly from 2020's crisis-elevated $98.7 billion but still reflecting a deeply liquid and active market.</p>

<hr class="my-6 border-gray-200">

<h2>Net Flows: $23.4 Billion — All Roads Lead to Equities</h2>
<p>Net flows of $23.4 billion were overwhelmingly directed into equity ETFs, which captured essentially all net new money. Australian broad-market equity remained the largest category by flows, led by VAS ($1.9 billion), but the year's defining theme was international equity — particularly US and global growth funds. VGS attracted $1.3 billion, NDQ drew $822 million, QUAL brought in $712 million, and IVV added $642 million. Investors were chasing the US equity rally with conviction, and diversified multi-asset products like VDHG (Vanguard Diversified High Growth) drew $856 million as the broader "set and forget" ETF investor base matured.</p>
<p>A striking new entrant to the flow leaders was HYGG (Hyperion Global Growth Companies Fund), a high-conviction active growth fund that launched during the year and drew $916 million in its first partial year — an extraordinary debut reflecting strong investor appetite for quality growth.</p>

<hr class="my-6 border-gray-200">

<h2>Top Performing ETFs: American Giants Dominate</h2>
<p>Performance in 2021 was led by US large-cap and growth exposures. Among products with at least $500 million in AUM, IVV (iShares S&P 500) returned 37.2%, NDQ returned 35.4%, VTS (Vanguard US Total Market) returned 34.1%, and QUAL returned 34.0%. IOO (iShares Global 100) returned 33.4%. Every top-performing product had heavy US or global tech exposure.</p>
<p>The worst performers were concentrated in Asian equities and bonds. ASIA returned -15.5%, reversing its exceptional 2020 gain as Chinese regulatory crackdowns devastated the technology sector that had driven its prior-year surge. The emerging markets exposure in IAA (iShares Asia 50) fell 5.5%. Bond funds including CRED, VGB, and IAF posted modest negative returns as inflation expectations began to rise and bond yields crept higher in the second half of the year.</p>

<hr class="my-6 border-gray-200">

<h2>Fund Flows Leaders: Equity Enthusiasm, Magellan's Reversal</h2>
<p>VAS again topped the flow tables at $1.9 billion, consolidating its position as Australia's most popular ETF for new money. But the year's defining flow story was the collapse in demand for MGOC (Magellan Global Open Class), which shed $1.4 billion in net outflows — a dramatic reversal for a product that had been Australia's largest ETF by AUM. Concerns about Magellan's performance, corporate governance, and co-founder departure began surfacing, and sophisticated investors began voting with their feet.</p>
<p>On the institutional side, MHHT and MSTR (Magellan-branded hedged products) each suffered over $100 million in outflows, suggesting the problem was systemic to the franchise rather than product-specific.</p>

<hr class="my-6 border-gray-200">

<h2>Asset Class Trends: Equities Extend Their Dominance</h2>
<p>Equity ETFs grew to $113.4 billion at year-end, representing 84.6% of industry AUM. Fixed income reached $15.4 billion, commodities $3.8 billion, and alternatives $400 million. The proportion of AUM in equities actually increased year-on-year, reflecting both the strong market performance of equities and investors' continued preference for equity exposure over bonds — even as interest rate risk began to build.</p>

<hr class="my-6 border-gray-200">


<div class="chart-grid">
  <div class="chart-box" style="margin:0">
    <h3>Asset Class Breakdown</h3>
    <div style="position:relative;height:220px"><canvas id="chart-ac-2021"></canvas></div>
  </div>
  <div class="chart-box" style="margin:0">
    <h3>Issuer Market Share</h3>
    <table>
      <thead><tr><th>Issuer</th><th>AUM</th><th>ETFs</th><th>Share</th></tr></thead>
      <tbody>
        <tr><td>Vanguard</td><td>$38.3B</td><td>30</td><td>30.0%</td></tr>
        <tr><td>iShares</td><td>$25.5B</td><td>36</td><td>20.0%</td></tr>
        <tr><td>Betashares</td><td>$22.2B</td><td>62</td><td>17.4%</td></tr>
        <tr><td>Magellan</td><td>$16.3B</td><td>4</td><td>12.8%</td></tr>
        <tr><td>VanEck</td><td>$10.0B</td><td>29</td><td>7.8%</td></tr>
        <tr><td>StateStreet</td><td>$8.5B</td><td>17</td><td>6.7%</td></tr>
        <tr><td>Global X</td><td>$4.7B</td><td>18</td><td>3.7%</td></tr>
        <tr><td>Other</td><td>$2.3B</td><td>1</td><td>1.8%</td></tr>
      </tbody>
    </table>
  </div>
</div>
<script>
(function() {
  const acLabels = ["International Equities", "Australian Equities", "Fixed Income", "Commodity", "Money Market", "Alternative", "Specialty", "Mixed Allocation", "Other"];
  const acData   = [78.6, 34.9, 15.4, 3.8, 0.6, 0.4, 0.2, 0.1, 0.1];
  const PALETTE  = ['#3b82f6','#f59e0b','#10b981','#ef4444','#8b5cf6','#ec4899','#06b6d4','#f97316','#84cc16','#6366f1'];
  new Chart(document.getElementById('chart-ac-2021'), {
    type: 'doughnut',
    data: { labels: acLabels, datasets: [{ data: acData, backgroundColor: PALETTE, borderWidth: 2, borderColor: '#fff' }] },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: 'right', labels: { boxWidth: 10, font: { size: 10 } } } }
    }
  });
})();
</script>

<div class="chart-box">
  <h3>Top 10 ETFs by AUM &#8212; 2021-12-31</h3>
  <table>
    <thead><tr><th>#</th><th>Code</th><th>Name</th><th>Issuer</th><th>AUM ($M)</th><th>1Y Return</th><th>Year Flows ($M)</th></tr></thead>
    <tbody>
      <tr><td>1</td><td><strong>MGOC</strong></td><td>Magellan Global Fund - Open Class Units</td><td>Magellan</td><td>$14,228M</td><td><span class="pos">+19.8%</span></td><td><span class="neg">$-1,394M</span></td></tr>
      <tr><td>2</td><td><strong>VAS</strong></td><td>Vanguard Australian Shares Index ETF</td><td>Vanguard</td><td>$10,124M</td><td><span class="pos">+18.7%</span></td><td><span class="pos">+$1,929M</span></td></tr>
      <tr><td>3</td><td><strong>IVV</strong></td><td>iShares S&P 500 ETF</td><td>iShares</td><td>$5,577M</td><td><span class="pos">+37.2%</span></td><td><span class="pos">+$642M</span></td></tr>
      <tr><td>4</td><td><strong>IOZ</strong></td><td>iShares Core S&P/ASX 200 ETF</td><td>iShares</td><td>$4,961M</td><td><span class="pos">+18.1%</span></td><td><span class="pos">+$772M</span></td></tr>
      <tr><td>5</td><td><strong>STW</strong></td><td>SPDR S&P/ASX 200 Fund</td><td>StateStreet</td><td>$4,817M</td><td><span class="pos">+18.2%</span></td><td><span class="pos">+$74M</span></td></tr>
      <tr><td>6</td><td><strong>VGS</strong></td><td>Vanguard MSCI Index International Shares ETF</td><td>Vanguard</td><td>$4,727M</td><td><span class="pos">+29.7%</span></td><td><span class="pos">+$1,280M</span></td></tr>
      <tr><td>7</td><td><strong>VTS</strong></td><td>Vanguard US Total Market Shares Index ETF</td><td>Vanguard</td><td>$3,220M</td><td><span class="pos">+34.1%</span></td><td><span class="pos">+$411M</span></td></tr>
      <tr><td>8</td><td><strong>QUAL</strong></td><td>VanEck MSCI International Quality ETF</td><td>VanEck</td><td>$2,831M</td><td><span class="pos">+34.0%</span></td><td><span class="pos">+$712M</span></td></tr>
      <tr><td>9</td><td><strong>NDQ</strong></td><td>BetaShares NASDAQ 100 ETF</td><td>Betashares</td><td>$2,819M</td><td><span class="pos">+35.4%</span></td><td><span class="pos">+$822M</span></td></tr>
      <tr><td>10</td><td><strong>IOO</strong></td><td>iShares Global 100 ETF</td><td>iShares</td><td>$2,721M</td><td><span class="pos">+33.4%</span></td><td><span class="pos">+$136M</span></td></tr>
    </tbody>
  </table>
</div>

<div class="chart-grid">
  <div class="chart-box" style="margin:0">
    <h3>Top 5 Performers (min $500M AUM)</h3>
    <table>
      <thead><tr><th>Code</th><th>Name</th><th>Asset Class</th><th>1Y Return</th></tr></thead>
      <tbody>
      <tr><td><strong>IVV</strong></td><td>iShares S&P 500 ETF</td><td>Equity</td><td><span class="pos">+37.2%</span></td></tr>
      <tr><td><strong>NDQ</strong></td><td>BetaShares NASDAQ 100 ETF</td><td>Equity</td><td><span class="pos">+35.4%</span></td></tr>
      <tr><td><strong>VTS</strong></td><td>Vanguard US Total Market Shares Index ETF</td><td>Equity</td><td><span class="pos">+34.1%</span></td></tr>
      <tr><td><strong>QUAL</strong></td><td>VanEck MSCI International Quality ETF</td><td>Equity</td><td><span class="pos">+34.0%</span></td></tr>
      <tr><td><strong>IOO</strong></td><td>iShares Global 100 ETF</td><td>Equity</td><td><span class="pos">+33.4%</span></td></tr>
      </tbody>
    </table>
  </div>
  <div class="chart-box" style="margin:0">
    <h3>Worst 5 Performers (min $500M AUM)</h3>
    <table>
      <thead><tr><th>Code</th><th>Name</th><th>Asset Class</th><th>1Y Return</th></tr></thead>
      <tbody>
      <tr><td><strong>ASIA</strong></td><td>BetaShares Asia Technology Tigers ETF</td><td>Equity</td><td><span class="neg">-15.5%</span></td></tr>
      <tr><td><strong>IAA</strong></td><td>iShares Asia 50 ETF</td><td>Equity</td><td><span class="neg">-5.5%</span></td></tr>
      <tr><td><strong>CRED</strong></td><td>BetaShares Australian Investment Grade Bond E</td><td>Fixed Income</td><td><span class="neg">-3.4%</span></td></tr>
      <tr><td><strong>VGB</strong></td><td>Vanguard Australian Government Bond Index ETF</td><td>Fixed Income</td><td><span class="neg">-3.3%</span></td></tr>
      <tr><td><strong>IAF</strong></td><td>iShares Core Composite Bond ETF</td><td>Fixed Income</td><td><span class="neg">-3.1%</span></td></tr>
      </tbody>
    </table>
  </div>
</div>

<h2>New Launches: 24 Products, Themes Diversify</h2>
<p>Twenty-four new ETPs launched in 2021. The year saw a wave of thematic products: CLDD (BetaShares Cloud Computing), CLNE (VanEck Global Clean Energy), ERTH (BetaShares Climate Change Innovation), and HYGG (Hyperion Global Growth). ESG-themed products also gained traction with the launch of IESG (iShares Core MSCI Australia ESG Leaders). The diversity of new launches signalled that issuers were moving beyond plain-vanilla beta to compete for investors interested in specific themes or active management within an ETF wrapper.</p>

<hr class="my-6 border-gray-200">

<h2>Issuer Landscape: BetaShares Closes the Gap</h2>
<p>Vanguard retained top position with $38.3 billion in AUM (up 48% from $25.8 billion), but the most dynamic mover was BetaShares, which grew from $14.4 billion to $22.2 billion — a 54% increase — overtaking iShares ($25.5 billion) was within reach. BetaShares' aggressive product development strategy and strong marketing to retail investors was paying clear dividends. VanEck also grew substantially, reaching $10.0 billion. Magellan, meanwhile, slipped from joint third position to fourth despite AUM growth to $16.3 billion, as outflows from MGOC masked the impact of market gains.</p>

<hr class="my-6 border-gray-200">

<h2>Fee Trends: Scale Drives AUM-Weighted Compression</h2>
<p>The simple average MER rose marginally to 0.534%, reflecting a growing cohort of newer, more specialised (and therefore higher-fee) products. However, the AUM-weighted MER fell to 0.444% — down from 0.457% in 2020 — as the largest, cheapest products continued to grow faster than the rest of the market. Vanguard and iShares, with MERs on flagship products often below 0.10%, were capturing disproportionate flows.</p>

<hr class="my-6 border-gray-200">

<h2>Looking Back</h2>
<p>2021 was the year the Australian ETF industry crossed $100 billion in AUM for the first time — a milestone that arrived faster than almost any forecast had predicted. In retrospect, the year also marked the peak of a growth-and-technology investment cycle that would begin to unravel in 2022, and the beginning of Magellan's prolonged institutional outflow problem that would reconfigure the industry's competitive map.</p>
""",
    },

    {
        "slug": "etf-year-review-2020",
        "title": "Australian ETF Year in Review: 2020",
        "subtitle": "A global pandemic triggered the sharpest equity market crash in decades — only to stage an equally stunning recovery — as the Australian ETF market closed the year with record inflows.",
        "date": "2020-12-31",
        "category": "Annual Report",
        "summary": "2020 was the most dramatic year in the history of the Australian ETF industry. A global pandemic triggered the sharpest equity market crash in decades, with Australian and global indices losing roughly a third of their value between February and March — only to stage an equally stunning recovery.",
        "body": """
<h2>A Year Defined by Crisis and Recovery</h2>
<p>2020 was the most dramatic year in the history of the Australian ETF industry. A global pandemic triggered the sharpest equity market crash in decades, with Australian and global indices losing roughly a third of their value between February and March — only to stage an equally stunning recovery. Against this backdrop of extreme volatility, the Australian ETF market not only survived but flourished, closing the year with record inflows and an industry reshaped by crisis-driven investor behaviour.</p>

<hr class="my-6 border-gray-200">

<div class="chart-box">
  <h3>Monthly AUM &amp; Net Flows &#8212; 2020</h3>
  <div style="position:relative;height:260px"><canvas id="chart-monthly-2020"></canvas></div>
</div>
<script>
(function() {
  const labels = ["2020-01", "2020-02", "2020-03", "2020-04", "2020-05", "2020-06", "2020-07", "2020-08", "2020-09", "2020-10", "2020-11", "2020-12"];
  const aums   = [65.68, 63.63, 56.89, 61.02, 63.73, 65.55, 66.9, 70.47, 71.12, 73.55, 92.07, 94.44];
  const flows  = [1915.0, 1512.0, 355.0, 1072.0, 1639.0, 1597.0, 1159.0, 1654.0, 2042.0, 2228.0, 2486.0, 2218.0];
  new Chart(document.getElementById('chart-monthly-2020'), {
    data: {
      labels,
      datasets: [
        { type: 'line',  label: 'AUM ($B)',         data: aums,  borderColor: '#3b82f6', backgroundColor: '#3b82f620', fill: true,  tension: 0.3, pointRadius: 2, borderWidth: 2, yAxisID: 'yAUM' },
        { type: 'bar',   label: 'Net Flows ($M)',    data: flows, backgroundColor: flows.map(v => v >= 0 ? '#10b98180' : '#ef444480'), borderRadius: 2, yAxisID: 'yFlow' }
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: { legend: { position: 'bottom', labels: { boxWidth: 10, font: { size: 10 } } } },
      scales: {
        x:     { ticks: { font: { size: 10 } }, grid: { display: false } },
        yAUM:  { type: 'linear', position: 'left',  ticks: { font: { size: 10 }, callback: v => '$' + v + 'B' }, grid: { color: '#1e3860' } },
        yFlow: { type: 'linear', position: 'right', ticks: { font: { size: 10 }, callback: v => '$' + v + 'M' }, grid: { display: false } }
      }
    }
  });
})();
</script>


<h2>Industry AUM: From Shock to Record Highs</h2>
<p>The Australian ETP industry began 2020 with $61.5 billion in assets under management. Despite the March sell-off, strong flows and a rapid market rebound propelled total AUM to $94.4 billion by year-end — a gain of $32.9 billion, or approximately 53%. The growth was driven by a combination of price appreciation and substantial net inflows. Net flows for the year (excluding admission-month distortions) reached $19.9 billion, meaning market performance contributed roughly $13 billion of the gain. Trading activity was exceptional, with $98.7 billion in total transacted value reflecting investors' heavy use of ETFs to reposition during the volatility.</p>

<hr class="my-6 border-gray-200">

<h2>Net Flows: $19.9 Billion in a Year of Extremes</h2>
<p>Full-year net flows of $19.9 billion represented a step-change for the industry. Equity ETFs dominated, capturing the lion's share as investors used the March drawdown as a buying opportunity. The most telling signal was that broad Australian equity products led inflows — suggesting that retail and self-managed super investors were buying the dip rather than fleeing to safety. Fixed income attracted $524 million into iShares' IAF alone, as investors also sought bond exposure during the uncertainty. Gold also had an exceptional year, with GOLD (Global X) attracting $826 million in net flows — reflecting demand for defensive assets.</p>

<hr class="my-6 border-gray-200">

<h2>Top Performing ETFs: Tech and Asia Lead, Cash Lags</h2>
<p>The return dispersion in 2020 was extreme. Among ETFs with at least $500 million in AUM, the best performer was the BetaShares Asia Technology Tigers ETF (ASIA) at +48.7%, followed by the BetaShares NASDAQ 100 ETF (NDQ) at +36.9% and the BetaShares Global Sustainability Leaders ETF (ETHI) at +30.6%. Technology and growth themes were supercharged by the pandemic-driven acceleration in digital adoption.</p>
<p>At the other end of the spectrum, cash-like instruments unsurprisingly delivered near-zero returns in an emergency rate-cut environment. MGOC (Magellan Global Open Class) posted a 0.0% 1-year return in the data, while BILL and AAA returned just 1.0% and 1.2% respectively as the RBA slashed rates toward zero.</p>

<hr class="my-6 border-gray-200">

<h2>Fund Flows Leaders: Australian Equities and Gold</h2>
<p>The top flow recipient for the year was VAS (Vanguard Australian Shares Index ETF), which attracted $2.3 billion in net new money — a record for the product at the time. IOZ (iShares Core S&P/ASX 200) followed with $1.6 billion. GOLD drew $826 million as investors sought inflation protection and safe-haven exposure amid unprecedented monetary stimulus. NDQ and QUAL (VanEck MSCI International Quality) each attracted roughly $550–570 million, capitalising on the quality-and-growth rotation.</p>
<p>Outflows were modest in absolute terms; the largest was IHEB (iShares J.P. Morgan USD Emerging Markets Bond) at -$101 million, reflecting the EM stress early in the year.</p>

<hr class="my-6 border-gray-200">

<h2>Asset Class Trends: Equities Dominant, Commodities Surge</h2>
<p>Equity ETFs ended the year with $77.1 billion in AUM — 81.7% of total industry assets. Fixed income held $12.6 billion (13.4%), while commodities reached $3.5 billion, boosted by gold's strong performance. The equity-dominated structure of the Australian ETF market was firmly established, with all other asset classes remaining comparatively small.</p>

<hr class="my-6 border-gray-200">


<div class="chart-grid">
  <div class="chart-box" style="margin:0">
    <h3>Asset Class Breakdown</h3>
    <div style="position:relative;height:220px"><canvas id="chart-ac-2020"></canvas></div>
  </div>
  <div class="chart-box" style="margin:0">
    <h3>Issuer Market Share</h3>
    <table>
      <thead><tr><th>Issuer</th><th>AUM</th><th>ETFs</th><th>Share</th></tr></thead>
      <tbody>
        <tr><td>Vanguard</td><td>$25.8B</td><td>30</td><td>28.2%</td></tr>
        <tr><td>iShares</td><td>$19.1B</td><td>35</td><td>20.9%</td></tr>
        <tr><td>Magellan</td><td>$14.4B</td><td>3</td><td>15.8%</td></tr>
        <tr><td>Betashares</td><td>$14.4B</td><td>56</td><td>15.8%</td></tr>
        <tr><td>StateStreet</td><td>$7.1B</td><td>17</td><td>7.8%</td></tr>
        <tr><td>VanEck</td><td>$6.4B</td><td>25</td><td>7.0%</td></tr>
        <tr><td>Global X</td><td>$3.3B</td><td>16</td><td>3.6%</td></tr>
        <tr><td>Other</td><td>$0.9B</td><td>5</td><td>1.0%</td></tr>
      </tbody>
    </table>
  </div>
</div>
<script>
(function() {
  const acLabels = ["International Equities", "Australian Equities", "Fixed Income", "Commodity", "Money Market", "Alternative", "Specialty", "Mixed Allocation", "Other"];
  const acData   = [51.1, 26.0, 12.6, 3.5, 0.7, 0.3, 0.2, 0.1, 0.0];
  const PALETTE  = ['#3b82f6','#f59e0b','#10b981','#ef4444','#8b5cf6','#ec4899','#06b6d4','#f97316','#84cc16','#6366f1'];
  new Chart(document.getElementById('chart-ac-2020'), {
    type: 'doughnut',
    data: { labels: acLabels, datasets: [{ data: acData, backgroundColor: PALETTE, borderWidth: 2, borderColor: '#fff' }] },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: 'right', labels: { boxWidth: 10, font: { size: 10 } } } }
    }
  });
})();
</script>

<div class="chart-box">
  <h3>Top 10 ETFs by AUM &#8212; 2020-12-31</h3>
  <table>
    <thead><tr><th>#</th><th>Code</th><th>Name</th><th>Issuer</th><th>AUM ($M)</th><th>1Y Return</th><th>Year Flows ($M)</th></tr></thead>
    <tbody>
      <tr><td>1</td><td><strong>MGOC</strong></td><td>Magellan Global Fund - Open Class Units</td><td>Magellan</td><td>$13,348M</td><td>&#8212;</td><td><span class="pos">+$402M</span></td></tr>
      <tr><td>2</td><td><strong>VAS</strong></td><td>Vanguard Australian Shares Index ETF</td><td>Vanguard</td><td>$7,170M</td><td><span class="pos">+13.7%</span></td><td><span class="pos">+$2,287M</span></td></tr>
      <tr><td>3</td><td><strong>STW</strong></td><td>SPDR S&P/ASX 200 Fund</td><td>StateStreet</td><td>$4,222M</td><td><span class="pos">+13.2%</span></td><td><span class="pos">+$505M</span></td></tr>
      <tr><td>4</td><td><strong>IOZ</strong></td><td>iShares Core S&P/ASX 200 ETF</td><td>iShares</td><td>$3,676M</td><td><span class="pos">+13.0%</span></td><td><span class="pos">+$1,642M</span></td></tr>
      <tr><td>5</td><td><strong>IVV</strong></td><td>iShares S&P 500 ETF</td><td>iShares</td><td>$3,572M</td><td><span class="pos">+18.9%</span></td><td><span class="neg">$-1M</span></td></tr>
      <tr><td>6</td><td><strong>VGS</strong></td><td>Vanguard MSCI Index International Shares ETF</td><td>Vanguard</td><td>$2,624M</td><td><span class="pos">+16.8%</span></td><td><span class="pos">+$337M</span></td></tr>
      <tr><td>7</td><td><strong>AAA</strong></td><td>BetaShares Australian High Interest Cash ETF</td><td>Betashares</td><td>$2,206M</td><td><span class="pos">+1.2%</span></td><td><span class="pos">+$415M</span></td></tr>
      <tr><td>8</td><td><strong>VTS</strong></td><td>Vanguard US Total Market Shares Index ETF</td><td>Vanguard</td><td>$2,084M</td><td><span class="pos">+20.2%</span></td><td><span class="pos">+$70M</span></td></tr>
      <tr><td>9</td><td><strong>GOLD</strong></td><td>Global X Physical Gold</td><td>Global X</td><td>$2,041M</td><td><span class="pos">+16.1%</span></td><td><span class="pos">+$826M</span></td></tr>
      <tr><td>10</td><td><strong>IOO</strong></td><td>iShares Global 100 ETF</td><td>iShares</td><td>$1,958M</td><td><span class="pos">+18.6%</span></td><td><span class="pos">+$40M</span></td></tr>
    </tbody>
  </table>
</div>

<div class="chart-grid">
  <div class="chart-box" style="margin:0">
    <h3>Top 5 Performers (min $500M AUM)</h3>
    <table>
      <thead><tr><th>Code</th><th>Name</th><th>Asset Class</th><th>1Y Return</th></tr></thead>
      <tbody>
      <tr><td><strong>ASIA</strong></td><td>BetaShares Asia Technology Tigers ETF</td><td>Equity</td><td><span class="pos">+48.7%</span></td></tr>
      <tr><td><strong>NDQ</strong></td><td>BetaShares NASDAQ 100 ETF</td><td>Equity</td><td><span class="pos">+36.9%</span></td></tr>
      <tr><td><strong>ETHI</strong></td><td>BetaShares Global Sustainability Leaders ETF</td><td>Equity</td><td><span class="pos">+30.6%</span></td></tr>
      <tr><td><strong>QUAL</strong></td><td>VanEck MSCI International Quality ETF</td><td>Equity</td><td><span class="pos">+23.3%</span></td></tr>
      <tr><td><strong>IAA</strong></td><td>iShares Asia 50 ETF</td><td>Equity</td><td><span class="pos">+21.6%</span></td></tr>
      </tbody>
    </table>
  </div>
  <div class="chart-box" style="margin:0">
    <h3>Worst 5 Performers (min $500M AUM)</h3>
    <table>
      <thead><tr><th>Code</th><th>Name</th><th>Asset Class</th><th>1Y Return</th></tr></thead>
      <tbody>
      <tr><td><strong>MGOC</strong></td><td>Magellan Global Fund - Open Class Units</td><td>Equity</td><td>0.0%</td></tr>
      <tr><td><strong>BILL</strong></td><td>iShares Core Cash ETF</td><td>Money Market</td><td><span class="pos">+1.0%</span></td></tr>
      <tr><td><strong>AAA</strong></td><td>BetaShares Australian High Interest Cash ETF</td><td>Fixed Income</td><td><span class="pos">+1.2%</span></td></tr>
      <tr><td><strong>QPON</strong></td><td>BetaShares Australian Bank Senior Floating Ra</td><td>Fixed Income</td><td><span class="pos">+3.1%</span></td></tr>
      <tr><td><strong>HBRD</strong></td><td>BetaShares Active Australian Hybrids Fund</td><td>Fixed Income</td><td><span class="pos">+5.1%</span></td></tr>
      </tbody>
    </table>
  </div>
</div>

<h2>New Launches: 23 Products in a Volatile Year</h2>
<p>Despite the market turmoil, 23 new ETPs were admitted to the ASX in 2020. Notable debuts included FANG (Global X FANG+ ETF), providing targeted exposure to mega-cap US tech; ATEC (BetaShares S&P/ASX Australian Technology ETF), which tapped into the domestic tech boom; and LNAS (Global X Ultra Long Nasdaq 100 Hedge Fund), a leveraged product catering to sophisticated investors. The diversity of launches signalled that issuers saw the pandemic dip as an opportunity rather than a deterrent.</p>

<hr class="my-6 border-gray-200">

<h2>Issuer Landscape: The Big Four Hold Firm</h2>
<p>Vanguard ended 2020 as the clear market leader with $25.8 billion in AUM, followed by iShares at $19.1 billion. Magellan and BetaShares were tied at approximately $14.4 billion each, with Magellan's position largely reflecting the enormous MGOC product. State Street held $7.1 billion and VanEck $6.4 billion. BetaShares was the most active product launcher and was gaining ground on the established index giants.</p>

<hr class="my-6 border-gray-200">

<h2>Fee Trends: Fees Compress as Scale Grows</h2>
<p>The average management expense ratio (MER) across the industry stood at 0.504%, while the AUM-weighted MER — a better measure of what investors actually paid — was 0.457%. The divergence between these figures reflected the concentration of assets in lower-cost index products run by Vanguard and iShares, pulling the weighted average well below the simple mean.</p>

<hr class="my-6 border-gray-200">

<h2>Looking Back</h2>
<p>2020 demonstrated that ETFs are particularly well-suited to crisis environments — they provided intraday liquidity and price discovery when active funds were gating or suspending redemptions. The year's record inflows during a bear market confirmed a structural shift: Australian investors were increasingly turning to exchange-traded vehicles as their primary investment tool, not just a complement to managed funds.</p>
""",
    },

    {
        "slug": "etf-fund-flows-march-2026",
        "title": "Where the Money Went: $5.2 Billion in ETF Inflows",
        "subtitle": "Australian investors poured a record $5.2 billion into ETFs last month. International equities led, fixed income surged, and Magellan kept bleeding.",
        "date": "2026-03-17",
        "category": "Market Trends",
        "summary": "The latest monthly fund flow data shows $5.2 billion in net ETF inflows across 466 funds. We break down where capital is flowing — and where it is leaving.",
        "body": """
<p>Australian investors added a net <strong>$5.24 billion</strong> to ETFs last month, spread across 466 funds. It is one of the strongest monthly flow figures on record, and the breakdown tells a clear story about where investor conviction currently sits.</p>

<h2>International equities dominate</h2>
<p>The single largest category by inflows was <strong>international equities</strong> at $2.38 billion — nearly half of all net flows. VGS led all individual ETFs with <strong>$328 million</strong> in inflows, its strongest month in recent memory, followed closely by VAS ($316M) and A200 ($218M). iShares IVV (S&P 500) attracted $136 million.</p>

<p>The preference for internationally-diversified index ETFs is consistent with the theme that has driven ETF market growth for several years: retail investors and self-managed super funds systematically rebalancing away from concentrated domestic exposure toward global diversification. The continued narrowing of fees — IVV at 0.04%, BGBL at 0.08% — makes this an increasingly cheap decision.</p>

<table>
  <thead><tr><th>ETF</th><th>Name</th><th>Category</th><th>Monthly Inflow</th><th>Total FUM</th></tr></thead>
  <tbody>
    <tr><td>VGS</td><td>Vanguard MSCI International Shares</td><td>Int'l Equities</td><td class="pos">+$328M</td><td>$14.0B</td></tr>
    <tr><td>VAS</td><td>Vanguard Australian Shares</td><td>Aus Equities</td><td class="pos">+$316M</td><td>$22.7B</td></tr>
    <tr><td>A200</td><td>BetaShares Australia 200</td><td>Aus Equities</td><td class="pos">+$218M</td><td>$9.2B</td></tr>
    <tr><td>BGBL</td><td>BetaShares Global Shares</td><td>Int'l Equities</td><td class="pos">+$187M</td><td>$3.4B</td></tr>
    <tr><td>IVV</td><td>iShares S&amp;P 500</td><td>Int'l Equities</td><td class="pos">+$136M</td><td>$12.4B</td></tr>
    <tr><td>VHY</td><td>Vanguard Australian High Yield</td><td>Aus Equities</td><td class="pos">+$119M</td><td>$6.8B</td></tr>
    <tr><td>ETPMAG</td><td>Global X Physical Silver</td><td>Commodities</td><td class="pos">+$115M</td><td>$2.4B</td></tr>
    <tr><td>WIRE</td><td>Global X Copper Miners</td><td>Commodities</td><td class="pos">+$109M</td><td>$672M</td></tr>
  </tbody>
</table>

<h2>Fixed income makes a comeback</h2>
<p>The second surprise of the month is the strength of <strong>fixed income</strong> flows at $917 million — the asset class's strongest showing in over a year. VAF (Vanguard Australian Fixed Interest) attracted $93 million; AAA (BetaShares Cash) added $92 million; SUBD (VanEck Subordinated Debt) pulled in $76 million. MONY, VanEck's newly-launched Cash Plus Active ETF, attracted exactly $100 million — its entire FUM — in its first month of operation.</p>

<p>The fixed income renaissance reflects growing conviction that Australian interest rates have peaked. As the RBA has held rates steady for several consecutive meetings, term deposit rates are beginning to ease, making bond ETF duration risk look more attractive at current yield levels. The 5.35% running yield on SUBD is competitive with any term deposit, with the added benefit of daily liquidity.</p>

<h2>Commodities momentum continued</h2>
<p>Commodity ETFs attracted $491 million in net flows despite already carrying very elevated recent returns. ETPMAG (physical silver, +212% 1Y) still pulled in $115 million — suggesting momentum investors remain committed even at current prices. WIRE (copper miners, +109% 1Y) added $109 million. GXLD (Global X physical gold) attracted $69 million.</p>

<h2>Digital assets: indifference despite volatility</h2>
<p>Crypto ETFs attracted just <strong>$6 million</strong> in aggregate net flows despite significant price moves in both directions. VBTC, IBTC, and EBTC are all down roughly 29% over the past year. Investors appear to be neither panic-selling (which would show up as large outflows) nor adding aggressively to falling prices. The category is being effectively ignored.</p>

<h2>The outflows: where money is leaving</h2>
<p>The most notable outflows came from a handful of specific situations:</p>

<table>
  <thead><tr><th>ETF</th><th>Name</th><th>Monthly Outflow</th><th>Context</th></tr></thead>
  <tbody>
    <tr><td>GEAR</td><td>BetaShares Geared Australian Equities</td><td class="neg">-$49M</td><td>Leveraged product, position-sizing</td></tr>
    <tr><td>QRE</td><td>BetaShares Australian Resources Sector</td><td class="neg">-$43M</td><td>Profit-taking after strong resources run</td></tr>
    <tr><td>MGOC</td><td>Magellan Global Fund (Open Class)</td><td class="neg">-$38M</td><td>Ongoing manager redemptions; -5.1% 1Y</td></tr>
    <tr><td>OZR</td><td>SPDR S&amp;P/ASX 200 Resources</td><td class="neg">-$21M</td><td>Resources sector rotation</td></tr>
    <tr><td>GRNV</td><td>VanEck MSCI Australian Sustainable</td><td class="neg">-$21M</td><td>ESG outflow trend continues</td></tr>
  </tbody>
</table>

<p>Magellan's continued outflows are notable. MGOC has now shed over $9 billion from its peak FUM of $14+ billion. At $5.2 billion remaining with ongoing monthly outflows, the question is where the floor is. The fund's -5.1% one-year return makes the 1.35% management fee particularly difficult to justify when index alternatives charge a fraction of that cost.</p>

<p>The resources outflow from QRE and OZR suggests some profit-taking after a strong run in commodities. Interestingly, investors appear to be rotating out of broad resources ETFs while simultaneously adding to specific metals themes (silver, copper) — a preference for targeted thematic exposure over broad sector coverage.</p>

<h2>The big picture</h2>
<p>Total Australian ETF market FUM now sits at approximately <strong>$336 billion</strong> across 472 products. At $5.2 billion in a single month, net new investment represents about 1.5% of the market being added per month — a pace that, if sustained, would push the market through $400 billion before the end of 2026. The structural growth story in Australian ETFs remains firmly intact.</p>
""",
    },

    {
        "slug": "defence-etf-surge-2026",
        "title": "Defence ETFs: The Unlikely Stars of the Past Year",
        "subtitle": "Three defence ETFs returned between 55% and 72% as NATO spending commitments and geopolitical realignment drove defence contractors to record valuations.",
        "date": "2026-03-17",
        "category": "Thematic",
        "summary": "DFND, ARMR and DTEC have surged 55–72% in twelve months, collectively attracting $60 million in new investment last month alone. We profile the funds, their holdings, and the geopolitical backdrop driving the rally.",
        "body": """
<p>Defence stocks are not a typical destination for Australian ETF investors. The sector is dominated by US and European contractors, carries political complexity, and has historically been overlooked by the retail market. But over the past twelve months, three defence-focused ETFs available on the ASX have delivered some of the strongest returns of any thematic category.</p>

<table>
  <thead><tr><th>ETF</th><th>Name</th><th>Issuer</th><th>1Y Return</th><th>FUM</th><th>Fee</th><th>Last Month Inflow</th></tr></thead>
  <tbody>
    <tr><td>DTEC</td><td>Global X Defence Tech ETF</td><td>Global X</td><td class="pos">+72.0%</td><td>$136M</td><td>0.50%</td><td class="pos">+$11.7M</td></tr>
    <tr><td>DFND</td><td>VanEck Global Defence ETF</td><td>VanEck</td><td class="pos">+68.7%</td><td>$312M</td><td>0.65%</td><td class="pos">+$24.9M</td></tr>
    <tr><td>ARMR</td><td>BetaShares Global Defence ETF</td><td>BetaShares</td><td class="pos">+54.9%</td><td>$248M</td><td>0.55%</td><td class="pos">+$24.2M</td></tr>
  </tbody>
</table>

<p>Combined, these three funds manage <strong>$696 million</strong> and attracted <strong>$60.8 million</strong> in new investment last month — a meaningful vote of confidence from investors who believe the rally has further to run.</p>

<h2>What is driving defence stocks higher</h2>
<p>The defence sector has been transformed by a series of geopolitical shocks that have fundamentally altered Western governments' attitudes toward defence spending:</p>

<ul>
  <li><strong>NATO commitments</strong>: Following Russia's invasion of Ukraine, NATO members agreed to raise defence spending targets from 2% to potentially 3% of GDP. Most European members had been well below even the original 2% target for years. Closing that gap requires hundreds of billions in new procurement over the next decade.</li>
  <li><strong>European rearmament</strong>: Germany broke a decades-long taboo by committing to a €100 billion special defence fund — the Sondervermögen — in 2022. That spending is now flowing into contracts for companies like Rheinmetall, Thales, Leonardo, and Saab.</li>
  <li><strong>Indo-Pacific tensions</strong>: Growing concerns about Taiwan and broader Asia-Pacific security have driven increased defence procurement in the region, boosting South Korean contractors like Hanwha Aerospace.</li>
  <li><strong>Drone warfare revolution</strong>: The Ukraine conflict demonstrated that autonomous systems, electronic warfare, and precision strike capabilities now dominate land warfare. This is creating demand for a new generation of defence technology rather than just legacy platforms.</li>
</ul>

<h2>Inside the holdings: same names, different weights</h2>
<p>The three funds overlap significantly in their core holdings but make different bets on specific companies:</p>

<table>
  <thead><tr><th>Company</th><th>DFND weight</th><th>ARMR weight</th><th>DTEC weight</th></tr></thead>
  <tbody>
    <tr><td>Lockheed Martin</td><td>—</td><td>9.7%</td><td>9.2%</td></tr>
    <tr><td>RTX Corp (Raytheon)</td><td>7.7%</td><td>8.0%</td><td>7.9%</td></tr>
    <tr><td>Northrop Grumman</td><td>—</td><td>8.1%</td><td>4.9%</td></tr>
    <tr><td>General Dynamics</td><td>—</td><td>7.1%</td><td>6.9%</td></tr>
    <tr><td>BAE Systems</td><td>—</td><td>7.0%</td><td>4.9%</td></tr>
    <tr><td>Rheinmetall</td><td>—</td><td>6.3%</td><td>6.2%</td></tr>
    <tr><td>Hanwha Aerospace</td><td>7.7%</td><td>—</td><td>5.0%</td></tr>
    <tr><td>Leonardo</td><td>7.6%</td><td>—</td><td>—</td></tr>
    <tr><td>Thales</td><td>7.3%</td><td>—</td><td>—</td></tr>
    <tr><td>Saab</td><td>6.2%</td><td>—</td><td>—</td></tr>
    <tr><td>Elbit Systems</td><td>6.0%</td><td>—</td><td>—</td></tr>
    <tr><td>Palantir Technologies</td><td>5.4%</td><td>5.7%</td><td>6.1%</td></tr>
    <tr><td>Curtiss-Wright</td><td>5.1%</td><td>—</td><td>—</td></tr>
    <tr><td>Safran</td><td>—</td><td>6.9%</td><td>—</td></tr>
  </tbody>
</table>

<p><strong>DFND</strong> (VanEck) is the most European-weighted of the three, with significant allocations to Hanwha (South Korea), Leonardo (Italy), Thales (France), Saab (Sweden), and Elbit (Israel). This mix has benefited from European rearmament and Middle East tensions driving Israeli defence contractors.</p>

<p><strong>ARMR</strong> (BetaShares) skews more toward traditional US prime contractors — Lockheed Martin, Northrop Grumman, General Dynamics — reflecting the established US defence industrial base.</p>

<p><strong>DTEC</strong> (Global X) emphasises defence technology and dual-use tech over pure-play contractors. Its Palantir position (6.1%) is the largest across the three funds — a bet on AI-enabled defence intelligence and autonomous systems rather than legacy hardware.</p>

<h2>Palantir: the cross-fund consensus</h2>
<p>One name appears in all three ETFs: <strong>Palantir Technologies</strong>, the data analytics and AI company whose government and defence contracts have become central to its growth story. Palantir's Gotham platform is used extensively by US defence and intelligence agencies; its AIP (AI Platform) product has been increasingly deployed by military customers. Palantir has been one of the top-performing S&P 500 stocks over the past year, contributing meaningfully to all three defence ETFs' returns.</p>

<h2>Fees and structure</h2>
<p>At 0.50–0.65%, defence ETFs charge a thematic premium over plain index ETFs — consistent with the broader pattern where specialised exposures cost more than vanilla market-cap products. ARMR at 0.55% is the cheapest; DFND at 0.65% is the most expensive but also the most differentiated in its European and emerging market defence exposure.</p>

<h2>The risks</h2>
<p>The geopolitical events that drove these returns could also reverse them. A durable ceasefire in Ukraine, renewed multilateral disarmament negotiations, or a US policy shift on defence spending commitments could quickly re-rate defence stocks downward. Some ESG frameworks explicitly exclude defence companies, meaning some institutional investors cannot hold these ETFs regardless of valuation. And concentration in the sector is real — all three funds are entirely dependent on one industry's fortunes.</p>

<p>For investors who are comfortable with the sector exposure and believe the structural uplift in Western defence spending is durable, any of these three funds provides genuine access to a theme that was previously difficult to express through listed products in Australia. The choice between them comes down to your geographic preference: more US if you like ARMR, more European if DFND suits, more defence-tech and AI if DTEC appeals.</p>
""",
    },

    {
        "slug": "silver-surge-etpmag-2026",
        "title": "The Silver Surge: ETPMAG's 212% Year in Review",
        "subtitle": "Physical silver delivered the best return of any Australian ETF over the past 12 months — and the case for further gains may not be over.",
        "date": "2026-03-16",
        "category": "Performance",
        "summary": "Global X's physical silver structured product returned 212% over the past year, outpacing gold, copper and every equity ETF on the ASX. We look at what drove the move and what investors are paying to get exposure.",
        "body": """
<p>When most investors talk about precious metals exposure, they reach for gold. But over the past twelve months, silver has quietly — then loudly — stolen the show. <strong>ETPMAG</strong>, Global X's Physical Silver structured product, returned <strong>212.6%</strong> in the year to March 2026, making it the best-performing ETF on the Australian market by a wide margin. Its three-year return sits at 366%.</p>

<p>The fund now holds <strong>$2.38 billion</strong> in assets, with investors piling in as silver prices surged on a combination of industrial demand and safe-haven buying. The annual management cost is 0.49% — modest for a physically-backed commodity product.</p>

<h2>Why silver outran gold</h2>
<p>Silver occupies an unusual position in commodity markets: it is simultaneously a precious metal and an industrial input. Around 60% of annual silver demand comes from industrial uses, including solar panel manufacturing, EV components, and electronics. The green energy build-out has structurally increased industrial silver consumption at exactly the moment that investment demand has also risen.</p>

<p>By comparison, Global X's Physical Gold product (GOLD) — Australia's fourth-largest ETF with <strong>$7.0 billion</strong> in assets — returned 64.7% over the same period. A strong result by any measure, but a third of silver's pace. Gold miner ETFs performed better than physical gold: VanEck's GDX returned 137.9% and BetaShares' MNRS returned 162.2%, reflecting the operating leverage miners carry into rising spot prices.</p>

<h2>The full precious metals leaderboard</h2>
<table>
  <thead><tr><th>ETF</th><th>Name</th><th>1Y Return</th><th>3Y Return</th><th>FUM</th></tr></thead>
  <tbody>
    <tr><td>ETPMAG</td><td>Global X Physical Silver</td><td class="pos">+212.6%</td><td class="pos">+366.0%</td><td>$2.38B</td></tr>
    <tr><td>MNRS</td><td>BetaShares Global Gold Miners (Hdg)</td><td class="pos">+162.2%</td><td class="pos">+219.8%</td><td>$298M</td></tr>
    <tr><td>GDX</td><td>VanEck Gold Miners ETF</td><td class="pos">+137.9%</td><td class="pos">+234.1%</td><td>$1.78B</td></tr>
    <tr><td>ETPMPT</td><td>Global X Physical Platinum</td><td class="pos">+129.5%</td><td class="pos">+147.9%</td><td>$117M</td></tr>
    <tr><td>QAU</td><td>BetaShares Gold Bullion (Hdg)</td><td class="pos">+81.2%</td><td class="pos">+153.1%</td><td>$1.72B</td></tr>
    <tr><td>NUGG</td><td>VanEck Gold Bullion ETF</td><td class="pos">+65.4%</td><td class="pos">+170.2%</td><td>$274M</td></tr>
    <tr><td>GOLD</td><td>Global X Physical Gold</td><td class="pos">+64.7%</td><td class="pos">+169.1%</td><td>$7.02B</td></tr>
  </tbody>
</table>

<h2>What to watch</h2>
<p>Silver's dual nature cuts both ways. A global industrial slowdown would remove a key demand driver, and the metal is far more volatile than gold. ETPMAG's current price of $146.10 reflects a very long run from recent lows. Investors considering entry here should be clear-eyed about the volatility profile — the same leverage that produced triple-digit gains can produce sharp drawdowns.</p>

<p>For long-term allocators, the structurally rising industrial demand from solar and EVs provides a more durable thesis than pure monetary demand. Whether that justifies a 212% re-rating in twelve months is a question of conviction.</p>
""",
    },

    {
        "slug": "active-etf-wave-2026",
        "title": "Australia's Active ETF Wave: 143 Funds, $60 Billion and Counting",
        "subtitle": "Active ETFs now account for nearly a third of all products on the ASX and CXA. The pace of launches has accelerated every year since 2021.",
        "date": "2026-03-16",
        "category": "Market Trends",
        "summary": "Active ETF launches hit a record 38 in 2025, and the category has accumulated $59.6 billion in FUM. We examine which managers are winning, what it costs investors, and whether the growth is sustainable.",
        "body": """
<p>Australia's ETF market reached a new milestone in early 2026: <strong>143 active ETFs</strong> are now listed on the ASX and CXA, collectively managing <strong>$59.6 billion</strong> in assets. That is roughly one active product for every two passive ones, a ratio that would have seemed implausible five years ago.</p>

<p>The category has compounded at pace. In 2021, just 10 active ETFs launched. By 2023 that had risen to 23, and 2025 saw a record <strong>38 new active listings</strong>. The pipeline into 2026 shows no sign of slowing: three active products have already listed in the first two months of the year.</p>

<h2>The cost of active management</h2>
<p>Active ETFs carry an average management fee of <strong>0.76%</strong> per annum, roughly double the 0.36% average for passive products. The gap narrows significantly when you focus on the largest active managers: DFA's Dimensional Australian Core Equity Trust (DACE) charges 0.28% and holds $6.6 billion, while Macquarie's newly listed MQHG comes in at a striking 0.18%.</p>

<p>At the other end, several boutique active products charge above 1.2%, including Antipodes' MIDS, Ausbil's GSCF, and Loftus Peak's LPHD — all launched in the second half of 2025.</p>

<h2>Standout launches from the past six months</h2>
<p>Several active ETFs launched recently have attracted substantial early inflows:</p>

<table>
  <thead><tr><th>ETF</th><th>Name</th><th>Launched</th><th>FUM</th><th>Fee</th></tr></thead>
  <tbody>
    <tr><td>QGFH</td><td>Quay Global Real Estate Fund (AUD Hdg)</td><td>Nov 2025</td><td>$692M</td><td>0.92%</td></tr>
    <tr><td>QGRU</td><td>Quay Global Real Estate Fund (Unhedged)</td><td>Nov 2025</td><td>$543M</td><td>0.88%</td></tr>
    <tr><td>DIVI</td><td>Ausbil Active Dividend Income Fund</td><td>Sep 2025</td><td>$560M</td><td>0.85%</td></tr>
    <tr><td>GSUS</td><td>Candriam Sustainable Global Equity</td><td>Oct 2025</td><td>$173M</td><td>0.55%</td></tr>
    <tr><td>GHIF</td><td>Ausbil Global Essential Infrastructure (Hdg)</td><td>Oct 2025</td><td>$268M</td><td>1.00%</td></tr>
    <tr><td>GSCF</td><td>Ausbil Global Small Cap Fund</td><td>Oct 2025</td><td>$129M</td><td>1.20%</td></tr>
    <tr><td>XX20</td><td>First Sentier Ex-20 Australian Share</td><td>Dec 2025</td><td>$109M</td><td>0.75%</td></tr>
  </tbody>
</table>

<p>The Quay funds warrant a closer look — and a note of caution on the headline FUM figures. QGFH and QGRU were not new launches in any conventional sense. They represent the conversion of Quay's existing unlisted global real estate fund to an ASX-listed ETF, with a <strong>Dual Access</strong> feature that allows investors to continue holding the fund in either its unlisted or listed form. The $1.24 billion on the books at listing largely reflects assets that were already in the fund before it converted — existing investors simply gained the option to hold and trade their investment on exchange. That is a meaningfully different story from an ETF attracting $1.24 billion in new investor capital: it tells us the conversion was well-received, but it does not reflect genuine new money entering the market.</p>

<h2>What is driving the wave?</h2>
<p>Several forces are converging. The dominant mechanism is the <strong>conversion</strong> of pre-existing unlisted managed funds into listed ETFs, often with a Dual Access feature that lets existing investors remain in the unlisted vehicle or migrate to the exchange-listed one. Managers with established track records — Ausbil's DIVI, the Quay funds, and many others — have converted existing funds rather than building new ones from scratch, bringing their existing FUM onto the exchange. The listed structure improves liquidity, simplifies custody and administration for financial advisers, and provides intraday pricing — without requiring any change to the underlying investment approach.</p>

<p>Whether active management adds value net of fees is a perennial debate. For the market overall, the answer will only become clear over a full cycle.</p>
""",
    },

    {
        "slug": "vanguard-dominance-australia-2026",
        "title": "Vanguard's $60 Billion Grip on Australian ETFs",
        "subtitle": "VAS alone is bigger than the entire New Zealand ETF market. How Vanguard came to dominate, and who is chipping away at its lead.",
        "date": "2026-03-16",
        "category": "Market Trends",
        "summary": "VAS has crossed $23 billion in assets, and Vanguard's total ETF FUM approaches $60 billion. A look at the concentration at the top of the market and the competitive dynamics playing out below.",
        "body": """
<p>The Australian ETF market holds approximately <strong>$336 billion</strong> in total assets across 472 listed products. Of that, a single manager — Vanguard — accounts for a disproportionate share of the largest funds.</p>

<p><strong>VAS</strong>, the Vanguard Australian Shares Index ETF, crossed <strong>$23.1 billion</strong> in assets as of March 2026, making it the largest ETF in the country by a significant margin. Its sibling VGS (MSCI International Shares) holds $14.1 billion. Between them, two funds account for nearly $37 billion — more than 11% of the entire market.</p>

<h2>The top of the market</h2>
<table>
  <thead><tr><th>ETF</th><th>Manager</th><th>FUM</th><th>Fee</th><th>1Y Return</th></tr></thead>
  <tbody>
    <tr><td>VAS</td><td>Vanguard</td><td>$23.1B</td><td>0.07%</td><td>+7.6%</td></tr>
    <tr><td>VGS</td><td>Vanguard</td><td>$14.1B</td><td>0.18%</td><td>+5.8%</td></tr>
    <tr><td>IVV</td><td>iShares</td><td>$12.8B</td><td>0.04%</td><td>+2.4%</td></tr>
    <tr><td>A200</td><td>BetaShares</td><td>$9.2B</td><td>0.04%</td><td>+7.2%</td></tr>
    <tr><td>IOZ</td><td>iShares</td><td>$8.0B</td><td>0.05%</td><td>+7.2%</td></tr>
    <tr><td>QUAL</td><td>VanEck</td><td>$8.0B</td><td>0.40%</td><td>+2.2%</td></tr>
    <tr><td>NDQ</td><td>BetaShares</td><td>$7.5B</td><td>0.48%</td><td>+6.0%</td></tr>
    <tr><td>GOLD</td><td>Global X</td><td>$7.0B</td><td>0.40%</td><td>+64.7%</td></tr>
  </tbody>
</table>

<h2>The VAS portfolio: BHP meets CBA</h2>
<p>VAS tracks the S&P/ASX 300 index, and its top holdings reflect Australia's resource and banking concentration. <strong>BHP</strong> and <strong>CBA</strong> each account for roughly 10% of the portfolio — a combined 20% in just two stocks. The big four banks collectively represent around 25% of the fund. Investors who want broad Australian equity exposure are inevitably taking a large implicit bet on financials and materials.</p>

<table>
  <thead><tr><th>Holding</th><th>Sector</th><th>Weight</th></tr></thead>
  <tbody>
    <tr><td>BHP Group</td><td>Metals &amp; Mining</td><td>10.3%</td></tr>
    <tr><td>Commonwealth Bank</td><td>Banks</td><td>10.1%</td></tr>
    <tr><td>National Australia Bank</td><td>Banks</td><td>5.2%</td></tr>
    <tr><td>Westpac</td><td>Banks</td><td>5.0%</td></tr>
    <tr><td>ANZ Group</td><td>Banks</td><td>4.1%</td></tr>
    <tr><td>Wesfarmers</td><td>Broadline Retail</td><td>3.1%</td></tr>
    <tr><td>Macquarie Group</td><td>Capital Markets</td><td>2.6%</td></tr>
    <tr><td>CSL</td><td>Biotechnology</td><td>2.5%</td></tr>
    <tr><td>Rio Tinto</td><td>Metals &amp; Mining</td><td>2.1%</td></tr>
    <tr><td>Goodman Group</td><td>Industrial REITs</td><td>2.0%</td></tr>
  </tbody>
</table>

<h2>Where BetaShares and iShares are competing</h2>
<p>BetaShares' A200 is the most direct competitor to VAS, tracking the ASX 200 at a fee of just 0.04% — matching iShares' IVV for the cheapest ETF available in Australia. A200 has grown to $9.2 billion, adding over $217 million in net inflows last month alone. The fee war at the large-cap passive end of the market has been good for investors and painful for margins.</p>

<p>iShares leads at the S&P 500 end with IVV at $12.8 billion, charging 0.04%. VanEck's QUAL takes a different approach — tracking quality-screened international companies at 0.40% — and has accumulated $8.0 billion, suggesting there is appetite for factor-based alternatives even at higher fees.</p>
""",
    },

    # ── ETF Basics ──────────────────────────────────────────────────────────────

    {
        "slug": "what-is-an-index",
        "title": "What Is an Index?",
        "subtitle": "Every passive ETF tracks one. But what actually is an index, who builds them, and why does the construction methodology matter so much?",
        "date": "2026-03-17",
        "category": "Education",
        "summary": "An index is a rulebook for selecting and weighting a set of securities. Understanding how that rulebook works — who writes it, how it handles size and liquidity, and when it rebalances — tells you exactly what you own when you buy an index ETF.",
        "body": """
<p>When you buy VAS, you are buying the S&P/ASX 300. When you buy IVV, you are buying the S&P 500. The fund is just the wrapper — the real product is the <strong>index</strong>. Understanding what an index is, who builds it, and how it makes decisions is the foundation of understanding what you own.</p>

<h2>An index is a set of rules</h2>
<p>An index is not a fund and it is not a portfolio. It is a <em>methodology</em> — a written set of rules that specifies which securities to include, how to weight them, and when to update the list. An independent organisation (the index provider) applies these rules systematically, and any fund that claims to track the index must hold the same securities in approximately the same weights.</p>

<p>The rules typically cover:</p>
<ul>
  <li><strong>Eligibility</strong>: What types of securities qualify? Common requirements include minimum market capitalisation, minimum daily trading volume, listing exchange, and domicile.</li>
  <li><strong>Weighting</strong>: How much of each security is included? The most common approach is market-capitalisation weighting.</li>
  <li><strong>Reconstitution</strong>: When are new securities added or removed? Most indices rebalance quarterly or semi-annually.</li>
  <li><strong>Corporate actions</strong>: How are mergers, delistings, spin-offs, and dividend payments handled?</li>
</ul>

<h2>The major index providers</h2>
<p>A small number of firms dominate index construction globally. Their names appear on nearly every passive ETF:</p>

<table>
  <thead><tr><th>Provider</th><th>Key indices</th><th>Geographic focus</th></tr></thead>
  <tbody>
    <tr><td>S&amp;P Dow Jones Indices</td><td>S&amp;P 500, S&amp;P/ASX 200, S&amp;P/ASX 300</td><td>US, Australia, global</td></tr>
    <tr><td>MSCI</td><td>MSCI World, MSCI Emerging Markets, MSCI ACWI</td><td>International, global</td></tr>
    <tr><td>FTSE Russell</td><td>FTSE 100, Russell 2000, FTSE All-World</td><td>UK, US small cap, global</td></tr>
    <tr><td>Bloomberg</td><td>Bloomberg Global Aggregate, Bloomberg AusBond</td><td>Fixed income</td></tr>
    <tr><td>Nasdaq</td><td>Nasdaq-100</td><td>US large-cap tech/growth</td></tr>
  </tbody>
</table>

<p>Index providers charge licensing fees to fund managers who use their indices. These fees are a material cost for ETF providers, particularly for widely-used indices like the MSCI World — one reason why some managers have developed proprietary indices for their cheapest products (BetaShares uses a Solactive-constructed index for A200 rather than the more expensive S&P/ASX 200 index used by IOZ and STW).</p>

<h2>Market capitalisation weighting</h2>
<p>The vast majority of equity indices are weighted by <strong>market capitalisation</strong> — a company's total market value (share price × shares outstanding). Larger companies get larger weights. In the S&P/ASX 200, BHP and CBA each represent around 10% of the index because they are Australia's two largest listed companies by market value.</p>

<p>Market cap weighting has an elegant logic: it reflects the collective judgement of all investors about what each company is worth. It also has a well-documented quirk: it automatically overweights companies that have become <em>expensive</em> (their price has risen faster than fundamentals justify) and underweights companies that have become cheap. Critics call this a momentum-chasing feature built in by design.</p>

<h2>Alternative weighting methods</h2>
<p>Recognising this limitation, index providers have developed alternatives:</p>

<ul>
  <li><strong>Equal weight</strong>: Every constituent gets the same allocation regardless of size. VanEck's MVW holds all ASX 200 companies at equal weight. Small companies get far more representation; rebalancing costs are higher.</li>
  <li><strong>Fundamental weight</strong>: Weight by accounting metrics (earnings, dividends, book value) rather than market price. Tends to tilt toward value stocks.</li>
  <li><strong>Factor/smart beta</strong>: Screen for a specific characteristic — quality (QUAL), minimum volatility (MVOL), dividend yield (VHY). The index rules select and weight based on factor scores rather than market cap.</li>
  <li><strong>Fixed weight</strong>: Some multi-asset and thematic indices specify fixed percentage allocations, rebalanced periodically back to target.</li>
</ul>

<h2>Why index construction matters</h2>
<p>Two ETFs that both claim to track "Australian equities" can produce meaningfully different returns depending on which index they follow. The S&P/ASX 200 covers the 200 largest companies; the S&P/ASX 300 adds 100 smaller companies; the S&P/ASX All Ordinaries includes ~500. Over time, the performance differences are usually modest, but they are not zero — particularly in years when small-cap and large-cap returns diverge significantly.</p>

<p>Similarly, whether an international index <em>includes</em> or <em>excludes</em> Australia matters: VGS (MSCI World) excludes Australia (because it is typically used alongside an Australian equity ETF), while IWLD explicitly notes "ex Australia" in its name. Buying both IVV (S&P 500) and VGS (MSCI World) gives you significant overlap in US holdings, since the US represents 70%+ of the MSCI World index.</p>

<h2>Rebalancing and index changes</h2>
<p>Most indices reconstitute quarterly. When a company grows large enough to be added to the ASX 200 — or shrinks below the cut-off and gets removed — all ETFs tracking that index must adjust their portfolios. This creates predictable buying and selling pressure around index rebalance dates, which is both a trading opportunity for sophisticated investors and a small performance drag for index fund holders.</p>
""",
    },

    {
        "slug": "what-is-active-management",
        "title": "What Is Active Management?",
        "subtitle": "Active managers read company reports, meet executives, and build models. The question is whether any of that produces better returns than a low-cost index fund.",
        "date": "2026-03-17",
        "category": "Education",
        "summary": "Active management means a human (or algorithm) making deliberate decisions about what to buy and sell, rather than following a rulebook. We explain how active managers work, what types exist, and what the long-run evidence says about their performance.",
        "body": """
<p>Every time you buy an index ETF, you are implicitly accepting the market's assessment of what every stock is worth. Active managers reject this premise. They believe that through research, analysis, and judgment, they can identify securities that are mispriced — and that buying cheap and avoiding expensive will produce better returns than the index over time.</p>

<h2>What active managers actually do</h2>
<p>The specifics vary enormously, but most fundamental active managers follow some version of this process:</p>

<ol>
  <li><strong>Idea generation</strong>: Screen for companies with interesting characteristics (cheap valuation, high returns on capital, unusual growth, event-driven catalysts) or conduct thematic research to identify sectors worth investigating.</li>
  <li><strong>Fundamental research</strong>: Read annual reports, talk to management, speak to customers and competitors, build financial models to project future cash flows.</li>
  <li><strong>Valuation</strong>: Estimate what the company is worth. If the market price is substantially below the estimated value, the stock is a candidate for purchase.</li>
  <li><strong>Portfolio construction</strong>: Decide how much to hold in each position, balancing conviction, correlation with other holdings, and liquidity constraints.</li>
  <li><strong>Ongoing monitoring</strong>: Watch for changes in the investment thesis — deteriorating fundamentals, management changes, competitive threats — and sell when the thesis breaks or the price reaches fair value.</li>
</ol>

<h2>Types of active management</h2>
<p>Active management is not a single approach. The major styles include:</p>

<table>
  <thead><tr><th>Style</th><th>Approach</th><th>Australian examples</th></tr></thead>
  <tbody>
    <tr><td>Fundamental growth</td><td>Seeks companies with superior earnings growth; willing to pay premium valuations</td><td>Hyperion (HYGG), Magellan (MGOC)</td></tr>
    <tr><td>Fundamental value</td><td>Seeks undervalued companies with improving fundamentals; avoids expensive growth</td><td>Airlie (AASF), Dimensional (DAVA)</td></tr>
    <tr><td>Income / yield</td><td>Focuses on dividend-paying or yield-generating assets</td><td>BetaShares Hybrids (HBRD), Ausbil (DIVI)</td></tr>
    <tr><td>Infrastructure / real assets</td><td>Holds listed infrastructure, property, or real-asset companies for stable cash flows</td><td>Magellan MICH, VanEck IFRA</td></tr>
    <tr><td>Systematic / quantitative</td><td>Uses quantitative models to systematically select and weight securities based on factors</td><td>Dimensional (DACE, DGCE), VanEck QUAL</td></tr>
    <tr><td>Global macro</td><td>Takes positions based on macroeconomic views — currencies, rates, commodities</td><td>Rare in listed ETF form</td></tr>
  </tbody>
</table>

<h2>The evidence on active management performance</h2>
<p>The S&P SPIVA (S&P Indices Versus Active) report is the most comprehensive ongoing study of active fund performance against benchmarks. It publishes data semi-annually for Australia and globally. The consistent finding: most active managers underperform their benchmark index after fees, and the gap widens over longer periods.</p>

<p>For Australian equity funds over the ten years to December 2025, approximately <strong>80% underperformed</strong> the S&P/ASX 200 index. For international equity funds, the underperformance rate was even higher — around 90%.</p>

<p>This does not mean active management is worthless. A minority of managers do deliver persistent outperformance. The challenge for investors is that:</p>
<ul>
  <li>Past performance is a poor predictor of future performance — many managers who outperform in one period underperform in the next</li>
  <li>The managers who charge the most are not more likely to outperform</li>
  <li>Identifying skill in advance requires deep knowledge of a manager's process, risk controls, and team stability</li>
</ul>

<h2>Where active management has a better case</h2>
<p>The case for active management is strongest in markets where information is less widely distributed and prices are more likely to be mispriced. Empirically, smaller companies tend to be less efficiently priced than large caps (fewer analysts covering them, less media attention). Similarly, emerging market equities, high-yield bonds, and illiquid credit tend to reward active managers more reliably than large-cap developed market equities.</p>

<p>Systematic active management — such as Dimensional's evidence-based factor approach — occupies a middle ground. It uses quantitative rules to systematically overweight stocks with characteristics historically associated with higher returns (small size, value, profitability), without relying on individual stock-picking judgment. The result looks more like a dynamic index than traditional stockpicking, but with deliberate tilts designed to capture well-documented return premiums.</p>

<h2>What to look for before investing in an active ETF</h2>
<p>If you are considering an active ETF, the key questions are:</p>
<ul>
  <li><strong>Is the process clearly defined and consistently applied?</strong> Vague statements about "conviction-based investing" are less reassuring than a documented, repeatable methodology.</li>
  <li><strong>How long is the track record?</strong> Three years is not long enough to distinguish skill from luck. Seven-to-ten years across a full market cycle is more meaningful.</li>
  <li><strong>Is the team stable?</strong> Many managed funds — including several that have converted to ETFs — had their strongest performance delivered by a team that no longer exists at the firm.</li>
  <li><strong>Does the fee justify the expected outperformance?</strong> A manager charging 1% needs to outperform by more than 1% just to match the index after fees. That is a meaningful hurdle.</li>
</ul>
""",
    },

    {
        "slug": "what-is-a-market-maker",
        "title": "What Is a Market Maker?",
        "subtitle": "Every time you buy or sell an ETF on the ASX, a market maker is on the other side. They are the reason you can trade instantly at a fair price.",
        "date": "2026-03-17",
        "category": "Education",
        "summary": "Market makers are specialised firms that continuously quote buy and sell prices on the exchange, providing liquidity to investors. We explain how they work, how they make money, and why they matter particularly for ETF investors.",
        "body": """
<p>When you place an order to buy 100 units of VGS on the ASX, you do not wait for another retail investor to decide they want to sell exactly 100 units at that moment. In almost every case, your order is filled by a <strong>market maker</strong> — a specialised firm whose job is to continuously buy and sell securities, ensuring investors can transact whenever they want to.</p>

<h2>What market makers do</h2>
<p>A market maker simultaneously posts a <em>bid price</em> (the price at which they will buy) and an <em>ask price</em> (the price at which they will sell) on the exchange at all times during market hours. The difference between these two prices is the <strong>bid-ask spread</strong> — the market maker's gross revenue per round trip.</p>

<p>For example, if VGS has a bid of $116.40 and an ask of $116.50, the spread is $0.10. A retail investor buying one unit pays $116.50 (the ask). A retail investor selling one unit receives $116.40 (the bid). The market maker collects $0.10 per unit per round trip on average, before their hedging costs and operational expenses.</p>

<h2>How market makers manage risk</h2>
<p>A market maker who only buys VGS all day would quickly accumulate a large position — and a large exposure to the Australian sharemarket declining. Sophisticated market makers hedge in real time:</p>

<ul>
  <li>They may <strong>short the underlying basket</strong>: sell S&P/ASX 300 futures or the individual shares that VAS holds, offsetting the market exposure of their ETF inventory.</li>
  <li>They use the <strong>creation and redemption mechanism</strong> to manage large imbalances: if they accumulate too many ETF units, they can redeem them with the fund manager and receive the underlying shares instead.</li>
  <li>They trade <strong>correlated instruments</strong> (e.g. index futures) to neutralise directional risk quickly and cheaply.</li>
</ul>

<p>The result is that market makers operate on very thin margins, with their profit coming from the spread multiplied by very high turnover — not from taking directional bets on the market.</p>

<h2>Market makers vs authorised participants</h2>
<p>These terms are related but distinct:</p>

<table>
  <thead><tr><th>Role</th><th>What they do</th></tr></thead>
  <tbody>
    <tr><td><strong>Market maker</strong></td><td>Quotes continuous bid/ask prices on the exchange; provides intraday liquidity to retail investors; hedges their inventory</td></tr>
    <tr><td><strong>Authorised participant (AP)</strong></td><td>Creates or redeems ETF units in large blocks (typically $500K+) directly with the fund manager; keeps ETF price aligned with NAV; often the same firm as the market maker, but not always</td></tr>
  </tbody>
</table>

<p>Most large ETF market makers in Australia are also authorised participants — they use the creation/redemption mechanism to source or offload large ETF positions efficiently. Smaller market makers who are not APs rely on buying and selling ETF units on market rather than going directly to the issuer.</p>

<h2>Why spreads vary so much across ETFs</h2>
<p>The bid-ask spread on a liquid ETF like VAS or IVV is typically <strong>1–3 cents</strong> on a unit price of $80–$120, representing a cost of around 0.01–0.04%. For a small or illiquid ETF, the spread might be $0.30–$1.00 or more — a far more meaningful transaction cost.</p>

<p>Several factors drive spread width:</p>
<ul>
  <li><strong>Underlying liquidity</strong>: An ETF holding liquid Australian shares is easy to hedge. An ETF holding illiquid small-cap stocks or physical commodities in a foreign market is harder — so the market maker demands more spread to compensate for hedging risk.</li>
  <li><strong>Trading volume</strong>: High-turnover ETFs allow market makers to turn over their inventory quickly, reducing the average holding period and therefore risk. Low-volume ETFs sit in inventory longer.</li>
  <li><strong>NAV transparency</strong>: Most equity ETFs publish an indicative NAV (iNAV) in near-real-time during market hours. This tight reference price lets market makers quote narrowly. For some fixed-income or less-transparent products, the NAV is harder to calculate intraday, so spreads are wider.</li>
  <li><strong>Number of market makers</strong>: Competition between multiple market makers on the same ETF compresses spreads. Less popular ETFs may have only one market maker quoting, reducing competitive pressure.</li>
</ul>

<h2>What this means for you as an investor</h2>
<p>For long-term buy-and-hold investors in the large liquid ETFs (VAS, VGS, IVV, A200), the bid-ask spread is nearly irrelevant — it amounts to a few dollars on a $10,000 transaction. But for anyone trading frequently, dealing in less-liquid products, or making large trades in smaller ETFs, spread costs can add up.</p>

<p>Two practical habits help: always use <strong>limit orders</strong> (specify a price rather than accepting whatever the ask is), and check the bid-ask spread before buying an unfamiliar ETF. If the spread is wider than 0.20%, factor that cost into your total cost calculation alongside the MER and brokerage commission.</p>

<p>Market makers also add value beyond just the daily bid-ask. They provide a shock absorber during volatile market conditions, continuing to quote prices when many retail participants have pulled back. In the March 2020 COVID sell-off, the creation/redemption arbitrage mechanism and active market-making kept most Australian ETF prices within a few percentage points of their underlying NAVs — a meaningful benefit compared to unlisted managed funds, which can gate redemptions entirely in stress periods.</p>
""",
    },

    # ── Issuer Profiles ─────────────────────────────────────────────────────────

    {
        "slug": "issuer-profile-vanguard",
        "title": "Issuer Profile: Vanguard",
        "subtitle": "The largest ETF provider in Australia manages $90 billion across 32 funds. How a Pennsylvania mutual company built Australia's most dominant investment franchise.",
        "date": "2026-03-17",
        "category": "Issuer Profile",
        "summary": "Vanguard entered Australia in 1996 and launched its first ETF in 2009. Today it manages $89.7 billion in Australian ETF assets across 32 funds, with fees as low as 0.03%. We profile its history, philosophy, and product range.",
        "body": """
<p>By almost any measure, Vanguard is Australia's dominant ETF provider. Its 32 listed funds manage <strong>$89.7 billion</strong> in assets — more than BetaShares and iShares combined. Its flagship, VAS, is the largest ETF in the country at $22.7 billion. And its average management fee of 0.25% sits well below the industry average.</p>

<h2>Background</h2>
<p>Vanguard was founded in 1975 by <strong>John C. Bogle</strong> in Valley Forge, Pennsylvania. Bogle's founding idea was radical: create an investment company owned by its own funds (and therefore by its fund investors), with no outside shareholders to profit from. Without the need to generate profit for external owners, Vanguard could continuously reduce fees and pass savings to investors.</p>

<p>The Vanguard Group launched the first retail index mutual fund in 1976, tracking the S&P 500. It took years for the concept to catch on — early critics dismissed it as "Bogle's folly." By the time Bogle died in 2019, Vanguard had grown to become the world's second-largest asset manager with over $7 trillion under management, and the index fund had become the dominant vehicle for retail investing globally.</p>

<p>Vanguard Australia was established in <strong>1996</strong> as a wholesale operation serving institutional and adviser clients. Its first Australian ETF, <strong>VAS</strong> (Vanguard Australian Shares Index ETF), listed on the ASX in <strong>May 2009</strong> — one of the earliest ETFs listed in Australia. The range has since grown to 32 products covering every major asset class.</p>

<h2>Size and market position</h2>
<p>Vanguard is the <strong>#1 ETF issuer in Australia</strong> by FUM with $89.7B, commanding roughly 27% of the total market. Its nearest competitors — BetaShares ($63.3B) and iShares ($54.5B) — trail by a significant margin. The gap is partly structural: Vanguard entered the market early, established trusted brands, and benefited from the low-cost advantage that compounds as assets grow.</p>

<h2>Philosophy</h2>
<p>Vanguard's investment philosophy is built on four principles: set clear goals, balance across asset classes, minimise costs, and maintain discipline (avoid market-timing). This translates into a product range that is almost entirely passive index products, with fees as low as possible and a deliberate reluctance to launch trendy or speculative themes.</p>

<p>Vanguard does not have a thematic range. It does not offer leveraged or inverse ETFs. Its most complex products are the diversified multi-asset funds (VDHG, VDGR etc.) which are simply blends of its own single-asset-class ETFs. This restraint is a feature, not a limitation.</p>

<h2>Key products</h2>
<table>
  <thead><tr><th>ETF</th><th>Name</th><th>FUM</th><th>Fee</th><th>1Y Return</th></tr></thead>
  <tbody>
    <tr><td>VAS</td><td>Australian Shares Index (ASX 300)</td><td>$22.7B</td><td>0.07%</td><td>+7.6%</td></tr>
    <tr><td>VGS</td><td>MSCI Index International Shares</td><td>$14.0B</td><td>0.18%</td><td>+5.8%</td></tr>
    <tr><td>VHY</td><td>Australian Shares High Yield</td><td>$6.8B</td><td>0.25%</td><td>+12.2%</td></tr>
    <tr><td>VGAD</td><td>MSCI International Shares (Hedged)</td><td>$6.2B</td><td>0.21%</td><td>+15.5%</td></tr>
    <tr><td>VTS</td><td>US Total Market Shares</td><td>$6.0B</td><td>0.03%</td><td>+1.7%</td></tr>
    <tr><td>VEU</td><td>All-World Ex-US Shares</td><td>$5.1B</td><td>0.07%</td><td>+19.5%</td></tr>
    <tr><td>VBND</td><td>Global Aggregate Bond (Hedged)</td><td>$3.9B</td><td>0.20%</td><td>+4.4%</td></tr>
    <tr><td>VDHG</td><td>Diversified High Growth (90/10)</td><td>$3.5B</td><td>0.27%</td><td>+9.0%</td></tr>
  </tbody>
</table>

<h2>Fee structure</h2>
<p>Vanguard consistently prices at or near the lowest fee in each category it competes in. VTS (US Total Market) charges just <strong>0.03%</strong> — among the cheapest ETFs in the world. VAS at 0.07% is the cheapest Australian equity ETF on market. VGS at 0.18% is competitive for broad international exposure.</p>

<p>The diversified funds (VDHG, VDGR etc.) charge 0.27%, which is slightly higher than assembling the components yourself but includes automatic rebalancing — a tangible value-add for investors who would otherwise let allocations drift.</p>

<h2>Recent developments</h2>
<p>In 2022 Vanguard launched <strong>Vanguard Personal Investor</strong> in Australia — a direct-to-consumer investment platform allowing Australians to invest in Vanguard's managed funds and ETFs without going through a broker. This brought Vanguard closer to its US model of dealing directly with end investors, rather than purely through financial advisers and brokers.</p>
""",
    },

    {
        "slug": "issuer-profile-betashares",
        "title": "Issuer Profile: BetaShares",
        "subtitle": "Australia's homegrown ETF giant has 102 funds, $63 billion in assets, and a product range that spans from 0.04% index funds to leveraged bear ETFs. The story of how a Sydney startup became a market leader.",
        "date": "2026-03-17",
        "category": "Issuer Profile",
        "summary": "Founded in Sydney in 2009, BetaShares is Australia's largest ETF provider by number of products and second-largest by FUM. We profile its history, its product philosophy, and its most important funds.",
        "body": """
<p>BetaShares is the only major ETF provider in Australia that was founded here. While its competitors are Australian arms of global giants (BlackRock, Vanguard, State Street), BetaShares was built from scratch in Sydney in 2009 and has grown to manage <strong>$63.3 billion</strong> across <strong>102 ETFs</strong> — the broadest product range of any issuer in the country.</p>

<h2>Background</h2>
<p>BetaShares was co-founded in 2009 by <strong>Alex Vynokur</strong> (CEO) and <strong>Drew Corbett</strong>. Vynokur had previously worked in law and financial services in Australia and South Africa; Corbett had an investment banking background. The pair identified that Australian investors were underserved by the ETF market compared to the US and UK, where ETF adoption was already accelerating.</p>

<p>The company's first ETF, <strong>QAU</strong> (Gold Bullion Currency Hedged), listed in December 2010. Early growth was modest — the Australian ETF market was small and the concept unfamiliar to most retail investors. BetaShares grew by expanding the product range aggressively, launching not just vanilla index ETFs but currency funds, cash products, leveraged and inverse ETFs, and thematic ideas that weren't available elsewhere.</p>

<p>A major milestone came with the launch of <strong>NDQ</strong> (Nasdaq 100) in 2015. Retail appetite for US technology exposure was growing rapidly, and NDQ became one of the fastest-growing ETFs in Australian history. It now manages $7.2 billion. The <strong>AAA</strong> high-interest cash ETF (launched 2012) was another landmark product that filled a genuine gap — a way to hold cash earning the best available overnight rate without needing a term deposit.</p>

<p>In 2021, BetaShares received a significant investment from <strong>TA Associates</strong>, a US-based growth equity firm, valuing the company at approximately $1.5 billion. The investment supported further product development and international expansion.</p>

<h2>Size and market position</h2>
<p>BetaShares is <strong>#1 in Australia by ETF count</strong> (102 funds) and <strong>#2 by FUM</strong> ($63.3B). Its scale across categories is remarkable: it competes in ultra-cheap index ETFs (A200 at 0.04%), income products (AAA, HBRD), ESG (ETHI), thematic (NDQ, URNM), active (HBRD), and leveraged/inverse products (BBOZ, BBUS). No other Australian issuer spans this breadth.</p>

<h2>Philosophy</h2>
<p>BetaShares' approach is less ideologically committed to passive indexing than Vanguard. It will launch active ETFs, smart-beta products, thematic funds, and complex structured products if it sees investor demand. This pragmatism has produced a broader range with more variety in quality — some products have accumulated strong followings; others have remained small.</p>

<h2>Key products</h2>
<table>
  <thead><tr><th>ETF</th><th>Name</th><th>FUM</th><th>Fee</th><th>1Y Return</th></tr></thead>
  <tbody>
    <tr><td>A200</td><td>Australia 200 ETF</td><td>$9.2B</td><td>0.04%</td><td>+7.2%</td></tr>
    <tr><td>NDQ</td><td>Nasdaq 100 ETF</td><td>$7.2B</td><td>0.48%</td><td>+6.0%</td></tr>
    <tr><td>AAA</td><td>Australian High Interest Cash ETF</td><td>$4.9B</td><td>0.18%</td><td>+3.9%</td></tr>
    <tr><td>ETHI</td><td>Global Sustainability Leaders ETF</td><td>$3.5B</td><td>0.59%</td><td>-4.1%</td></tr>
    <tr><td>BGBL</td><td>Global Shares ETF</td><td>$3.4B</td><td>0.08%</td><td>+6.5%</td></tr>
    <tr><td>HBRD</td><td>Australian Hybrids Active ETF</td><td>$2.6B</td><td>0.55%</td><td>+4.9%</td></tr>
    <tr><td>HGBL</td><td>Global Shares Currency Hedged</td><td>$2.0B</td><td>0.11%</td><td>+16.3%</td></tr>
    <tr><td>QPON</td><td>Australian Bank Senior Floating Rate Bond</td><td>$1.9B</td><td>0.22%</td><td>+5.1%</td></tr>
  </tbody>
</table>

<h2>The A200–VAS fee war</h2>
<p>BetaShares' most significant competitive move has been in Australian equities. A200 tracks the ASX 200 at <strong>0.04%</strong>, significantly undercutting Vanguard's VAS (ASX 300, 0.07%) and iShares' IOZ (ASX 200, 0.05%). It uses a Solactive index rather than S&P's ASX 200, which avoids the S&P licensing fee and enables the lower price. A200 has grown to $9.2 billion — substantial scale but still well behind VAS at $22.7B, reflecting VAS's head start and the natural stickiness of existing holdings.</p>

<h2>Income and fixed income range</h2>
<p>BetaShares has one of the strongest fixed income and income ETF ranges in Australia. AAA provides cash-rate returns in ETF form — an innovative product that has attracted nearly $5 billion from investors seeking yield without term deposit lockups. HBRD (Australian hybrids, active) and QPON (floating rate bonds) address specific income needs that passive products don't easily capture. This breadth in fixed income is a meaningful differentiator versus competitors.</p>
""",
    },

    {
        "slug": "issuer-profile-ishares",
        "title": "Issuer Profile: iShares (BlackRock)",
        "subtitle": "The Australian arm of the world's largest asset manager runs 56 ETFs and $54.5 billion. iShares built Australia's second-ever ETF in 2007 and today offers some of the cheapest products on the market.",
        "date": "2026-03-17",
        "category": "Issuer Profile",
        "summary": "iShares is the Australian ETF brand of BlackRock, the world's largest asset manager. We profile its history, its ultra-low-cost Core range, and the key funds that have made it the third-largest ETF provider in Australia.",
        "body": """
<p><strong>BlackRock</strong> is the world's largest asset manager, with over $11 trillion in assets under management globally. Its ETF brand, <strong>iShares</strong>, is the largest ETF provider in the world by assets. In Australia, iShares runs <strong>56 ETFs</strong> managing <strong>$54.5 billion</strong> — making it the third-largest provider in the country behind Vanguard and BetaShares.</p>

<h2>Background</h2>
<p>The iShares brand originated at <strong>Barclays Global Investors (BGI)</strong>, the quantitative investment arm of Barclays Bank. BGI launched the first iShares products in the US in 2000, building on the ETF structure that State Street had pioneered with SPY in 1993. BGI's quantitative heritage gave it a particular strength in index and factor investing.</p>

<p>In 2009, <strong>BlackRock acquired Barclays Global Investors</strong> for approximately $13.5 billion, gaining the iShares brand and its global ETF operation. The acquisition transformed BlackRock from a large fixed-income manager into the world's dominant ETF provider overnight.</p>

<p>iShares Australia launched in <strong>October 2007</strong> with the listing of IVV (S&P 500) and IOZ (ASX 200) — among the earliest ETFs on the ASX. The Australian operation has grown steadily, benefiting from BlackRock's global scale in index licensing, technology, and capital markets relationships.</p>

<h2>Size and market position</h2>
<p>iShares is <strong>#3 in Australia by FUM</strong> at $54.5B. Its range spans Australian equities, international equities, bonds, and thematic products. The average management fee of 0.27% reflects its strength in low-cost core products — its Core range includes some of the cheapest ETFs available in Australia.</p>

<h2>The Core range</h2>
<p>iShares' most important strategic move in Australia was the development of a dedicated <strong>Core range</strong> — a set of low-cost, broad-market building blocks explicitly designed for long-term portfolio construction rather than trading. The Core range products are priced at cost:</p>

<table>
  <thead><tr><th>ETF</th><th>Name</th><th>FUM</th><th>Fee</th><th>1Y Return</th></tr></thead>
  <tbody>
    <tr><td>IVV</td><td>S&amp;P 500 ETF (Core)</td><td>$12.4B</td><td>0.04%</td><td>+2.4%</td></tr>
    <tr><td>IOZ</td><td>Core S&amp;P/ASX 200 ETF</td><td>$8.1B</td><td>0.05%</td><td>+7.2%</td></tr>
    <tr><td>IAF</td><td>Core Composite Bond ETF</td><td>$3.6B</td><td>0.10%</td><td>+3.1%</td></tr>
    <tr><td>IHVV</td><td>S&amp;P 500 AUD Hedged (Core)</td><td>$3.2B</td><td>0.10%</td><td>+14.5%</td></tr>
    <tr><td>IWLD</td><td>Core MSCI World ex Australia ESG</td><td>$1.4B</td><td>0.09%</td><td>+6.8%</td></tr>
  </tbody>
</table>

<p>IVV at <strong>0.04%</strong> is the joint-cheapest ETF in Australia (alongside BetaShares A200). At $12.4 billion it is Australia's third-largest ETF. Its fee has been cut multiple times over the years as BlackRock's scale economies improved — IVV once charged 0.07%, then 0.05%, now 0.04%.</p>

<h2>Beyond the Core range</h2>
<p>Outside the Core range, iShares offers:</p>
<ul>
  <li><strong>Global thematic</strong>: IOO (Global 100, 0.40%) is Australia's fifth-largest ETF at $5.0B, offering exposure to the world's 100 largest companies across all sectors.</li>
  <li><strong>Infrastructure</strong>: GLIN (Global Infrastructure AUD Hedged, 0.15%) at $1.65B covers listed infrastructure companies globally.</li>
  <li><strong>Emerging markets</strong>: IEM (MSCI Emerging Markets, 0.69%) at $1.5B for developing-world equity exposure.</li>
  <li><strong>ESG</strong>: Several ESG-screened variants of core indices, including IWLD's ex-controversial weapons, tobacco and civilian firearms screen.</li>
</ul>

<h2>BlackRock's global advantage</h2>
<p>iShares benefits from BlackRock's extraordinary scale in ways that are not always visible to retail investors. BlackRock's capital markets team maintains relationships with hundreds of market makers globally, helping ensure tight spreads on iShares products. Its securities lending programmes generate revenue that can offset fund costs. And its negotiating power with index providers (S&P, MSCI) on licensing fees helps keep the Core range priced at levels that competitors struggle to match.</p>
""",
    },

    {
        "slug": "issuer-profile-vaneck",
        "title": "Issuer Profile: VanEck",
        "subtitle": "The Dutch-American specialist runs 48 ETFs and $31 billion in Australia, built around its flagship QUAL quality-factor ETF. A profile of a manager who resisted going cheap and won.",
        "date": "2026-03-17",
        "category": "Issuer Profile",
        "summary": "VanEck entered Australia with a focus on smart beta and factor investing, not plain vanilla index ETFs. Its QUAL ETF (quality factor, 0.40%) has grown to $7.8 billion — one of Australia's top 10 ETFs. We profile VanEck's history, product range, and investment approach.",
        "body": """
<p>VanEck's Australian operation is a study in focus. Rather than competing with Vanguard and iShares on pure cost in vanilla index ETFs, VanEck built its Australian franchise around factor investing, emerging markets, and fixed income niches. The strategy has worked: its <strong>48 ETFs</strong> manage <strong>$31 billion</strong>, and its flagship QUAL is one of Australia's largest ETFs at $7.8 billion — despite charging 0.40%, ten times what the cheapest ETFs cost.</p>

<h2>Background</h2>
<p>VanEck was founded in <strong>1955</strong> by <strong>John van Eck</strong> in New York. The firm was an early proponent of international investing for US investors at a time when domestic stocks dominated US portfolios. Van Eck's son <strong>Jan van Eck</strong>, who took over as CEO, extended the firm into commodities (it managed the first US gold ETF, GDX, in 2006) and into factors and smart beta.</p>

<p>VanEck entered Australia in <strong>2013</strong>, initially with a small range of market access and emerging market products. The transformative Australian launch was <strong>QUAL</strong> — the MSCI International Quality ETF — which identified high-quality international companies (high return on equity, stable earnings, low financial leverage) and weighted them accordingly. QUAL's philosophy resonated with Australian self-managed super fund (SMSF) trustees and financial advisers who wanted international equity exposure with a quality screen rather than pure market-cap weight.</p>

<h2>QUAL: the flagship</h2>
<p>QUAL is one of the most successful non-plain-vanilla ETFs in Australian history. At $7.8 billion in assets, it is Australia's sixth-largest ETF. It charges 0.40% — a fee that would doom a plain index ETF in a competitive market — but which investors have accepted as reasonable for a differentiated factor exposure.</p>

<p>The MSCI Quality index screens for three metrics: return on equity, earnings variability, and debt-to-equity. The resulting portfolio is concentrated in US technology and healthcare — companies like Apple, Microsoft, Nvidia, and Eli Lilly feature heavily. It is effectively a quality-filtered version of the US large-cap market with a modest tilt away from financial stocks (which tend to have high debt).</p>

<p>VanEck has since extended the QUAL brand with <strong>QHAL</strong> (hedged version, $2.3B) and <strong>QSML</strong> (small company quality, $1.5B).</p>

<h2>Key products</h2>
<table>
  <thead><tr><th>ETF</th><th>Name</th><th>FUM</th><th>Fee</th><th>1Y Return</th></tr></thead>
  <tbody>
    <tr><td>QUAL</td><td>MSCI International Quality ETF</td><td>$7.8B</td><td>0.40%</td><td>+2.2%</td></tr>
    <tr><td>SUBD</td><td>Australian Subordinated Debt ETF</td><td>$3.5B</td><td>0.29%</td><td>+5.8%</td></tr>
    <tr><td>MVW</td><td>Australian Equal Weight ETF</td><td>$3.1B</td><td>0.35%</td><td>+4.5%</td></tr>
    <tr><td>QHAL</td><td>MSCI International Quality (Hedged)</td><td>$2.3B</td><td>0.43%</td><td>+11.9%</td></tr>
    <tr><td>IFRA</td><td>FTSE Global Infrastructure (Hedged)</td><td>$1.9B</td><td>0.20%</td><td>+13.3%</td></tr>
    <tr><td>GDX</td><td>Gold Miners ETF</td><td>$1.6B</td><td>0.53%</td><td>+137.9%</td></tr>
    <tr><td>QSML</td><td>MSCI International Small Co. Quality</td><td>$1.5B</td><td>0.59%</td><td>-1.7%</td></tr>
    <tr><td>FLOT</td><td>Australian Floating Rate ETF</td><td>$1.0B</td><td>0.22%</td><td>+4.8%</td></tr>
  </tbody>
</table>

<h2>Fixed income and alternatives strength</h2>
<p>Beyond equities, VanEck has built meaningful scale in Australian fixed income. <strong>SUBD</strong> (subordinated debt, $3.5B) fills a gap — it provides exposure to Australian bank subordinated bonds (Tier 2 capital instruments) that sit between senior debt and hybrids in the capital structure. <strong>FLOT</strong> (floating rate, $1.0B) and several other debt ETFs demonstrate VanEck's depth in fixed income niches that larger generalist providers have not prioritised.</p>

<p><strong>GDX</strong> (Gold Miners, $1.6B) is VanEck's longest-standing commodity product in Australia. In the past year it returned <strong>+137.9%</strong>, as gold miner equities benefited from both rising gold prices and operating leverage. It is one of the few ETFs in Australia to have returned more than 100% in a single year.</p>

<h2>Philosophy</h2>
<p>VanEck's positioning is explicit: it occupies the space between passive vanilla index ETFs and high-cost active management. Its smart beta and factor products charge more than plain index ETFs but less than most active managers, offering systematic exposures with low-to-moderate cost. Whether that positioning proves durable as competitors launch competing factor products at lower prices will be one of the defining competitive questions for the Australian ETF market in the years ahead.</p>
""",
    },

    {
        "slug": "issuer-profile-global-x",
        "title": "Issuer Profile: Global X",
        "subtitle": "Australia's fourth-largest ETF provider started as a gold vault operator in 2003 and grew into a 49-fund thematic powerhouse. The story behind GOLD, ETPMAG, and FANG.",
        "date": "2026-03-17",
        "category": "Issuer Profile",
        "summary": "Global X Australia has its roots in ETF Securities, a company founded by Graham Tuckwell that pioneered physically-backed commodity ETPs in Australia in 2003. Today it manages $17.3 billion across 49 funds. We profile its history and product range.",
        "body": """
<p>Global X Australia has an unusually rich history. The business traces its origins to <strong>ETF Securities</strong>, which launched the world's first physically-backed gold ETP in 2003 — years before the US or UK equivalents. Today, as part of the global Global X network, it manages <strong>$17.3 billion</strong> across <strong>49 ETFs</strong>, with an emphasis on physical commodities and thematic investing.</p>

<h2>Background: ETF Securities and Graham Tuckwell</h2>
<p>In 2003, <strong>Graham Tuckwell</strong> — a former Goldman Sachs banker — established <strong>Gold Bullion Securities</strong> in Australia, creating the world's first exchange-listed product backed by physical gold. Each unit represented a fractional claim on gold stored in a secure vault. The product, later rebranded under the ETF Securities umbrella, proved enormously successful and spawned equivalents in London, New York, and across Europe.</p>

<p>ETF Securities Australia went on to launch silver, platinum, palladium, and other commodity products using the same physical-backing model. GOLD (Global X Physical Gold) and ETPMAG (Global X Physical Silver) are direct descendants of those original products, and GOLD at $6.8 billion is Australia's fourth-largest ETF.</p>

<p>In <strong>2021</strong>, Mirae Asset — a South Korean asset management giant — acquired ETF Securities' Australian and European operations, rebranding them under the <strong>Global X</strong> name. Global X had been a US ETF provider that Mirae had separately acquired in 2018. The combination created a significant global thematic ETF network, with the Australian arm gaining access to Global X's established US product shelf.</p>

<h2>Size and market position</h2>
<p>Global X is <strong>#4 in Australia by FUM</strong> at $17.3B across 49 ETFs. Its product range is distinctly different from the top three — it is the clear leader in physical commodity products and has a strong thematic range. Its average fee of 0.47% reflects this premium-product orientation.</p>

<h2>Key products</h2>
<table>
  <thead><tr><th>ETF</th><th>Name</th><th>FUM</th><th>Fee</th><th>1Y Return</th></tr></thead>
  <tbody>
    <tr><td>GOLD</td><td>Physical Gold Structured Product</td><td>$6.8B</td><td>0.40%</td><td>+64.7%</td></tr>
    <tr><td>ETPMAG</td><td>Physical Silver Structured Product</td><td>$2.4B</td><td>0.49%</td><td>+212.6%</td></tr>
    <tr><td>FANG</td><td>FANG+ ETF</td><td>$1.3B</td><td>0.35%</td><td>+0.6%</td></tr>
    <tr><td>GXLD</td><td>Gold Bullion ETF (trust structure)</td><td>$650M</td><td>0.15%</td><td>+65.3%</td></tr>
    <tr><td>ACDC</td><td>Battery Tech &amp; Lithium ETF</td><td>$687M</td><td>0.69%</td><td>+64.0%</td></tr>
    <tr><td>WIRE</td><td>Copper Miners ETF</td><td>$672M</td><td>0.65%</td><td>+109.8%</td></tr>
    <tr><td>USTB</td><td>US Treasury Bond (Currency Hedged)</td><td>$608M</td><td>0.19%</td><td>+5.0%</td></tr>
    <tr><td>SEMI</td><td>Semiconductor ETF</td><td>$535M</td><td>0.45%</td><td>+59.2%</td></tr>
  </tbody>
</table>

<h2>Physical commodities: the heritage business</h2>
<p>GOLD and ETPMAG are the crown jewels of the Australian operation. Physical gold ETPs differ structurally from equity ETFs: each unit is backed by a fractional entitlement to physical gold held in HSBC's London vaults. Investors effectively own gold without needing to arrange storage and insurance themselves.</p>

<p>GOLD's $6.8B in assets makes it both the largest gold product and the largest non-equity ETF in Australia. Over the past year it returned <strong>+64.7%</strong>, driven by a strong gold price. ETPMAG's +212.6% return made it the best-performing ETF of any type in Australia over that period. A second gold product, GXLD, offers the same physical gold exposure at a lower fee (0.15%) through a different trust structure.</p>

<h2>Thematic range</h2>
<p>Following the 2021 rebrand, Global X significantly expanded its thematic offering, importing products from the US shelf. FANG+ tracks the ten most-traded non-financial large-cap US tech and consumer companies (including Meta, Apple, Nvidia, Tesla). SEMI tracks semiconductor companies. WIRE tracks copper miners. ACDC tracks battery technology and lithium companies.</p>

<p>This thematic range positions Global X as the natural destination for investors seeking exposure to specific trends in the energy transition, technology, or commodities, rather than broad market index products.</p>
""",
    },

    {
        "slug": "issuer-profile-spdr-state-street",
        "title": "Issuer Profile: SPDR / State Street",
        "subtitle": "State Street created the world's first ETF in 1993 and listed the first ETF in Australia in 2001. Its combined SPDR and StateStreet range now manages $11.4 billion across 17 products.",
        "date": "2026-03-17",
        "category": "Issuer Profile",
        "summary": "State Street Global Advisors created SPY — the world's first ETF — in 1993 and STW, Australia's first ETF, in 2001. Today it runs 17 ETFs managing $11.4 billion, including the iconic STW and a growing ESG-focused range.",
        "body": """
<p>State Street Global Advisors (SSGA) occupies a unique place in ETF history. It launched <strong>SPY</strong> — the SPDR S&P 500 ETF — on the American Stock Exchange on 22 January 1993, making it the world's first modern ETF. SPY remains the most traded security in the world by dollar volume. In Australia, SSGA was equally first: <strong>STW</strong> (SPDR S&P/ASX 200) listed on the ASX in <strong>August 2001</strong>, making it Australia's first ETF.</p>

<h2>Background</h2>
<p>State Street Corporation was founded in 1792 in Boston, making it one of the oldest financial institutions in the United States. Its asset management arm, State Street Global Advisors, is the third-largest asset manager globally with over $4 trillion under management. SSGA was a pioneer in quantitative and index investing from the 1970s onward.</p>

<p>The creation of SPY in 1993 was a collaboration between SSGA, the American Stock Exchange, and the Options Clearing Corporation. The product was designed to make it easier for institutional investors to take broad market exposure quickly — few anticipated that retail investors would ultimately become the dominant user base. SPY now manages over $600 billion in the US alone.</p>

<p>In Australia, STW's early-mover advantage was substantial but not permanent. As Vanguard, iShares, and BetaShares entered the market with lower fees and broader product ranges, STW's dominance eroded. It remains a significant product at $6.25B but now faces intense competition in its core ASX 200 market from IOZ (0.05%), A200 (0.04%), and E200 (0.05%).</p>

<h2>The two brands: SPDR and StateStreet</h2>
<p>Confusingly, SSGA operates two ETF brands in Australia. <strong>SPDR</strong> (pronounced "spider") is the consumer-facing brand for its main products. <strong>StateStreet</strong> is used for a separate range of products targeted at institutional and ESG-focused investors. Both are legally managed by State Street Global Advisors Trust Company.</p>

<h2>Key products</h2>
<table>
  <thead><tr><th>ETF</th><th>Name</th><th>FUM</th><th>Fee</th><th>1Y Return</th></tr></thead>
  <tbody>
    <tr><td>STW</td><td>SPDR S&amp;P/ASX 200 ETF</td><td>$6.3B</td><td>0.05%</td><td>+7.3%</td></tr>
    <tr><td>WXOZ</td><td>SPDR S&amp;P World ex Australia Carbon Aware</td><td>$620M</td><td>0.07%</td><td>+4.8%</td></tr>
    <tr><td>SYI</td><td>SPDR MSCI Australia Select High Dividend Yield</td><td>$611M</td><td>0.35%</td><td>+11.0%</td></tr>
    <tr><td>SFY</td><td>SPDR S&amp;P/ASX 50 ETF</td><td>$726M</td><td>0.20%</td><td>+4.4%</td></tr>
    <tr><td>DJRE</td><td>SPDR Dow Jones Global Real Estate ESG</td><td>$508M</td><td>0.50%</td><td>-2.7%</td></tr>
    <tr><td>SPY</td><td>SPDR S&amp;P 500 ETF (AU-listed)</td><td>$371M</td><td>0.09%</td><td>+2.3%</td></tr>
    <tr><td>QMIX</td><td>SPDR MSCI World Quality Mix</td><td>$372M</td><td>0.35%</td><td>+6.5%</td></tr>
    <tr><td>E200</td><td>SPDR S&amp;P/ASX 200 ESG ETF</td><td>$273M</td><td>0.13%</td><td>+6.6%</td></tr>
  </tbody>
</table>

<h2>ESG focus as a differentiator</h2>
<p>SSGA has invested heavily in ESG as a product differentiator. Several of its newer products — WXOZ (carbon-aware world ex Australia), E200 (ASX 200 ESG) — incorporate climate and sustainability screens. SSGA's parent company has also been prominent in shareholder engagement on environmental issues, including the Fearless Girl campaign and its proxy voting framework through its asset stewardship team.</p>

<h2>STW vs the competition</h2>
<p>STW was Australia's only ETF for several years after its 2001 launch. Today it faces at least four direct competitors in the ASX 200 space alone. At 0.05%, it is priced competitively with IOZ (iShares, 0.05%) but above A200 (BetaShares, 0.04%) and E200 (its own ESG product at 0.13%). STW's $6.3B reflects its historical head start more than any current product superiority.</p>

<p>SSGA's global reputation and institutional relationships keep it relevant in the Australian market despite its narrower product range compared to BetaShares or iShares. For many institutional allocators, the SPDR brand carries a trust premium that newer entrants have not yet earned.</p>
""",
    },

    {
        "slug": "issuer-profile-dimensional",
        "title": "Issuer Profile: Dimensional (DFA)",
        "subtitle": "The academic factor investing pioneer launched its first Australian ETFs in late 2023 and immediately attracted $18 billion. Why Dimensional's approach is unlike anything else in the market.",
        "date": "2026-03-17",
        "category": "Issuer Profile",
        "summary": "Dimensional Fund Advisors brought its evidence-based, factor-tilted approach to the Australian ETF market in November 2023. Within 18 months it had accumulated $18 billion across 6 ETFs — one of the fastest institutional-to-retail launches in market history.",
        "body": """
<p>Dimensional Fund Advisors (DFA) is unlike any other ETF provider in the Australian market. Its products are active ETFs — a manager is making decisions — but the approach is systematic and evidence-based, drawing on decades of academic research rather than individual stock selection. The result sits between passive index funds and traditional active management in both philosophy and cost.</p>

<h2>Background</h2>
<p>DFA was founded in <strong>1981</strong> in Santa Monica, California, by David Booth and Rex Sinquefield. The founding insight came from academic research: <strong>Eugene Fama</strong> and <strong>Kenneth French</strong>, whose three-factor model (market, size, and value) identified systematic return premiums that could be captured by investing deliberately in small-cap and value stocks.</p>

<p>DFA built its entire business around these academic insights. Rather than trying to pick individual outperforming stocks, it tilts portfolios systematically toward stocks with characteristics historically associated with higher expected returns — small size, low price relative to book value (value), and high profitability. The portfolio construction is rules-based but not mechanical; DFA maintains some flexibility in implementation (e.g. patient trading to reduce market impact) that distinguishes it from pure passive index funds.</p>

<p>For decades, DFA was famously <strong>exclusive</strong>: its funds were only accessible through a narrow network of approved financial advisers who had completed DFA's training programme. This adviser-only distribution built institutional credibility and a loyal client base, but limited retail access. The launch of ETFs in the US (2020) and Australia (2023) represented a fundamental shift — DFA products were now available to any investor with a brokerage account.</p>

<h2>The Australian launch</h2>
<p>DFA listed its first six Australian ETFs in <strong>November 2023</strong> and <strong>August 2024</strong>. The early FUM figures were extraordinary: within months, DACE had accumulated several billion dollars. This reflected a large pool of existing DFA investors who had held unlisted managed fund versions of the same strategies and <strong>converted</strong> their holdings into the ETF wrappers. But there was also genuine new investment from self-directed investors who had known of DFA's reputation but previously couldn't access its products.</p>

<h2>Key products</h2>
<table>
  <thead><tr><th>ETF</th><th>Name</th><th>FUM</th><th>Fee</th><th>1Y Return</th></tr></thead>
  <tbody>
    <tr><td>DACE</td><td>Dimensional Australian Core Equity Trust</td><td>$6.3B</td><td>0.28%</td><td>+14.1%</td></tr>
    <tr><td>DGCE</td><td>Dimensional Global Core Equity (Unhedged)</td><td>$4.8B</td><td>0.36%</td><td>+6.7%</td></tr>
    <tr><td>DFGH</td><td>Dimensional Global Core Equity (Hedged)</td><td>$3.8B</td><td>0.36%</td><td>+16.2%</td></tr>
    <tr><td>DAVA</td><td>Dimensional Australian Value Trust</td><td>$1.4B</td><td>0.34%</td><td>+20.7%</td></tr>
    <tr><td>DGVA</td><td>Dimensional Global Value Trust</td><td>$1.1B</td><td>0.45%</td><td>+11.8%</td></tr>
    <tr><td>DGSM</td><td>Dimensional Global Small Company Trust</td><td>$636M</td><td>0.65%</td><td>+4.6%</td></tr>
  </tbody>
</table>

<h2>How the approach works</h2>
<p>DFA's funds are classified as <em>active ETFs</em> because a portfolio manager makes discretionary implementation decisions. But the strategy itself is systematic: the funds tilt toward stocks with higher expected returns based on three criteria:</p>
<ul>
  <li><strong>Size</strong>: Overweight smaller companies relative to a market-cap-weighted index</li>
  <li><strong>Value</strong>: Overweight stocks with low price-to-book ratios</li>
  <li><strong>Profitability</strong>: Overweight stocks with high operating profitability</li>
</ul>

<p>DFA's DACE (Australian Core Equity) returned <strong>+14.1%</strong> over the past year, significantly outperforming VAS (+7.6%) and A200 (+7.2%). The value and profitability tilts have favoured Australian financials and resources, which performed strongly. Whether this advantage persists through a full market cycle is the central question for potential investors.</p>

<h2>Fee positioning</h2>
<p>At 0.28–0.65%, DFA's fees are higher than plain index ETFs but lower than most active managers. DACE at 0.28% — cheaper than VGS (0.18%) is comparably priced and much cheaper than most active Australian equity funds. This positions DFA as a "better index" rather than competing directly with high-conviction active managers charging 0.80%+.</p>

<p>DFA's rapid accumulation of $18 billion since late 2023 suggests strong demand for its middle-ground positioning — more rigorous than market-cap indexing, more systematic and cheaper than traditional active management.</p>
""",
    },

    {
        "slug": "issuer-profile-magellan",
        "title": "Issuer Profile: Magellan",
        "subtitle": "Australia's most famous active manager had the sharpest rise and the most dramatic fall in recent ETF history. What happened to Magellan, and where does it stand today?",
        "date": "2026-03-17",
        "category": "Issuer Profile",
        "summary": "Magellan Financial Group went from managing $100 billion to less than $40 billion in under two years. Its ETFs have been caught in the fallout. We profile the rise, the collapse, and the cautious recovery.",
        "body": """
<p>No story in Australian asset management has been more dramatic in recent years than Magellan Financial Group. Founded in 2006, it became Australia's most celebrated active manager by delivering exceptional risk-adjusted returns through the 2010s. Then, beginning in late 2021, it unravelled in ways that few had anticipated — producing important lessons for ETF investors about manager risk.</p>

<h2>The rise</h2>
<p>Magellan was co-founded in 2006 by <strong>Hamish Douglass</strong> and <strong>Chris Mackay</strong>. Douglass, the more prominent face, built a reputation as a deep-thinking global equity investor with a particular focus on quality businesses with durable competitive advantages — consumer franchises, payment networks, and US technology companies with genuine moats.</p>

<p>The strategy worked spectacularly through the 2010s. Magellan's global equity strategies delivered strong risk-adjusted returns with lower volatility than the index, making them attractive to both retail investors and institutional clients (superannuation funds). FUM grew from a standing start to over <strong>$100 billion</strong> by 2021, making Magellan one of the largest active managers in Australia by a significant margin.</p>

<p>The <strong>MGOC</strong> ETF (Magellan Global Fund Open Class) listed in 2015 and grew to over $14 billion at its peak — at the time the largest active ETF in the world. Magellan's stock price, listed on the ASX itself, became a bellwether for the company's success.</p>

<h2>The collapse</h2>
<p>The decline began in late 2021. Magellan's global equity portfolio, with significant weights in Chinese technology companies and defensively-positioned global franchises, underperformed severely as global growth stocks sold off and Chinese regulatory action hit the sector. Returns trailed the index substantially — the inverse of what the fund's marketing had promised for a decade.</p>

<p>Investors withdrew funds rapidly. Then, in early 2022, a series of personal and governance shocks made matters worse: Douglass took a medical leave, then resigned; the company's lead institutional client terminated its mandate; and the Magellan-backed FutureFund mandate was also not renewed. FUM fell from $100B+ to under $40B in under two years.</p>

<h2>Where things stand today</h2>
<p>By March 2026, Magellan manages <strong>$7.6 billion</strong> across 6 ETFs. That is a fraction of its peak but still represents a substantial active manager. The firm has undergone significant leadership change and has shifted its positioning toward infrastructure (MICH and MCSI) — where its track record is stronger — while the global equity strategies slowly rebuild credibility under new portfolio management.</p>

<h2>Key products</h2>
<table>
  <thead><tr><th>ETF</th><th>Name</th><th>FUM</th><th>Fee</th><th>1Y Return</th></tr></thead>
  <tbody>
    <tr><td>MGOC</td><td>Magellan Global Fund (Open Class)</td><td>$5.2B</td><td>1.35%</td><td>-5.1%</td></tr>
    <tr><td>AASF</td><td>Airlie Australian Share Fund</td><td>$937M</td><td>0.78%</td><td>-0.04%</td></tr>
    <tr><td>MICH</td><td>Magellan Infrastructure Fund (Hedged)</td><td>$539M</td><td>1.06%</td><td>+16.2%</td></tr>
    <tr><td>MCSI</td><td>Magellan Core Infrastructure Fund</td><td>$510M</td><td>0.50%</td><td>+18.4%</td></tr>
    <tr><td>OPPT</td><td>Magellan Global Opportunities Fund</td><td>$311M</td><td>0.75%</td><td>-6.9%</td></tr>
    <tr><td>MHG</td><td>Magellan Global Equity Fund (Hedged)</td><td>$97M</td><td>1.35%</td><td>+3.0%</td></tr>
  </tbody>
</table>

<h2>Airlie: the Australian equity bright spot</h2>
<p>Magellan acquired <strong>Airlie Funds Management</strong> in 2018, and Airlie has been the more stable part of the business. Airlie manages AASF (Australian Share Fund), a concentrated active Australian equity ETF charging 0.78%. While returns have been modest (-0.04% over 1 year), the team — led by Matt Williams — has maintained a consistent process and avoided the strategic confusion that affected Magellan's global equity products.</p>

<h2>The lesson for ETF investors</h2>
<p>Magellan's trajectory illustrates a risk specific to active ETFs: <em>manager risk</em>. Unlike an index ETF where the methodology is locked down by rules, an active ETF's returns depend on the continued skill and stability of the investment team. When a star manager departs, strategy drifts, or a process breaks down, the ETF's performance can deteriorate rapidly — and because active ETFs are marketed on the strength of past performance, investors may be slow to recognise that the product they bought no longer exists in any meaningful sense.</p>

<p>This is not an argument against all active management. But it does argue for investing in active ETFs based on the robustness of the process, not the track record of a single individual.</p>
""",
    },

    {
        "slug": "what-is-an-etf",
        "title": "What Is an ETF?",
        "subtitle": "Exchange traded funds combine the diversification of a managed fund with the simplicity of buying a single share. Here is how they work.",
        "date": "2026-03-17",
        "category": "Education",
        "summary": "ETFs are the fastest-growing investment vehicle in Australia. We explain what they are, how the creation and redemption mechanism keeps prices fair, and why they have become the default choice for low-cost, diversified investing.",
        "body": """
<p>An <strong>exchange traded fund (ETF)</strong> is a fund that trades on a stock exchange — just like a share in BHP or CBA. When you buy one unit of VAS, for example, you are buying a small slice of a portfolio that holds every company in the S&P/ASX 300 index. The fund does all the work of owning and rebalancing those 300 stocks; you just own one line item in your brokerage account.</p>

<p>Australia now has <strong>472 ETFs</strong> listed on the ASX and Cboe Australia, collectively managing over <strong>$336 billion</strong> in assets. The market has grown at roughly 25% per year for the past decade and shows no sign of slowing.</p>

<h2>How an ETF is different from a managed fund</h2>
<p>Before ETFs, the standard way to access a diversified portfolio was through a <em>managed fund</em> — an unlisted trust where you sent money directly to the fund manager and received units in return. Managed funds are priced once a day (typically at 4pm), and withdrawals can take several days to settle.</p>

<p>ETFs solve both problems. Because units trade continuously on the exchange during market hours, you can buy or sell at any point in the trading day at the current market price. Settlement is T+2, the same as shares.</p>

<h2>The creation and redemption mechanism</h2>
<p>The key to understanding ETFs is the <strong>creation and redemption</strong> process — the system that keeps an ETF's market price closely aligned with the value of its underlying holdings.</p>

<p>Large financial institutions called <em>authorised participants</em> (APs) can create new ETF units by delivering a basket of the underlying securities to the fund manager. In return they receive ETF units to sell on market. They can also do the reverse: redeem ETF units by handing them back and receiving the underlying securities.</p>

<p>This arbitrage mechanism is self-correcting. If ETF units trade at a <em>premium</em> to the underlying basket, APs buy the basket cheaply, create new units, and sell them at the premium — pushing the ETF price back down. If units trade at a <em>discount</em>, APs buy cheap ETF units, redeem them for the basket, and sell the basket — pushing the ETF price back up. In practice, for liquid ETFs, this keeps the gap between market price and Net Asset Value (NAV) to a few cents.</p>

<h2>Index ETFs vs active ETFs</h2>
<p>Most ETFs in Australia are <strong>index ETFs</strong> — they track a published index such as the S&P/ASX 200, the MSCI World, or the Bloomberg AusBond Composite. The portfolio is determined by the index rules, not by a manager making active decisions. This means costs are low: the median Australian index ETF charges around <strong>0.20% per year</strong>.</p>

<p><strong>Active ETFs</strong> use the same exchange-traded wrapper but let a portfolio manager make investment decisions. They typically charge more — the average active ETF in Australia charges around 0.76% — but offer the potential for returns that differ from the index.</p>

<h2>What you own when you buy an ETF</h2>
<p>When you buy units in a fund structured as a trust (most Australian ETFs), you are a <em>beneficiary</em> of the trust — you have a proportional claim on the trust's assets. You do not directly own the underlying shares; the trustee holds them on your behalf. This is an important distinction for tax purposes: capital gains and income are realised at the trust level and distributed to unitholders, with their own tax implications.</p>

<p>Structured products like Global X's ETPMAG (physical silver) are slightly different — they are classified as exchange traded products (ETPs) rather than managed investment trusts, and are backed by physical commodity held in a vault rather than by shares in companies.</p>

<h2>Getting started</h2>
<p>To buy an ETF in Australia you need a brokerage account with a firm that has ASX access — CommSec, SelfWealth, Pearler, Stake, and interactive brokers are common choices. You can search for any ETF by its three- or four-letter code (VAS, NDQ, IVV, etc.) and place a buy order just like you would for a share. Most online brokers charge $5–$15 per trade, and some offer commission-free ETF purchases for accounts under certain thresholds.</p>
""",
    },

    {
        "slug": "etf-costs-explained",
        "title": "ETF Costs Explained: MER, Brokerage and Bid-Ask Spreads",
        "subtitle": "The management fee is only one of three costs you pay when investing in ETFs. Understanding all three is the difference between a good outcome and a mediocre one.",
        "date": "2026-03-17",
        "category": "Education",
        "summary": "Management expense ratios get most of the attention, but brokerage commissions and bid-ask spreads can easily dwarf the MER for small or frequent investors. We break down every cost and show when each one matters most.",
        "body": """
<p>Cost is one of the strongest predictors of long-term investment returns. In a world where future market returns are uncertain, minimising fees is one of the few things investors can control. ETFs are generally cheap — but understanding exactly what you're paying requires looking at three distinct costs.</p>

<h2>1. The management expense ratio (MER)</h2>
<p>The <strong>MER</strong> — sometimes called the management fee or total expense ratio — is an annual charge expressed as a percentage of assets under management. It is deducted from the fund's assets continuously (not billed separately), which means the unit price you see already reflects this cost.</p>

<p>For Australian index ETFs, MERs range from <strong>0.04%</strong> (IVV, A200) at the very cheapest to around <strong>0.69%</strong> for specialised thematic products. The average across all Australian ETFs is roughly <strong>0.40%</strong>. Active ETFs charge more — typically 0.60–1.00% — to cover the cost of portfolio management.</p>

<table>
  <thead><tr><th>Type</th><th>Typical MER range</th><th>Examples</th></tr></thead>
  <tbody>
    <tr><td>Broad index ETFs</td><td>0.04% – 0.20%</td><td>A200 (0.04%), VAS (0.07%), VGS (0.18%)</td></tr>
    <tr><td>Factor / smart beta</td><td>0.25% – 0.50%</td><td>QUAL (0.40%), VDHG (0.27%), DHHF (0.19%)</td></tr>
    <tr><td>Thematic ETFs</td><td>0.45% – 0.69%</td><td>NDQ (0.48%), WIRE (0.65%), XMET (0.69%)</td></tr>
    <tr><td>Active ETFs</td><td>0.60% – 1.20%</td><td>DIVI (0.85%), QGFH (0.92%), MIDS (1.25%)</td></tr>
    <tr><td>Physical commodities</td><td>0.40% – 0.65%</td><td>GOLD (0.40%), ETPMAG (0.49%), QAU (0.58%)</td></tr>
  </tbody>
</table>

<p>Over long periods, even small differences in MER compound significantly. At 7% annual returns, $100,000 invested for 20 years in a 0.04% MER fund grows to approximately $385,000. The same investment in a 0.75% MER fund grows to only $339,000 — a difference of $46,000 on an identical underlying return.</p>

<h2>2. Brokerage commission</h2>
<p>Every time you buy or sell ETF units on the exchange, your broker charges a commission. This is typically a flat fee between <strong>$5 and $15 per trade</strong> for standard retail brokers in Australia, though some platforms (Pearler, Stake) offer commission-free ETF investing under certain conditions.</p>

<p>Brokerage matters most for small or frequent investors. A $10 brokerage fee on a $200 purchase is a 5% drag — dwarfing even the highest ETF MERs. The same $10 on a $5,000 purchase is just 0.2%, which is more reasonable. As a general rule, many advisers suggest a minimum trade size of <strong>$1,000–$2,000</strong> to keep brokerage as a fraction of the investment.</p>

<p>For investors using a <em>dollar-cost averaging</em> strategy (buying a fixed dollar amount on a regular schedule), the math is simple: minimise brokerage by buying less frequently in larger amounts, or use a zero-commission platform.</p>

<h2>3. Bid-ask spread</h2>
<p>When you look at an ETF's price on your broker platform, you'll see two prices: the <em>bid</em> (what buyers are willing to pay) and the <em>ask</em> (what sellers are asking). The difference is the <strong>bid-ask spread</strong>, and it is paid to market makers as compensation for providing liquidity.</p>

<p>For Australia's largest, most liquid ETFs — VAS, VGS, IVV, A200 — the spread is typically just 1–3 cents on a $100+ unit price, making it negligible. For smaller or less-traded ETFs, the spread can be 0.1% to 0.5% or more, adding a hidden cost to every transaction.</p>

<p>Practical tip: always use <strong>limit orders</strong> rather than market orders when buying or selling ETFs. A limit order lets you specify the maximum price you'll pay (or minimum you'll accept), preventing you from accidentally paying an inflated ask in a thin market. Place your limit at or near the mid-price (midpoint between bid and ask) for the best fill.</p>

<h2>How the three costs interact</h2>
<p>For a long-term, buy-and-hold investor making a small number of large purchases, the MER dominates total cost. For an active trader making many small purchases, brokerage and spreads dominate. Most retail investors land somewhere in between.</p>

<p>A useful mental model: <em>the MER eats at your returns slowly and continuously; brokerage and spreads hit you every time you trade</em>. Minimise MER by choosing low-cost funds, and minimise trading costs by keeping transactions infrequent and large.</p>
""",
    },

    {
        "slug": "passive-vs-active-etfs",
        "title": "Passive vs Active ETFs: What's the Difference?",
        "subtitle": "Index ETFs follow rules. Active ETFs have a manager making decisions. The distinction shapes fees, tax efficiency, transparency and long-run performance expectations.",
        "date": "2026-03-17",
        "category": "Education",
        "summary": "Australia's ETF market is split roughly two-to-one between passive index products and active strategies. We explain the core difference, when active might add value, and what the evidence says about each approach.",
        "body": """
<p>When you buy a passive ETF, you are buying the market. When you buy an active ETF, you are hiring a manager to try to beat it. Both use the same exchange-traded wrapper, but they represent fundamentally different investment philosophies.</p>

<h2>How passive (index) ETFs work</h2>
<p>A passive ETF tracks an index — a rules-based list of securities that is constructed and maintained by an independent index provider such as S&P, MSCI, or Bloomberg. The fund's job is to hold the same securities as the index in the same proportions, and to buy or sell only when the index changes.</p>

<p>Because there is no research team deciding what to buy, costs are low. The cheapest Australian index ETFs — BetaShares A200 and iShares IVV — charge just <strong>0.04% per year</strong>. Even the most expensive passive products rarely exceed 0.70%.</p>

<p>Passive ETFs are also highly transparent: the index rules are public, so you always know exactly what you own. VAS holds every company in the ASX 300. VGS holds large and mid-cap companies across 23 developed markets. There are no surprises.</p>

<h2>How active ETFs work</h2>
<p>An active ETF gives a portfolio manager discretion to construct a portfolio that differs from any index. The manager might overweight sectors they find attractive, underweight individual stocks they consider overvalued, or hold cash when they expect a market decline.</p>

<p>Australia now has <strong>143 active ETFs</strong> managing $59.6 billion — a category that barely existed five years ago. Many are conversions of pre-existing unlisted managed funds, which brings the advantage of an established track record before the ETF listing.</p>

<p>Active ETFs charge more. The average fee for an active ETF in Australia is <strong>0.76% per year</strong>, roughly double the passive average. But fees alone do not determine outcomes — the question is whether the manager's skill generates enough extra return to compensate.</p>

<h2>The performance evidence</h2>
<p>The S&P SPIVA report — which tracks how actively managed funds perform against their benchmark indices — has consistently found that a <em>majority</em> of active managers underperform their benchmark index after fees over periods of five years or longer. In Australia, the 10-year SPIVA data shows that roughly 80% of active Australian equity funds underperformed the S&P/ASX 200 index.</p>

<p>This does not mean active management is always inferior. A minority of managers have demonstrated genuine, persistent skill. The challenge for investors is identifying those managers before the fact — and paying fees only when they are justified by expected outperformance.</p>

<h2>When active might make sense</h2>
<p>There are categories where active management has a more credible case:</p>

<ul>
  <li><strong>Less efficient markets</strong>: In small-cap Australian stocks or emerging markets, information is less widely distributed and prices may be more frequently mispriced — giving a skilled analyst a better edge.</li>
  <li><strong>Fixed income</strong>: Active bond managers can add value through duration positioning, credit selection and avoiding defaults in ways that mechanical index-tracking does not.</li>
  <li><strong>Specific mandates</strong>: An active fund with a clear, narrow mandate — such as a quality-screened equity income strategy — may serve a portfolio purpose that no passive index cleanly captures.</li>
</ul>

<h2>A practical framework</h2>
<p>Many investors use a <em>core-satellite</em> approach: a low-cost passive core (VAS + VGS or similar) forms the foundation, representing 70–90% of the portfolio. Active or thematic ETFs make up the satellite — smaller allocations to specific ideas or manager strategies where the investor has a particular conviction.</p>

<p>For most investors starting out, the case for beginning with passive index ETFs is strong: low fees, full transparency, no manager selection risk, and returns that track the market they're invested in. Complexity can be added later, when the reasoning is clear.</p>
""",
    },

    {
        "slug": "building-a-portfolio-with-etfs",
        "title": "Building a Portfolio with ETFs",
        "subtitle": "From a single diversified fund to a multi-asset portfolio — the building blocks available to Australian investors and how they fit together.",
        "date": "2026-03-17",
        "category": "Education",
        "summary": "With 472 ETFs to choose from, building a portfolio can feel overwhelming. We show how a handful of funds can cover Australian shares, international shares, bonds and alternatives — and how to think about combining them.",
        "body": """
<p>One of the most powerful features of ETFs is that you can build a globally diversified portfolio from just two or three funds. Or from one. The depth of the Australian ETF market means investors have an enormous range of building blocks, from ultra-broad diversified funds to narrow single-country or single-commodity products.</p>

<h2>Option 1: One-fund simplicity</h2>
<p>Several Australian ETFs are themselves diversified multi-asset portfolios. Vanguard's VDHG (Diversified High Growth) and BetaShares' DHHF (Diversified All Growth) hold a basket of underlying index ETFs spanning Australian shares, international shares, and bonds. You buy one fund and get exposure to thousands of companies across dozens of countries.</p>

<table>
  <thead><tr><th>ETF</th><th>Allocation</th><th>Fee</th><th>FUM</th></tr></thead>
  <tbody>
    <tr><td>VDHG</td><td>90% growth / 10% defensive</td><td>0.27%</td><td>$2.5B</td></tr>
    <tr><td>DHHF</td><td>100% growth (shares)</td><td>0.19%</td><td>$2.1B</td></tr>
    <tr><td>VDBA</td><td>70% growth / 30% defensive</td><td>0.27%</td><td>$230M</td></tr>
    <tr><td>VDGR</td><td>80% growth / 20% defensive</td><td>0.27%</td><td>$610M</td></tr>
  </tbody>
</table>

<p>These funds are particularly well-suited to investors who want simplicity, or who are starting out and want a sensible default. The main trade-off is slightly less control over the precise allocation and a slightly higher fee than assembling the components individually.</p>

<h2>Option 2: The two or three-fund portfolio</h2>
<p>The classic approach for a growth-oriented investor is a simple split between:</p>
<ul>
  <li><strong>Australian equities</strong>: VAS (ASX 300, 0.07%) or A200 (ASX 200, 0.04%)</li>
  <li><strong>International equities</strong>: VGS (MSCI World, 0.18%) or IVV (S&P 500, 0.04%)</li>
</ul>

<p>A common starting split is 30% Australian / 70% international, though this is a personal decision that depends on your income exposure to Australian assets (if you work in Australia, you already have significant local economic exposure), your currency preference, and your views on the relative valuation of each market.</p>

<p>Adding a third fund — a bond ETF such as VAF (Vanguard Australian Fixed Interest, 0.20%) or VBND (Vanguard Global Bond, 0.20%) — introduces a defensive component that tends to buffer portfolio volatility during equity market downturns.</p>

<h2>The main asset class building blocks</h2>
<table>
  <thead><tr><th>Asset class</th><th>Examples</th><th>Role in portfolio</th></tr></thead>
  <tbody>
    <tr><td>Australian equities</td><td>VAS, A200, IOZ</td><td>Growth, income, AUD exposure</td></tr>
    <tr><td>International equities</td><td>VGS, IVV, IWLD</td><td>Growth, global diversification</td></tr>
    <tr><td>US equities</td><td>IVV, VGAD, QUS</td><td>S&amp;P 500 / US large caps</td></tr>
    <tr><td>Emerging markets</td><td>VGE, EMKT, IEMG</td><td>Higher growth potential, higher volatility</td></tr>
    <tr><td>Australian bonds</td><td>VAF, IAF, AGVT</td><td>Defensive, income, low correlation to equities</td></tr>
    <tr><td>Global bonds (hedged)</td><td>VBND, ILB, BHYB</td><td>Defensive with currency hedge</td></tr>
    <tr><td>Property / REITs</td><td>VAP, MVA, DJRE</td><td>Real asset exposure, income</td></tr>
    <tr><td>Gold</td><td>GOLD, QAU, NUGG</td><td>Inflation hedge, safe haven</td></tr>
  </tbody>
</table>

<h2>Core-satellite: adding targeted exposure</h2>
<p>Once you have a passive core, you can add smaller <em>satellite</em> positions in ETFs with a more specific focus. A satellite position might be:</p>
<ul>
  <li>A thematic ETF (NDQ for US tech, WIRE for copper miners, URNM for uranium)</li>
  <li>A factor ETF (QUAL for quality stocks, MVOL for minimum volatility, VDHG for value)</li>
  <li>An active ETF (a manager you believe will outperform over the long term)</li>
  <li>A sector ETF (MVB for Australian banks, MVR for resources)</li>
</ul>

<p>The satellite should be sized according to your conviction and your tolerance for underperformance relative to the broader market. Most practitioners suggest keeping individual satellite positions to 5–10% of the total portfolio at most.</p>

<h2>Home bias: how much Australian exposure?</h2>
<p>Australia represents about 2% of global equity market capitalisation. A purely market-cap-weighted global portfolio would hold about 2% in Australian stocks and 98% internationally. In practice, most Australian investors hold significantly more than 2% in Australian stocks — a phenomenon called home bias.</p>

<p>There are legitimate reasons for some Australian tilt: Australian dividends carry franking credits that are valuable for Australian tax residents; AUD-denominated assets reduce currency risk for AUD-based investors; and the Australian dividend yield is historically higher than many comparable international markets. A range of 20–40% Australian equities is common, though there is no single correct answer.</p>

<h2>Starting simply</h2>
<p>The best portfolio is one you will actually maintain through market volatility. Complexity adds nothing if it leads to poor decisions under pressure. Many experienced investors look back and wish they had started with something simpler: a two or three-fund portfolio, low fees, broad diversification, and regular contributions. The rest is refinement.</p>
""",
    },

    {
        "slug": "green-metals-etf-boom-2026",
        "title": "Green Metals on the ASX: The ETFs Betting on the Energy Transition",
        "subtitle": "Copper, uranium, lithium and energy transition metals have produced some of the strongest ETF returns of the past year. Here is the full landscape.",
        "date": "2026-03-16",
        "category": "Thematic",
        "summary": "From uranium at +87% to copper miners at +110%, energy transition metals have dominated ETF performance tables. We map the available products, their fees, and the underlying thesis for each.",
        "body": """
<p>The energy transition is not just an infrastructure story — it is a materials story. Solar panels require silver. EVs require copper and lithium. Nuclear power requires uranium. Wind turbines require rare earths. And Australian ETF investors now have more ways than ever to access these themes directly.</p>

<p>Over the past twelve months, energy transition metal ETFs have clustered at the top of the performance tables, led by ETPMAG (silver, +212%) and including significant returns across copper, uranium, and broader transition metals baskets.</p>

<h2>The performance picture</h2>
<table>
  <thead><tr><th>ETF</th><th>Theme</th><th>1Y Return</th><th>3Y Return</th><th>FUM</th><th>Fee</th></tr></thead>
  <tbody>
    <tr><td>ETPMAG</td><td>Physical silver</td><td class="pos">+212.6%</td><td class="pos">+366.0%</td><td>$2.38B</td><td>0.49%</td></tr>
    <tr><td>WIRE</td><td>Copper miners</td><td class="pos">+109.8%</td><td class="pos">+131.8%</td><td>$750M</td><td>0.65%</td></tr>
    <tr><td>XMET</td><td>Energy transition metals basket</td><td class="pos">+116.2%</td><td class="pos">+95.8%</td><td>$123M</td><td>0.69%</td></tr>
    <tr><td>ATOM</td><td>Uranium (Global X)</td><td class="pos">+87.1%</td><td class="pos">+180.5%</td><td>$158M</td><td>—</td></tr>
    <tr><td>URNM</td><td>Uranium (BetaShares)</td><td class="pos">+79.7%</td><td class="pos">+144.2%</td><td>$367M</td><td>—</td></tr>
    <tr><td>GMTL</td><td>Green metal miners</td><td class="pos">+97.9%</td><td class="pos">+39.8%</td><td>$14M</td><td>0.69%</td></tr>
    <tr><td>URAN</td><td>Uranium &amp; energy innovation</td><td>New</td><td>—</td><td>$15M</td><td>0.59%</td></tr>
  </tbody>
</table>

<h2>Copper: the metal the energy transition cannot do without</h2>
<p>Global X's WIRE (Copper Miners ETF) returned <strong>109.8%</strong> over twelve months and now manages $750 million. Copper is arguably the most important metal in the energy transition — electrification at scale requires substantially more copper per unit of energy than the fossil fuel infrastructure it is replacing. The International Energy Agency estimates that a net-zero scenario requires nearly three times current copper production by 2040.</p>

<p>WIRE provides exposure through the equity of copper mining companies rather than the physical metal, which means investors take on both commodity price risk and the operational leverage of the underlying miners. BetaShares' XMET takes a broader approach, holding a basket of energy transition metals including copper, lithium, cobalt and nickel — a more diversified but still highly volatile exposure.</p>

<h2>Uranium: the nuclear renaissance</h2>
<p>Two uranium ETFs compete for investor attention on the ASX: BetaShares' <strong>URNM</strong> ($367M, +79.7%) and Global X's <strong>ATOM</strong> ($158M, +87.1%). Both track indices of uranium mining and related companies. VanEck added a third option in October 2025 with the launch of <strong>URAN</strong>, the Uranium and Energy Innovation ETF, which has attracted $14.6 million in its first few months.</p>

<p>The uranium thesis rests on a global reassessment of nuclear power as a low-carbon baseload energy source. Several G7 nations have reversed previous nuclear phase-out policies, and a number of new reactors are under construction in Asia. Uranium spot prices remain well above the incentive price for new mine development, creating a supply-demand dynamic that advocates argue has further to run.</p>

<h2>The risks</h2>
<p>Thematic concentration is a double-edged sword. The same funds that topped the 12-month performance tables have also, at various points in recent history, delivered severe drawdowns. ATOM's three-year return of +180% masks periods of -40% or worse. Investors allocating to green metals themes should size positions accordingly and be prepared for volatility that significantly exceeds broad market indices.</p>

<p>For a more diversified energy transition exposure with lower fee drag, XMET's basket approach reduces single-commodity risk while retaining the broad theme. The 0.69% fee is mid-range for thematic products in Australia.</p>
""",
    },

    # ── Subordinate Debt / AT1 Migration ──────────────────────────────────────

    {
        "slug": "subordinated-debt-etfs-after-at1-abolition",
        "title": "After the Hybrid Farewell: Where $45B in AT1 Money Is Going",
        "subtitle": "APRA's decision to abolish AT1 hybrids from Australian bank capital stacks has redirected billions toward subordinated debt ETFs. SUBD, BSUB, MQSD and BANK have collectively grown from under $3B to more than $4.7B in twelve months.",
        "date": "2026-03-27",
        "category": "Fixed Income",
        "summary": "When APRA announced in November 2024 that it would phase out Additional Tier 1 (AT1) hybrid securities from the Australian banking system by 2027, it set in motion the largest reallocation in Australian fixed income in a generation. The roughly $45 billion AT1 hybrid market — the home of retail investors seeking bank-backed income — needed somewhere to go. Subordinated debt ETFs have been one of the clearest beneficiaries. SUBD, BSUB, MQSD, BANK and FSUB have collectively gathered more than $4.7 billion in assets, up from under $2.7 billion a year ago.",
        "body": """
<h2>What APRA changed — and why it matters</h2>
<p>In November 2024, the Australian Prudential Regulation Authority announced that it would eliminate Additional Tier 1 (AT1) capital instruments — commonly called hybrids — from the capital structures of Australian banks and insurers. The phase-out runs through to 2032, but new issuance will effectively cease well before that. Banks must replace AT1 with a mix of ordinary equity (CET1) and Tier 2 subordinated debt.</p>

<p>For investors, this announcement was seismic. AT1 hybrids had for two decades been the go-to product for self-managed superannuation funds and income-oriented retail investors: bank-issued, paying floating yields in the 3–5% range above BBSW, and listed on the ASX for daily liquidity. The market totalled approximately $45 billion across dozens of individual securities.</p>

<p>APRA's concern was that retail investors — who dominate ASX hybrid ownership — did not properly understand the bail-in risk embedded in AT1 instruments. In a genuine banking stress scenario, AT1 hybrids can be converted to equity or written to zero without the bank being insolvent. The Credit Suisse AT1 write-down in March 2023, which wiped out CHF 16 billion of AT1 capital while equity holders received partial consideration, provided a vivid international illustration of exactly this risk.</p>

<p>The decision forces income investors to ask: where do we go instead?</p>

<h2>Subordinated debt: the structural alternative</h2>
<p>Tier 2 subordinated debt sits below senior unsecured bonds in the capital hierarchy but above AT1 hybrids. Unlike AT1 securities, Tier 2 bonds cannot be written off or converted at the regulator's discretion — they only absorb losses if a bank actually fails and enters resolution. In practice, given the implicit government backing of Australia's major banks, the credit risk of Tier 2 subordinated debt from CBA, ANZ, Westpac and NAB is regarded as extremely low.</p>

<p>The yield compensation for accepting subordination over senior bonds has historically been in the range of 50–100 basis points. With the RBA cash rate currently at 4.10% and bank senior FRNs yielding approximately 4.0–4.5%, subordinated debt ETFs are delivering 5.0–6.5% distribution yields — competitive with where AT1 hybrids traded, but with a cleaner risk profile.</p>

<div class="chart-box">
  <h3>Subordinated Debt ETFs — FUM Growth (March 2025 → February 2026)</h3>
  <div style="position:relative;height:220px"><canvas id="chart-subdebt-fum"></canvas></div>
</div>
<script>
(function() {
  const labels = ['SUBD','BSUB','MQSD','BANK','FSUB'];
  const mar25  = [2379, 239, 17, 52, 0];
  const feb26  = [3474, 619, 441, 182, 16];
  new Chart(document.getElementById('chart-subdebt-fum'), {
    type: 'bar',
    data: {
      labels,
      datasets: [
        { label: 'Mar 2025 ($M)', data: mar25, backgroundColor: '#3b82f640', borderColor: '#3b82f6', borderWidth: 1, borderRadius: 3, borderSkipped: false },
        { label: 'Feb 2026 ($M)', data: feb26, backgroundColor: '#6366f180', borderColor: '#6366f1', borderWidth: 1, borderRadius: 3, borderSkipped: false },
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: 'bottom', labels: { boxWidth: 10, font: { size: 10 } } } },
      scales: {
        x: { ticks: { font: { size: 10 } }, grid: { display: false } },
        y: { ticks: { font: { size: 10 }, callback: v => '$' + v.toLocaleString() + 'M' }, grid: { color: '#1e3860' } }
      }
    }
  });
})();
</script>

<h2>The five products and how they differ</h2>
<table>
  <thead><tr><th>ETF</th><th>Issuer</th><th>Strategy</th><th>1Y Return</th><th>Yield</th><th>MER</th><th>FUM</th></tr></thead>
  <tbody>
    <tr><td>SUBD</td><td>VanEck</td><td>Investment grade sub-debt index (iBoxx)</td><td class="pos">+5.84%</td><td>5.35%</td><td>0.29%</td><td>$3,474M</td></tr>
    <tr><td>BSUB</td><td>BetaShares</td><td>Big 4 + Macquarie FRN sub-debt index</td><td class="pos">+5.57%</td><td>4.80%</td><td>0.29%</td><td>$619M</td></tr>
    <tr><td>MQSD</td><td>Macquarie</td><td>Active — sub-debt + senior credit blend</td><td class="pos">+6.46%</td><td>5.47%</td><td>n/a</td><td>$441M</td></tr>
    <tr><td>BANK</td><td>Global X</td><td>Australian bank credit (sub-debt + senior)</td><td class="pos">+4.50%</td><td>4.42%</td><td>0.25%</td><td>$182M</td></tr>
    <tr><td>FSUB</td><td>VanEck</td><td>Fixed rate sub-debt (complement to SUBD)</td><td>—</td><td>1.26%†</td><td>0.29%</td><td>$16M</td></tr>
  </tbody>
</table>
<p class="caption">† FSUB launched December 2025 and holds fixed-rate securities; distribution yield reflects short history and mark-to-market dynamics rather than ongoing income. Data as at February 2026.</p>

<h3>SUBD — the market leader</h3>
<p>VanEck's SUBD is the category's dominant product, with $3.47 billion in assets and a three-year track record that spans a full interest rate cycle. The fund tracks the iBoxx AUD Investment Grade Subordinated Debt Index, providing diversified exposure to Tier 2 subordinated FRNs from Australian banks and other investment grade issuers. Units on issue grew from approximately 97 million in March 2025 to 141 million in February 2026 — a 45% increase in twelve months — which implies net inflows of roughly $1.1 billion over the period.</p>

<p>SUBD's benchmark includes FRN (floating rate note) structures, meaning the portfolio's coupon payments move with BBSW rather than being fixed at issuance. This gives the fund interest rate duration close to zero — very different from a long-duration corporate bond fund — while still providing credit spread compensation. It is, in structure, a closer substitute for AT1 hybrids than senior floating rate products like QPON.</p>

<h3>BSUB — concentrated bank exposure</h3>
<p>BetaShares' BSUB is more concentrated: it holds subordinated FRNs issued by Australia's Big 4 banks plus Macquarie, providing a purer play on the major bank credit curve. The fund has grown rapidly — units on issue nearly tripled from under 10 million in March 2025 to over 24 million in February 2026, with FUM climbing from approximately $239 million to $619 million. The 0.29% management fee matches SUBD.</p>

<p>The tradeoff versus SUBD is concentration: investors who believe Australian major banks are effectively risk-free may prefer BSUB's tighter focus and higher yield relative to the broader SUBD universe. Those seeking broader issuer diversification will lean toward SUBD.</p>

<h3>MQSD — the active manager</h3>
<p>Macquarie's MQSD takes an active approach, holding a blend of subordinated debt and senior credit across Australian banks and financials. Its 6.46% one-year return is the highest in the category, and its 5.47% distribution yield is the most attractive on a current-income basis. MQSD has grown extraordinarily fast: from approximately $17 million at inception in early 2025 to $441 million in a single year, suggesting strong institutional as well as retail demand for an actively managed subordinated credit product.</p>

<h3>BANK and FSUB — rounding out the landscape</h3>
<p>Global X's BANK takes the widest mandate of the group, holding both senior and subordinated bank credit across the Australian banking system. Its lower yield (4.42%) reflects the inclusion of senior, lower-yielding paper. It is better characterised as a broad Australian bank credit ETF than a pure sub-debt play.</p>

<p>VanEck's FSUB, which launched in December 2025, targets fixed-rate subordinated debt — a structural complement to SUBD's floating-rate focus. FSUB will have interest rate duration and will benefit from falling rates in ways that SUBD will not. Its $16 million in assets reflects an early stage of launch rather than a long-term FUM position.</p>

<h2>How these compare to HBRD and AT1 hybrids</h2>
<p>BetaShares' HBRD — the hybrid market's primary ETF vehicle, with $2.57 billion in assets — continues to hold AT1 instruments issued under the current regulatory regime. As those instruments mature and are not replaced, HBRD will need to reinvest proceeds into alternative securities. The fund's managers are aware of this dynamic and have indicated a transition toward Tier 2 subordinated debt as AT1 supply diminishes.</p>

<p>The direct comparison between HBRD and SUBD is instructive. HBRD returned 4.75% over the past year with a 5.30% distribution yield at a 0.55% management fee. SUBD returned 5.84% over the same period with a 5.35% yield at 0.29%. The subordinated debt product outperformed the hybrid product on both return and fee in the most recent year — though the longer-term track record favours AT1 hybrids in periods of credit spread compression.</p>

<div class="chart-box">
  <h3>Income Positioning: Yield vs Duration (approximate)</h3>
  <div style="position:relative;height:240px"><canvas id="chart-yield-dur"></canvas></div>
</div>
<script>
(function() {
  const datasets = [
    { label: 'MQSD', data: [{x: 0.3, y: 5.47}], backgroundColor: '#6366f1', pointRadius: 8, pointHoverRadius: 10 },
    { label: 'SUBD', data: [{x: 0.4, y: 5.35}], backgroundColor: '#3b82f6', pointRadius: 8, pointHoverRadius: 10 },
    { label: 'HBRD (AT1)', data: [{x: 0.5, y: 5.30}], backgroundColor: '#f59e0b', pointRadius: 8, pointHoverRadius: 10 },
    { label: 'BSUB', data: [{x: 0.4, y: 4.80}], backgroundColor: '#8b5cf6', pointRadius: 8, pointHoverRadius: 10 },
    { label: 'BANK', data: [{x: 0.5, y: 4.42}], backgroundColor: '#0ea5e9', pointRadius: 8, pointHoverRadius: 10 },
    { label: 'QPON (Senior FRN)', data: [{x: 0.2, y: 4.30}], backgroundColor: '#6b7280', pointRadius: 8, pointHoverRadius: 10 },
    { label: 'CRED (IG Corp)', data: [{x: 3.2, y: 5.00}], backgroundColor: '#10b981', pointRadius: 8, pointHoverRadius: 10 },
  ];
  new Chart(document.getElementById('chart-yield-dur'), {
    type: 'scatter',
    data: { datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: {
        legend: { position: 'right', labels: { boxWidth: 8, font: { size: 9 } } },
        tooltip: { callbacks: { label: ctx => ctx.dataset.label + ': ' + ctx.parsed.y.toFixed(2) + '% yield, ~' + ctx.parsed.x.toFixed(1) + 'yr dur' } }
      },
      scales: {
        x: { title: { display: true, text: 'Approx. Interest Rate Duration (years)', font: { size: 10 } }, ticks: { font: { size: 10 } }, grid: { color: '#1e3860' } },
        y: { title: { display: true, text: 'Distribution Yield (%)', font: { size: 10 } }, ticks: { font: { size: 10 }, callback: v => v + '%' }, grid: { color: '#1e3860' } }
      }
    }
  });
})();
</script>

<h2>The structural shift in bank capital</h2>
<p>APRA's policy change increases the supply of Tier 2 subordinated debt from Australian banks — banks that previously issued AT1 must now issue more Tier 2 to maintain their capital adequacy ratios. More supply, all else equal, would widen credit spreads and increase yields on subordinated debt. Whether this plays out in practice depends on whether demand from investors like the sub-debt ETFs and direct institutional buyers keeps pace with supply growth.</p>

<p>The early evidence suggests demand is robust. The combined inflows into SUBD, BSUB, MQSD and BANK over the past twelve months approximate $2 billion — a meaningful redirection of capital that would previously have flowed into ASX-listed hybrid securities. The structural tailwind of AT1 phase-out is a multi-year phenomenon: as existing AT1 instruments mature through to 2032, each maturity event potentially recycles more capital toward subordinated debt products.</p>

<h2>What investors should consider</h2>
<p>Subordinated debt ETFs are not a like-for-like replacement for AT1 hybrids in all respects. AT1 hybrids carried an equity conversion risk that sub-debt does not — which is both a risk reduction and a yield reduction relative to what AT1s offered when spreads were wide. The current sub-debt yields of 5–6.5% are attractive in absolute terms and relative to current short-term rates, but they embed meaningful bank credit exposure.</p>

<p>For income-oriented investors navigating the post-AT1 landscape, SUBD (for broad floating-rate sub-debt exposure at 0.29%) and MQSD (for an actively managed, higher-yield approach) appear to be the products the market has voted for with its capital. BSUB offers a purer major-bank concentration play. FSUB provides a rate-sensitive fixed-rate complement that will behave differently from the floating-rate products in an easing cycle.</p>

<p>The scale of the AT1 market — $45 billion — dwarfs the current $4.7 billion in sub-debt ETFs. The reallocation is still in early innings.</p>
""",
    },

    # ── Active ETF Surge ───────────────────────────────────────────────────────

    {
        "slug": "active-etf-surge-2025",
        "title": "The Active Revolution: How Active ETFs Came to Dominate New Launches",
        "subtitle": "In 2025, active ETFs outnumbered new passive launches by nearly four to one. The Dimensional phenomenon — $17.5B across six funds in two years — is reshaping what Australian investors expect from professionally managed portfolios.",
        "date": "2026-03-27",
        "category": "Market Trends",
        "summary": "The structure of the Australian ETF market is undergoing a quiet revolution. In 2025, 58 active ETFs launched versus just 15 passive products — a nearly four-to-one ratio that would have been unthinkable five years ago. The catalyst was partly regulatory (ASIC's 2019 active ETF framework) and partly the Dimensional Asset Management effect: DACE, DGCE and DFGH collectively attracted $17.5 billion in assets after converting from managed funds to ETFs in November 2023, demonstrating that institutional-quality active management and ETF wrapper convenience are not mutually exclusive.",
        "body": """
<h2>The numbers tell the story</h2>
<p>In 2025, Australian fund managers launched <strong>58 active ETFs</strong> and just <strong>15 passive index products</strong>. That four-to-one ratio represents a structural inflection point. For most of the ETF market's history in Australia, the product pipeline skewed heavily passive — in 2017 and 2018, passive launches dominated. The shift has been building for several years, but 2025 marked the year active launches became the clear majority by any measure.</p>

<p>The aggregate numbers are equally stark. Australia's ETF market now holds $327 billion in total assets. Of that, passive products — index-tracking ETFs that have been the traditional core of the market — account for $265.8 billion across 217 funds. Active ETFs hold $61.3 billion across 260 funds. Passive still holds a four-to-one FUM advantage, but that reflects a fifteen-year head start: the oldest ETFs in Australia, like STW (listed 2001) and VAS (2009), have had decades to compound inflows. The new product pipeline tells a very different story about where the industry is heading.</p>

<div class="chart-box">
  <h3>New ETF Launches by Year — Active vs Passive</h3>
  <div style="position:relative;height:240px"><canvas id="chart-launches-type"></canvas></div>
</div>
<script>
(function() {
  const years   = ['2019','2020','2021','2022','2023','2024','2025'];
  const active  = [11, 13, 18, 22, 32, 40, 58];
  const passive = [7,  12,  7, 13, 22, 24, 15];
  new Chart(document.getElementById('chart-launches-type'), {
    type: 'bar',
    data: {
      labels: years,
      datasets: [
        { label: 'Active',  data: active,  backgroundColor: '#6366f180', borderColor: '#6366f1', borderWidth: 1, borderRadius: 3 },
        { label: 'Passive', data: passive, backgroundColor: '#3b82f640', borderColor: '#3b82f6', borderWidth: 1, borderRadius: 3 },
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: 'bottom', labels: { boxWidth: 10, font: { size: 10 } } } },
      scales: {
        x: { ticks: { font: { size: 10 } }, grid: { display: false } },
        y: { ticks: { font: { size: 10 } }, grid: { color: '#1e3860' }, title: { display: true, text: 'Products Launched', font: { size: 10 } } }
      }
    }
  });
})();
</script>

<h2>The Dimensional effect</h2>
<p>No story about Australia's active ETF market is complete without Dimensional Fund Advisors. In November 2023, DFA converted three of its largest managed funds to ASX-listed active ETFs: DACE (Australian Core Equity), DGCE (Global Core Equity Unhedged), and DFGH (Global Core Equity Hedged). At conversion, these products already had decades of institutional assets behind them. The move brought them onto the ASX for the first time, making them accessible to retail investors, financial advisers, and self-managed superannuation funds without minimum investment thresholds.</p>

<p>The result has been one of the most remarkable growth stories in Australian ETF history. DACE now holds <strong>$6.31 billion</strong> — making it the ninth-largest ETF on the ASX and one of only two products launched after 2020 to crack the top ten by FUM. DGCE holds $4.83 billion and DFGH $3.76 billion. Together, the three November 2023 conversions manage $14.9 billion. Dimensional then listed three additional ETFs in August 2024 — DAVA (Australian Value), DGVA (Global Value) and DGSM (Global Small Company) — which have collectively attracted a further $3.1 billion. The Dimensional ETF range now manages $17.5 billion across six products, all active, none with a publicly disclosed expense ratio.</p>

<table>
  <thead><tr><th>ETF</th><th>Strategy</th><th>Listed</th><th>FUM</th><th>1Y Return</th></tr></thead>
  <tbody>
    <tr><td>DACE</td><td>Australian Core Equity</td><td>Nov 2023</td><td>$6,307M</td><td class="pos">+14.05%</td></tr>
    <tr><td>DGCE</td><td>Global Core Equity (Unhedged)</td><td>Nov 2023</td><td>$4,830M</td><td class="pos">+6.70%</td></tr>
    <tr><td>DFGH</td><td>Global Core Equity (Hedged)</td><td>Nov 2023</td><td>$3,758M</td><td class="pos">+16.22%</td></tr>
    <tr><td>DAVA</td><td>Australian Value</td><td>Aug 2024</td><td>$1,398M</td><td class="pos">+20.66%</td></tr>
    <tr><td>DGVA</td><td>Global Value</td><td>Aug 2024</td><td>$1,075M</td><td class="pos">+11.77%</td></tr>
    <tr><td>DGSM</td><td>Global Small Company</td><td>Aug 2024</td><td>$648M</td><td class="pos">+4.64%</td></tr>
  </tbody>
</table>

<p>Dimensional's approach is often described as "systematic active" — it does not track an index, but nor does it rely on stock-picking in the traditional sense. The funds tilt toward documented risk premia: value, profitability, and size, in proportions determined by rules-based factor models rather than individual analyst conviction. This places Dimensional in a conceptual space between passive index investing and traditional stock-picking active management — and its growth suggests that investors find this positioning attractive at a time when both passive concentration risk and traditional active fund manager underperformance are live concerns.</p>

<h2>Beyond Dimensional: the broader active surge</h2>
<p>Dimensional accounts for $17.5 billion of the active market's $61.3 billion total, but the remaining $43.8 billion tells its own story. The cohort of large active ETFs includes names that will be familiar to long-term investors in Australian managed funds: Magellan ($5.1B for MGOC), Plato Global Alpha ($1.05B for PGA1), Antipodes ($373M for AGX1), Platinum ($299M for PAXX), and Pendal. These are fund managers with decades of track records who have found the ETF wrapper to be a more efficient distribution channel than traditional unlisted managed fund structures.</p>

<p>The ETF format offers genuine advantages for active managers: daily NAV transparency, no investor entry and exit spread costs on secondary market trades, ASX listing providing superannuation fund eligibility, and no minimum investment requirement. For advisers recommending products to clients across diverse account sizes, ETF-wrapped active funds are simply easier to work with.</p>

<h2>The fee gap — and what performance data says</h2>
<p>Active ETFs charge more: the market-wide average expense ratio is 0.52% for active products versus 0.37% for passive. For a $100,000 portfolio, that difference is $150 per year — compounding to a meaningful drag over a decade. The question is whether active outperformance justifies the cost.</p>

<p>The data available is mixed. Among Australian equity active ETFs with one-year return data, DAVA (Dimensional Australian Value) returned +20.66% versus VAS (Vanguard Australian Shares, passive) at +7.61% — a remarkable gap. DACE (Dimensional Australian Core) returned +14.05% versus VAS's 7.61%. In international equities, DFGH returned +16.22% versus VGS (passive global) at approximately +6%, and PGA1 (Plato Global Alpha) delivered +24.22%. These are single-year comparisons that don't account for fees in the Dimensional case, but the magnitude of outperformance over 2025 suggests the active tilt was real.</p>

<p>The harder test comes over full market cycles. The one-year period to early 2026 was unusual in several respects — particularly the divergence between hedged and unhedged returns as the AUD moved — and active managers with factor tilts (particularly value and quality) benefited from a factor environment that suited their models. Whether these results persist in a more benign or growth-oriented market environment remains to be seen.</p>

<h2>What the active surge means for the market</h2>
<p>The rise of active ETFs is changing the competitive dynamics of the industry in ways that go beyond product counts. Passive giants like Vanguard, iShares and BetaShares have responded by launching ultra-cheap core products (VTS at 0.03%, A200 at 0.04%) that make the cost of passive ownership negligibly small — effectively daring active managers to justify their fees through performance alone. Meanwhile, the active managers are competing on both performance and the ETF convenience that was previously a passive-only advantage.</p>

<p>The 2025 launch pipeline — 58 active products across Australian equities, international equities, fixed income, thematic and alternatives — suggests this trend has not peaked. Every traditional fund manager that converts an unlisted fund to an ETF structure brings an established investor base onto the ASX, seeding new products with meaningful day-one assets. The 2025 active ETF cohort began their lives with $8.7 billion in combined FUM — vastly more than the typical passive ETF launch.</p>

<p>The Australian ETF market in 2030 will almost certainly look different from today: more active products, more institutional assets, and a passive core that remains dominant in broad-market exposure but increasingly shares shelf space with systematic and active alternatives. The question for investors is not whether active ETFs have a place in portfolios — at $61 billion, that question has been answered — but which active strategies will justify their fees over the full market cycle.</p>
""",
    },

    # ── Fee War ────────────────────────────────────────────────────────────────

    {
        "slug": "etf-fee-war-australia-2026",
        "title": "Four Dollars a Year: The ETF Fee War and What It Means for Your Portfolio",
        "subtitle": "A $100,000 investment in Australia's cheapest broad-market ETFs now costs as little as $40 per year in management fees. The race to zero has created extraordinary value for long-term investors — but fees remain stubbornly high in niche categories.",
        "date": "2026-03-27",
        "category": "Costs & Fees",
        "summary": "The management fees charged by Australian ETF providers have been falling for two decades, and the trajectory shows no sign of stopping. VTS (Vanguard US Total Market) charges just 0.03% — $3 per year on a $10,000 investment. A200 (BetaShares), IVV (iShares S&P 500) and VEU (Vanguard All-World ex-US) all sit at 0.04%. The fee war has been decisively won for investors in broad-market equity exposure, and its compounding benefits over twenty years are substantial. But the same competitive pressure has barely touched thematic ETFs (averaging 0.53%), currency products (1.07%) and some active funds — creating a barbell market where cheap and expensive products coexist.",
        "body": """
<h2>The race to zero</h2>
<p>When State Street launched Australia's first ETF — STW, tracking the ASX 200 — in August 2001, its management expense ratio was 0.286%. That was considered competitive for the era. Today, STW charges 0.05% and manages $6.24 billion. The 0.236% fee reduction, on STW's current asset base, represents $14.7 million per year in savings for investors relative to its launch-era pricing. Compounded across the industry over two decades, the fee compression story is one of the most meaningful wealth transfers in Australian financial history — from fund managers to investors.</p>

<p>The current fee floor sits at <strong>0.03%</strong>, charged by Vanguard's VTS (US Total Market Shares Index ETF, $5.97 billion). VTS is unusual because it achieves this fee by holding US-domiciled Vanguard ETF units rather than directly holding securities — a fund-of-fund structure that passes through Vanguard US's enormous scale advantage. On the same $5.97 billion base, 0.03% generates approximately $1.8 million per year in management fees. That is less than some small-cap ETF managers earn on $50 million in assets.</p>

<div class="chart-box">
  <h3>Management Fees: Broad Market ETFs (Largest Products)</h3>
  <div style="position:relative;height:260px"><canvas id="chart-fee-core"></canvas></div>
</div>
<script>
(function() {
  const data = [
    { code: 'VTS',  fee: 0.03, fum: 5972,  issuer: 'Vanguard' },
    { code: 'IVV',  fee: 0.04, fum: 12294, issuer: 'iShares' },
    { code: 'A200', fee: 0.04, fum: 9130,  issuer: 'BetaShares' },
    { code: 'VEU',  fee: 0.04, fum: 5149,  issuer: 'Vanguard' },
    { code: 'STW',  fee: 0.05, fum: 6241,  issuer: 'SPDR' },
    { code: 'VAS',  fee: 0.07, fum: 22560, issuer: 'Vanguard' },
    { code: 'V500', fee: 0.07, fum: 24,    issuer: 'Vanguard' },
    { code: 'BGBL', fee: 0.08, fum: 3355,  issuer: 'BetaShares' },
    { code: 'IOZ',  fee: 0.09, fum: 7998,  issuer: 'iShares' },
  ];
  const labels = data.map(d => d.code);
  const fees   = data.map(d => d.fee);
  const fums   = data.map(d => d.fum);
  new Chart(document.getElementById('chart-fee-core'), {
    type: 'bar',
    data: {
      labels,
      datasets: [
        { label: 'MER (%)',  data: fees, backgroundColor: '#3b82f680', borderColor: '#3b82f6', borderWidth: 1, borderRadius: 3, borderSkipped: false, yAxisID: 'y' },
        { label: 'FUM ($M)', data: fums, type: 'line', borderColor: '#f59e0b', backgroundColor: 'transparent', pointRadius: 4, borderWidth: 2, yAxisID: 'y2' },
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { position: 'bottom', labels: { boxWidth: 10, font: { size: 10 } } } },
      scales: {
        x:  { ticks: { font: { size: 10 } }, grid: { display: false } },
        y:  { ticks: { font: { size: 10 }, callback: v => v.toFixed(2) + '%' }, grid: { color: '#1e3860' }, title: { display: true, text: 'MER (%)', font: { size: 10 } } },
        y2: { position: 'right', ticks: { font: { size: 10 }, callback: v => '$' + (v/1000).toFixed(0) + 'B' }, grid: { display: false }, title: { display: true, text: 'FUM', font: { size: 10 } } },
      }
    }
  });
})();
</script>

<h2>What these fees mean in dollars</h2>
<p>Fee percentages can obscure the real magnitude of differences. On a $100,000 portfolio held for twenty years and growing at 8% per annum:</p>

<table>
  <thead><tr><th>ETF</th><th>MER</th><th>Annual fee on $100K</th><th>20-year fee drag (est.)</th></tr></thead>
  <tbody>
    <tr><td>VTS</td><td>0.03%</td><td>$30</td><td>~$1,400</td></tr>
    <tr><td>A200 / IVV / VEU</td><td>0.04%</td><td>$40</td><td>~$1,900</td></tr>
    <tr><td>VAS</td><td>0.07%</td><td>$70</td><td>~$3,300</td></tr>
    <tr><td>IOZ</td><td>0.09%</td><td>$90</td><td>~$4,200</td></tr>
    <tr><td>Average passive ETF</td><td>0.37%</td><td>$370</td><td>~$17,000</td></tr>
    <tr><td>Average active ETF</td><td>0.52%</td><td>$520</td><td>~$23,800</td></tr>
    <tr><td>QUAL (VanEck Quality)</td><td>0.35%</td><td>$350</td><td>~$16,000</td></tr>
    <tr><td>NDQ (Nasdaq 100)</td><td>0.48%</td><td>$480</td><td>~$21,900</td></tr>
  </tbody>
</table>
<p class="caption">Twenty-year fee drag estimated using a simplified model: FV of annual fee payments growing at 8% annually. Actual drag depends on portfolio growth and fee timing.</p>

<p>The comparison between VTS at $30/year and an average active ETF at $520/year — on the same $100,000 — is a $490 annual difference that compounds over decades. Over twenty years, an investor in the cheapest passive products foregoes approximately $21,000 less to fees than one invested in the average active fund. This is the core argument of index investing, made concrete in Australian dollar terms.</p>

<h2>Where the fee war has not reached</h2>
<p>Fee compression has been uneven. Broad-market equities — where the products are commoditised and competition is intense — have seen the most dramatic falls. But several categories remain expensive by any objective measure:</p>

<ul>
  <li><strong>Currency ETFs</strong> average 1.07%, with products like ZUSD (US Dollar ETF) charging 1.38%. These are niche products with limited competition.</li>
  <li><strong>Thematic ETFs</strong> average 0.53%, with defence (ARMR: 0.55%, DFND: 0.65%), uranium (URNM: 0.69%), and copper (WIRE: 0.65%) all in the top half of the fee distribution. Issuers justify this on the basis of portfolio construction complexity and research costs.</li>
  <li><strong>Commodities</strong> average 0.55%, ranging from GLDN (Physical Gold, iShares) at 0.18% to leveraged commodity futures products at 1.29%.</li>
  <li><strong>Some active managers</strong> — including Dimensional, Platinum, Plato and several others — do not publicly disclose their expense ratios through ASX data, making comparison difficult for retail investors.</li>
</ul>

<div class="chart-box">
  <h3>Average MER by Asset Class</h3>
  <div style="position:relative;height:240px"><canvas id="chart-mer-class"></canvas></div>
</div>
<script>
(function() {
  const labels = ['Currency','Alternatives','Commodities','Thematic','Intl Equities','Digital Assets','Aust Equities','Fixed Income','Diversified','Property','Cash'];
  const data   = [1.07, 1.00, 0.55, 0.53, 0.48, 0.45, 0.37, 0.35, 0.31, 0.24, 0.18];
  const colors = data.map(v => v > 0.5 ? '#f8717180' : v > 0.25 ? '#fbbf2480' : '#4ade8080');
  const borders = data.map(v => v > 0.5 ? '#f87171' : v > 0.25 ? '#fbbf24' : '#4ade80');
  new Chart(document.getElementById('chart-mer-class'), {
    type: 'bar',
    data: { labels, datasets: [{ label: 'Avg MER (%)', data, backgroundColor: colors, borderColor: borders, borderWidth: 1, borderRadius: 3, borderSkipped: false }] },
    options: {
      indexAxis: 'y', responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false }, tooltip: { callbacks: { label: c => c.parsed.x.toFixed(2) + '%' } } },
      scales: {
        x: { ticks: { font: { size: 10 }, callback: v => v + '%' }, grid: { color: '#1e3860' } },
        y: { ticks: { font: { size: 10 } }, grid: { display: false } }
      }
    }
  });
})();
</script>

<h2>The pressure builds: new entrants and fee matching</h2>
<p>The fee war continues to claim new victims. VanEck launched A300 (Australia 300 ETF) at 0.04%, directly matching A200 and staking a claim at the cheapest end of the Australian equities market. Vanguard's new V500 (S&P 500) launched at 0.07% — cheaper than IVV's 0.04% only in the sense that Vanguard's structure passes through lower underlying costs — and V5AH (hedged variant) at 0.09%.</p>

<p>BetaShares has taken an interesting approach with HGBL (Global Shares Currency Hedged, $1.99 billion) at 0.11% — positioning it as a "core-plus" product that provides hedged global exposure at a price point well below the 0.20–0.30% typical for hedged products historically. BGBL (unhedged, $3.36 billion) at 0.08% is one of the cheapest unhedged global equity ETFs in the market. These are products designed to capture fee-sensitive investors who have graduated from the cheapest tier but don't want to pay thematic or factor premiums.</p>

<h2>The fee-performance relationship in practice</h2>
<p>A lower fee does not guarantee better net performance, but in the absence of genuine alpha, it is the most reliable lever. The one-year return data reveals the challenge active managers face: VAS (0.07% fee) returned 7.61% over the past year; DACE (Dimensional Australian Core, active, undisclosed fee) returned 14.05%. On that single-year basis, DACE's outperformance was extraordinary — but Dimensional's factor tilts happened to align with the 2025 market environment.</p>

<p>The more consistent picture comes from the middle ground: factor ETFs like QUAL (VanEck Quality, 0.35%) returned 5.06% over 1 year, less than the passive VAS at a higher fee. VLUE (VanEck Value, 0.28%) returned 26.5% — dramatically outperforming, but driven by a value factor environment rather than any discretionary insight. NDQ (Nasdaq 100, 0.48%) returned 2.2% in a year where US tech lagged — a case where paying more for concentration delivered less.</p>

<h2>Practical guidance for fee-conscious investors</h2>
<p>The data suggests a clear hierarchy for investors seeking to minimise fee drag on core exposures:</p>

<ol>
  <li><strong>Australian equities core:</strong> A200 (0.04%), STW (0.05%) or VAS (0.07%) — at these levels, the fee difference is economically trivial and other factors (issuer risk, distribution timing, tax optimisation) dominate.</li>
  <li><strong>Global equities unhedged:</strong> VTS (0.03%), IVV/VEU (0.04%), BGBL (0.08%) — highly competitive; choose based on index, geographic scope, and structure preference.</li>
  <li><strong>Hedged global:</strong> HGBL (0.11%) or VGAD (0.21%) — the hedging mechanics add costs that cannot compress as far as unhedged products; 0.11% is now the market floor.</li>
  <li><strong>Fixed income:</strong> VAF, VBND and the senior floating rate products (QPON at 0.22%) represent reasonable value; subordinated debt ETFs at 0.25–0.29% carry a justified premium for the additional complexity.</li>
  <li><strong>Thematic and active:</strong> Fee compression has been limited. Investors should satisfy themselves that the strategy's return potential — not just the performance chart — justifies fees of 0.50%+.</li>
</ol>

<p>The fee war's lasting legacy is that the cost of building a diversified, globally exposed Australian portfolio has never been lower. A straightforward three-ETF portfolio — VAS (Australian equities), IVV (US equities), VEU (international ex-US) — can be assembled with a blended fee of approximately 0.05%. On a $500,000 portfolio, that is $250 per year. A generation ago, the equivalent managed fund portfolio would have cost 1.5% annually — $7,500. The difference, compounded over twenty years, is retirement-defining money.</p>
""",
    },
]


def get_article(slug):
    for a in ARTICLES:
        if a["slug"] == slug:
            return a
    return None


def get_all_articles():
    return sorted(ARTICLES, key=lambda a: a["date"], reverse=True)
