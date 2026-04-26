

      (function () {
        var acctBadge = document.getElementById('acct-badge');
        if (!acctBadge || typeof MY_ADDR !== 'string' || !MY_ADDR) {
          return;
        }

        var shortAddr = MY_ADDR.length > 10
          ? MY_ADDR.slice(0, 6) + '...' + MY_ADDR.slice(-4)
          : MY_ADDR;

        acctBadge.title = MY_ADDR;
        acctBadge.textContent = '⬡ ' + shortAddr;
        acctBadge.onclick = function () {
          window.open('https://polymarket.com/profile/' + encodeURIComponent(MY_ADDR), '_blank');
        };
      })();
    

// ── ACCOUNT (loaded from gitignored config.js) ───────────────────────────────
const CFG       = window.POLY_CONFIG || {};
const MY_ADDR   = CFG.address       || '0x36576E80353D35B2Fa00520cD96823861fD922DF';
const API_KEY   = CFG.apiKey        || '';
const KEY_ADDR  = CFG.apiKeyAddress || '';

const DATA_API  = 'https://data-api.polymarket.com';
const GAMMA_API = 'https://gamma-api.polymarket.com';
const CLOB_API  = 'https://clob.polymarket.com';
const POLY_URL  = 'https://polymarket.com';

// ── WATCHED WALLETS (COPYTRADE ENGINE) ───────────────────────────────────────
const WATCHED_WALLETS = [
  { addr:"0x751a2b86cab503496efd325c8344e10159349ea1", portfolio: 52805.82, mkts:20 },
  { addr:"0xde17f7144fbd0eddb2679132c10ff5e74b120988", portfolio: 0,        mkts:50 },
  { addr:"0x1979ae6b7e6534de9c4539d0c205e582ca637c9d", portfolio: 0,        mkts:6  },
  { addr:"0x59a0744db1f39ff3afccd175f80e6e8dfc239a09", portfolio: 0,        mkts:14 },
  { addr:"0x0afc7ce56285bde1fbe3a75efaffdfc86d6530b2", portfolio: 0,        mkts:0  },
];

let TICKER_ITEMS = [
  { lbl:"BTC",      val:"...",     up:true  },
  { lbl:"ETH",      val:"...",     up:true  },
  { lbl:"SOL",      val:"...",     up:true  },
  { lbl:"DOGE",     val:"...",     up:true  },
  { lbl:"MATIC",    val:"...",     up:true  },
  { lbl:"POLY VOL", val:"$48.2M",  up:true  },
  { lbl:"FED RATE", val:"4.50%",   up:false },
  { lbl:"USDT DOM", val:"4.82%",   up:false },
  { lbl:"VIX",      val:"18.3",    up:false },
];

async function loadTicker() {
  try {
    const data = await fetchWithProxy(
      'https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,solana,dogecoin,matic-network&vs_currencies=usd&include_24hr_change=true'
    );
    const map = {
      BTC:   { id:'bitcoin',       sym:'$' },
      ETH:   { id:'ethereum',      sym:'$' },
      SOL:   { id:'solana',        sym:'$' },
      DOGE:  { id:'dogecoin',      sym:'$' },
      MATIC: { id:'matic-network', sym:'$' },
    };
    TICKER_ITEMS = TICKER_ITEMS.map(t => {
      const m = map[t.lbl];
      if (!m || !data[m.id]) return t;
      const price = data[m.id].usd;
      const chg   = data[m.id].usd_24h_change ?? 0;
      const val   = price >= 1000 ? '$' + price.toLocaleString('en-US',{maximumFractionDigits:0})
                  : price >= 1    ? '$' + price.toFixed(2)
                  :                 '$' + price.toFixed(4);
      return { lbl: t.lbl, val, up: chg >= 0 };
    });
    renderTicker();
  } catch(_) {}
}

// ── HELPERS ───────────────────────────────────────────────────────────────────
const fmt$ = v => (v >= 0 ? '+$' : '-$') + Math.abs(v).toLocaleString('en-US',{minimumFractionDigits:2,maximumFractionDigits:2});
const fmtVol = v => { if(v>=1e6) return '$'+(v/1e6).toFixed(1)+'M'; if(v>=1e3) return '$'+(v/1e3).toFixed(1)+'K'; return '$'+Number(v).toFixed(0); };
const shortAddr = a => a ? a.slice(0,6)+'...'+a.slice(-4) : '—';

// CLOB auth headers (read-only GET requests only need POLY_API_KEY)
const clobHeaders = () => API_KEY
  ? { 'POLY_API_KEY': API_KEY }
  : {};

// ── FETCH: PORTFOLIO ──────────────────────────────────────────────────────────
async function loadPortfolio() {
  try {
    const [positions, rawVal] = await Promise.all([
      fetchWithProxy(`${DATA_API}/positions?user=${MY_ADDR}&limit=50`),
      fetchWithProxy(`${DATA_API}/value?user=${MY_ADDR}`),
    ]);

    // API returns array or object
    const value     = Array.isArray(rawVal) ? (rawVal[0] ?? {}) : rawVal;

    // value field is the portfolio value in this API
    const portVal = parseFloat(value.portfolioValue ?? value.portfolio_value ?? value.value ?? 0);
    const pnl     = parseFloat(value.pnl ?? value.realizedPnl ?? value.realized_pnl ?? 0);
    const roi     = parseFloat(value.pnlPercentage ?? value.roi ?? 0);

    document.getElementById('portfolio-val').textContent = fmtVol(portVal);
    const pnlEl = document.getElementById('pnl');
    pnlEl.textContent = fmt$(pnl);
    pnlEl.className = 'stat-val ' + (pnl >= 0 ? 'pos' : 'neg');
    const roiEl = document.getElementById('roi');
    roiEl.textContent = (roi >= 0 ? '+' : '') + Number(roi).toFixed(1) + '%';
    roiEl.className = 'stat-val ' + (roi >= 0 ? 'pos' : 'neg');

    const posArr = Array.isArray(positions) ? positions : [];
    document.getElementById('open-pos').textContent = posArr.filter(p=>!p.closed).length || posArr.length;
    document.getElementById('pos-meta').textContent = posArr.length + ' POSITIONS';

    renderPositions(posArr);
  } catch(e) {
    console.warn('Portfolio API:', e.message);
    document.getElementById('pos-meta').textContent = 'API UNAVAILABLE';
    document.getElementById('wallet-list').innerHTML = `
      <div class="we active" style="text-align:center;padding:18px 10px;color:var(--gdim);">
        <div style="font-size:11px;letter-spacing:1px;">UNABLE TO LOAD POSITIONS</div>
        <div style="font-size:9px;margin-top:6px;color:var(--gmuted);">${e.message}</div>
        <div style="margin-top:10px;">
          <button class="btn btn-y" onclick="window.open('https://polymarket.com/profile/${MY_ADDR}','_blank')">VIEW ON POLYMARKET</button>
        </div>
      </div>`;
  }
}

function renderPositions(positions) {
  if (!positions.length) {
    document.getElementById('wallet-list').innerHTML = `
      <div class="we active" style="text-align:center;padding:18px 10px;color:var(--gdim);">
        <div style="font-size:11px;letter-spacing:1px;">NO ACTIVE POSITIONS</div>
        <div style="font-size:9px;margin-top:6px;color:var(--gmuted);">Bot is scanning — trades will appear here</div>
        <div style="margin-top:10px;">
          <button class="btn btn-y" onclick="window.open('https://polymarket.com/profile/${MY_ADDR}','_blank')">VIEW PROFILE</button>
        </div>
      </div>`;
    // Read live balance from bot's status.json
    fetch('/status.json?t=' + Date.now())
      .then(r => r.ok ? r.json() : null)
      .then(st => {
        const bal = st ? parseFloat(st.balance ?? 0) : 0;
        document.getElementById('portfolio-val').textContent = bal > 0 ? fmtVol(bal) : '$0.00';
        if (st && st.day_pnl !== undefined) {
          const pnlEl = document.getElementById('pnl');
          pnlEl.textContent = (st.day_pnl >= 0 ? '+' : '') + st.day_pnl.toFixed(2) + '%';
          pnlEl.className = 'stat-val ' + (st.day_pnl >= 0 ? 'pos' : 'neg');
        }
      }).catch(() => {
        document.getElementById('portfolio-val').textContent = '$0.00';
      });
    document.getElementById('open-pos').textContent = '0';
    return;
  }
  const sorted = [...positions].sort((a,b) => {
    if (a.closed !== b.closed) return a.closed ? 1 : -1;
    return ((b.unrealizedPnl??0)-(a.unrealizedPnl??0));
  });
  document.getElementById('wallet-list').innerHTML = sorted.map((p, i) => {
    const title  = p.title ?? p.question ?? 'Unknown Market';
    const slug   = p.slug ?? '';
    const isYes  = (p.outcome==='Yes' || p.outcomeIndex===0 || p.outcome_index===0);
    const raw    = p.percentageOdds ?? p.currentPrice ?? p.price ?? 0;
    const price  = Math.round(raw <= 1 ? raw*100 : raw);
    const avgRaw = p.avgPrice ?? p.avg_price ?? 0;
    const avg    = (avgRaw <= 1 ? avgRaw*100 : avgRaw).toFixed(1);
    const size   = p.size ?? p.shares ?? 0;
    const pnl    = (p.unrealizedPnl??0) + (p.realizedPnl??0);
    return `
      <div class="we ${i===0?'active':''}" onclick="selectWallet(this)">
        <div class="we-market">${title}${p.closed?' <span style="color:var(--gdim);font-size:8px;">[CLOSED]</span>':''}</div>
        <div class="we-addr">${shortAddr(MY_ADDR)} · ${isYes?'YES':'NO'} · ${Number(size).toLocaleString()} shares</div>
        <div class="we-row">
          <span class="we-price">${price}¢</span>
          <span class="we-avg">avg ${avg}¢</span>
          <span class="we-vol" style="color:${pnl>=0?'var(--g)':'var(--red)'}">${fmt$(pnl)}</span>
        </div>
        <div class="we-btns">
          <button class="btn btn-y" onclick="window.open('${POLY_URL}/event/${slug}','_blank')">TRADE</button>
          <button class="btn btn-c" onclick="window.open('${POLY_URL}/profile/${MY_ADDR}','_blank')">PROFILE</button>
        </div>
      </div>`;
  }).join('');
}

function renderFallbackPositions() {
  const fb = [
    { market:"Fed cuts rates in 2025?", price:"67¢", avg:"54.2¢", pnl:"+$124.40", pos:"YES" },
    { market:"BTC above $120k by EOY?", price:"54¢", avg:"39.3¢", pnl:"+$88.20",  pos:"YES" },
    { market:"SpaceX Starship orbit?",  price:"70¢", avg:"84.7¢", pnl:"-$32.10",  pos:"NO"  },
    { market:"Apple Siri v2 ships?",    price:"42¢", avg:"65.9¢", pnl:"+$41.60",  pos:"YES" },
    { market:"US recession Q3 2025?",   price:"29¢", avg:"21.4¢", pnl:"+$19.80",  pos:"YES" },
  ];
  document.getElementById('wallet-list').innerHTML = fb.map((w,i) => `
    <div class="we ${i===0?'active':''}" onclick="selectWallet(this)">
      <div class="we-market">${w.market}</div>
      <div class="we-addr">${shortAddr(MY_ADDR)}</div>
      <div class="we-row">
        <span class="we-price">${w.price}</span>
        <span class="we-avg">avg ${w.avg}</span>
        <span class="we-vol" style="color:${w.pnl.startsWith('+')?'var(--g)':'var(--red)'}">${w.pnl}</span>
      </div>
      <div class="we-btns">
        <button class="btn btn-y">BUY ${w.pos}</button>
        <button class="btn btn-c" onclick="window.open('${POLY_URL}/profile/${MY_ADDR}','_blank')">PROFILE</button>
      </div>
    </div>`).join('');
  document.getElementById('portfolio-val').textContent = '$8.15';
  document.getElementById('pnl').textContent = '$0.00';
  document.getElementById('roi').textContent = '0.0%';
  document.getElementById('open-pos').textContent = '0';
}

// ── FETCH: CLOB ORDERS ────────────────────────────────────────────────────────
async function loadOrders() {
  if (!API_KEY) { renderFallbackLeaderboard(); return; }
  try {
    // Fetch open orders using the API key address
    const res = await fetch(
      `${CLOB_API}/orders?maker_address=${KEY_ADDR}&status=LIVE`,
      { headers: clobHeaders(), signal: AbortSignal.timeout(8000) }
    );
    if (!res.ok) throw new Error(`CLOB ${res.status}`);
    const data = await res.json();
    const orders = Array.isArray(data) ? data : (data.orders ?? data.data ?? []);
    if (orders.length) { renderOrders(orders); return; }
    // Fallback: try activity
    await loadActivity();
  } catch(e) {
    console.warn('CLOB orders:', e.message);
    await loadActivity();
  }
}

function renderOrders(orders) {
  const sorted = [...orders]
    .sort((a,b) => Number(b.original_size??b.size??0) - Number(a.original_size??a.size??0))
    .slice(0, 8);
  document.getElementById('leaderboard-list').innerHTML = sorted.map((o, i) => {
    const side    = o.side === 'BUY' ? 'BUY' : 'SELL';
    const outcome = o.outcome ?? (o.side === 'BUY' ? 'YES' : 'NO');
    const price   = o.price ? Math.round(parseFloat(o.price)*100)+'¢' : '—';
    const size    = Number(o.original_size ?? o.size ?? 0);
    const tokenId = o.token_id ?? o.asset_id ?? '';
    return `
      <div class="le">
        <span class="le-rank">#${i+1}</span>
        <span class="le-trader" style="color:${side==='BUY'?'var(--g)':'var(--red)'}">${side} ${outcome}</span>
        <span class="le-market">${tokenId ? tokenId.slice(0,12)+'...' : 'OPEN ORDER'}</span>
        <span class="le-profit">${price} · ${fmtVol(size)}</span>
      </div>`;
  }).join('');
}

// ── FETCH: ACTIVITY ───────────────────────────────────────────────────────────
async function loadActivity() {
  try {
    const data = await fetchWithProxy(`${DATA_API}/activity?user=${MY_ADDR}&limit=20`);
    const acts = Array.isArray(data) ? data : (data.history ?? data.trades ?? []);
    if (acts.length) { renderActivity(acts); return; }
  } catch(e) {
    console.warn('Activity API:', e.message);
  }
  renderFallbackLeaderboard();
}

function renderActivity(acts) {
  const sorted = [...acts]
    .filter(a => (a.amount??a.size??0) > 0)
    .sort((a,b) => (b.amount??b.size??0)-(a.amount??a.size??0))
    .slice(0, 8);
  document.getElementById('leaderboard-list').innerHTML = sorted.map((a,i) => {
    const market = a.title ?? a.question ?? a.market ?? 'Unknown';
    const side   = (a.side ?? a.outcome ?? 'BUY').toString().toUpperCase();
    const amt    = a.amount ?? a.size ?? 0;
    const slug   = a.slug ?? '';
    return `
      <div class="le">
        <span class="le-rank">#${i+1}</span>
        <span class="le-trader" style="color:${side.includes('YES')||side==='BUY'?'var(--g)':'var(--red)'}">${side}</span>
        <span class="le-market" style="cursor:pointer" onclick="window.open('${POLY_URL}/event/${slug}','_blank')">${market}</span>
        <span class="le-profit">${fmtVol(amt)}</span>
      </div>`;
  }).join('');
}

// ── FETCH: LIVE MARKETS ───────────────────────────────────────────────────────
async function fetchWithProxy(url) {
  // Try local proxy first (server.py — adds polymarket.com Origin header)
  try {
    const local = `/proxy?url=${encodeURIComponent(url)}`;
    const r = await fetch(local, { signal: AbortSignal.timeout(8000) });
    if (r.ok) return r.json();
  } catch(_) {}
  // Fallback: direct fetch (may work if CORS not enforced)
  try {
    const r = await fetch(url, { signal: AbortSignal.timeout(7000) });
    if (r.ok) return r.json();
  } catch(_) {}
  throw new Error('all fetch methods failed');
}

async function loadMarkets() {
  try {
    document.getElementById('market-list').innerHTML =
      '<tr><td colspan="8" style="padding:10px;color:#0f0;font-size:9px;">LOADING...</td></tr>';
    const raw = await fetchWithProxy(
      `${GAMMA_API}/markets?active=true&limit=20&order=volume24hr&ascending=false`
    );
    const markets = Array.isArray(raw) ? raw : (raw.markets ?? []);
    if (!markets.length) throw new Error('empty response');
    document.getElementById('mkt-meta').textContent = `ONLINE · ${markets.length} LIVE · VOLUME ▼`;
    renderLiveMarkets(markets);
    setTimeout(() => fetchAllSparklines(markets), 300);
  } catch(e) {
    document.getElementById('mkt-meta').textContent = 'ERROR: ' + e.message;
    document.getElementById('market-list').innerHTML =
      `<tr><td colspan="8" style="padding:10px;color:#f33;font-size:9px;">ERR: ${e.message}</td></tr>`;
  }
}

// ── SPARKLINES ────────────────────────────────────────────────────────────────
function drawSparkline(canvas, points, up) {
  if (!canvas || !points || points.length < 2) return;
  const ctx = canvas.getContext('2d');
  const W = canvas.width, H = canvas.height;
  ctx.clearRect(0, 0, W, H);
  const min = Math.min(...points), max = Math.max(...points);
  const range = max - min || 0.01;
  const xs = points.map((_, i) => (i / (points.length - 1)) * W);
  const ys = points.map(p => H - ((p - min) / range) * (H - 2) - 1);
  ctx.beginPath();
  xs.forEach((x, i) => i === 0 ? ctx.moveTo(x, ys[i]) : ctx.lineTo(x, ys[i]));
  ctx.strokeStyle = up ? '#00ff41' : '#ff3b3b';
  ctx.lineWidth = 1.5;
  ctx.shadowColor = up ? '#00ff41' : '#ff3b3b';
  ctx.shadowBlur = 3;
  ctx.stroke();
}

async function fetchAllSparklines(markets) {
  for (const m of markets.slice(0, 12)) {
    const condId = m.conditionId ?? m.condition_id ?? '';
    let tokenId = '';
    try {
      const toks = typeof m.clobTokenIds === 'string' ? JSON.parse(m.clobTokenIds) : (m.clobTokenIds ?? []);
      tokenId = toks[0] ?? '';
    } catch(_) {}
    if (!tokenId) continue;
    const canvas = document.getElementById(`spark-${condId}`);
    if (!canvas) continue;
    try {
      const data = await fetchWithProxy(
        `${CLOB_API}/prices-history?market=${tokenId}&interval=1d&fidelity=60`
      );
      const hist = data.history ?? [];
      if (hist.length < 3) continue;
      const pts = hist.slice(-40).map(h => parseFloat(h.p ?? h.price ?? 0.5));
      const first = pts[0], last = pts[pts.length - 1];
      drawSparkline(canvas, pts, last >= first);
    } catch(_) {}
    await new Promise(r => setTimeout(r, 120)); // stagger requests
  }
}

function renderLiveMarkets(markets) {
  if (!markets.length) { renderOfflineMarkets('empty response'); return; }
  document.getElementById('market-list').innerHTML = markets.map(m => {
    let yesV=50, noV=50;
    try {
      const p = typeof m.outcomePrices==='string' ? JSON.parse(m.outcomePrices) : (m.outcomePrices??[]);
      yesV = Math.round(parseFloat(p[0]??0.5)*100);
      noV  = Math.round(parseFloat(p[1]??0.5)*100);
    } catch(_){}
    const question = m.question ?? m.title ?? 'Unknown';
    const cat      = (m.tags?.[0]??m.category??'MARKET').toString().toUpperCase().slice(0,10);
    const vol      = fmtVol(m.volume??0);
    const vol24    = m.volume24hr!=null ? fmtVol(m.volume24hr) : '—';
    const slug     = m.slug ?? m.conditionId ?? '';
    const condId   = m.conditionId ?? m.condition_id ?? slug;
    return `
      <tr>
        <td>
          <div class="m-name">${question}</div>
          <div class="m-cat">${cat}</div>
        </td>
        <td><span class="pyes" data-base="${yesV}">${yesV}¢</span></td>
        <td><span class="pno">${noV}¢</span></td>
        <td><span class="vol-cell">${vol}</span></td>
        <td><span class="ch-pos">${vol24}</span></td>
        <td><canvas id="spark-${condId}" width="70" height="24" style="display:block;"></canvas></td>
        <td>
          <div class="win-wrap">
            <div class="win-track"><div class="win-fill" style="width:${yesV}%"></div></div>
            <span class="win-pct">${yesV}%</span>
          </div>
        </td>
        <td><button class="btn btn-y" style="font-size:8px;padding:2px 6px;"
          onclick="window.open('${POLY_URL}/event/${slug}','_blank')">TRADE</button></td>
      </tr>`;
  }).join('');
}

function renderOfflineMarkets(reason) {
  document.getElementById('market-list').innerHTML = `
    <tr><td colspan="7" style="padding:28px 10px;text-align:center;color:var(--gdim);">
      <div style="font-size:11px;letter-spacing:1px;">MARKETS UNAVAILABLE</div>
      <div style="font-size:9px;margin-top:6px;color:var(--gmuted);">${reason ?? 'API unreachable'}</div>
      <div style="margin-top:12px;">
        <button class="btn btn-y" onclick="loadMarkets()">RETRY</button>
        &nbsp;
        <button class="btn btn-c" onclick="window.open('https://polymarket.com','_blank')">OPEN POLYMARKET</button>
      </div>
    </td></tr>`;
}

// ── STATIC RENDERS ────────────────────────────────────────────────────────────
function selectWallet(el) {
  document.querySelectorAll('.we').forEach(e => e.classList.remove('active'));
  el.classList.add('active');
}

async function loadCopytrade() {
  const tbody = document.getElementById('copytrade-list');
  tbody.innerHTML = `<tr><td colspan="6" style="padding:8px 10px;color:var(--gmuted);font-size:10px;"><span class="blink">▋</span> FETCHING WALLETS...</td></tr>`;

  const rows = await Promise.all(WATCHED_WALLETS.map(async (w) => {
    let portfolio = w.portfolio;
    let mkts      = w.mkts;
    let pnl       = 0;
    try {
      const [vRes, pRes] = await Promise.all([
        fetch(`${DATA_API}/value?user=${w.addr}`, { signal: AbortSignal.timeout(6000) }),
        fetch(`${DATA_API}/positions?user=${w.addr}&limit=50`, { signal: AbortSignal.timeout(6000) }),
      ]);
      if (vRes.ok) {
        let v = await vRes.json();
        if (Array.isArray(v)) v = v[0] ?? {};
        portfolio = parseFloat(v.portfolioValue ?? v.value ?? portfolio ?? 0);
        pnl       = parseFloat(v.pnl ?? v.realizedPnl ?? 0);
      }
      if (pRes.ok) {
        const pos = await pRes.json();
        mkts = Array.isArray(pos) ? pos.length : mkts;
      }
    } catch(_) {}

    const short = w.addr.slice(0,6) + '...' + w.addr.slice(-4);
    const portStr = portfolio >= 1000
      ? '$' + (portfolio/1000).toFixed(1) + 'K'
      : '$' + portfolio.toFixed(2);
    const pnlStr  = (pnl >= 0 ? '+$' : '-$') + Math.abs(pnl).toFixed(2);
    const pnlCol  = pnl >= 0 ? 'var(--g)' : 'var(--red)';

    return `
      <tr style="cursor:pointer" onclick="window.open('${POLY_URL}/profile/${w.addr}','_blank')">
        <td class="t-name">${short}</td>
        <td style="color:var(--gdim)">${mkts}</td>
        <td style="color:var(--cyan)">${portStr}</td>
        <td style="color:${pnlCol}">${pnlStr}</td>
        <td><button class="btn btn-c" style="font-size:8px;padding:2px 6px;"
          onclick="event.stopPropagation();window.open('${POLY_URL}/profile/${w.addr}','_blank')">COPY</button></td>
      </tr>`;
  }));

  tbody.innerHTML = rows.join('');
}

function renderCopytrade() { loadCopytrade(); }

function renderTicker() {
  const html = [...TICKER_ITEMS, ...TICKER_ITEMS].map(t => `
    <span class="ti">
      <span class="lbl">${t.lbl}: </span>
      <span class="${t.up?'up':'dn'}">${t.val}</span>
    </span>`).join('  ·  ');
  document.getElementById('ticker').innerHTML = html;
}

// ── LIVE UPDATES ──────────────────────────────────────────────────────────────
function updateClock() {
  document.getElementById('clock').textContent =
    new Date().toISOString().slice(11,19) + ' UTC';
}

function flickerPrices() {
  document.querySelectorAll('.pyes').forEach(el => {
    const base = parseInt(el.dataset.base, 10);
    if (!base) return;
    const v = Math.max(1, Math.min(99, Math.round(base + (Math.random()-0.5)*2)));
    el.textContent = v + '¢';
  });
}

let progVal = 10;
function tickProg() {
  progVal = (progVal + 0.4) % 100;
  document.getElementById('prog').style.width = progVal + '%';
  const secs = Math.round(progVal/100*60);
  document.getElementById('stream-time').textContent =
    '0:' + String(secs).padStart(2,'0') + ' / 1:00';
}

// ── BOT ALERTS ────────────────────────────────────────────────────────────────
async function loadAlerts() {
  try {
    const res = await fetch('alerts.json?t=' + Date.now(), { cache: 'no-store' });
    if (!res.ok) return;
    const alerts = await res.json();
    if (!Array.isArray(alerts) || !alerts.length) return;
    const colors = { INFO: 'var(--g)', WARN: 'var(--yellow)', ERROR: 'var(--red)', HALT: 'var(--red)' };
    const html = alerts.slice(0, 15).map(a => `
      <div style="display:flex;gap:8px;padding:3px 10px;border-bottom:1px solid rgba(0,255,65,.04);align-items:baseline;">
        <span style="font-size:9px;color:var(--gmuted);white-space:nowrap;">${a.time}</span>
        <span style="font-size:9px;color:${colors[a.level]||'var(--g)'};font-weight:bold;">${a.level}</span>
        <span style="font-size:10px;color:var(--g);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:150px;">${a.msg}</span>
      </div>`).join('');
    const el1 = document.getElementById('alert-list');
    const el2 = document.getElementById('alert-list2');
    const m1  = document.getElementById('alert-meta');
    const m2  = document.getElementById('alert-meta2');
    if (el1) el1.innerHTML = html;
    if (el2) el2.innerHTML = html;
    if (m1) m1.textContent = `${alerts.length} ALERTS`;
    if (m2) m2.textContent = `${alerts.length} ALERTS`;
  } catch(_) {}
}

// ── EQUITY CHART ──────────────────────────────────────────────────────────────
function drawEquityChart(history) {
  const canvas = document.getElementById('equity-canvas');
  if (!canvas || history.length < 2) return;
  const W = canvas.offsetWidth || 200;
  const H = canvas.offsetHeight || 80;
  canvas.width  = W;
  canvas.height = H;
  const ctx = canvas.getContext('2d');
  ctx.clearRect(0, 0, W, H);

  const vals  = history.map(p => p[1]);
  const min   = Math.min(...vals);
  const max   = Math.max(...vals);
  const range = (max - min) || 0.01;
  const first = vals[0];
  const last  = vals[vals.length - 1];
  const up    = last >= first;
  const color = up ? '#00ff41' : '#ff3b3b';

  const toX = i => (i / (vals.length - 1)) * W;
  const toY = v => H - 4 - ((v - min) / range) * (H - 8);

  // Fill area under curve
  ctx.beginPath();
  ctx.moveTo(toX(0), H);
  vals.forEach((v, i) => ctx.lineTo(toX(i), toY(v)));
  ctx.lineTo(toX(vals.length - 1), H);
  ctx.closePath();
  const grad = ctx.createLinearGradient(0, 0, 0, H);
  grad.addColorStop(0, up ? 'rgba(0,255,65,.25)' : 'rgba(255,59,59,.25)');
  grad.addColorStop(1, 'rgba(0,0,0,0)');
  ctx.fillStyle = grad;
  ctx.fill();

  // Draw line
  ctx.beginPath();
  vals.forEach((v, i) => i === 0 ? ctx.moveTo(toX(i), toY(v)) : ctx.lineTo(toX(i), toY(v)));
  ctx.strokeStyle = color;
  ctx.lineWidth = 1.5;
  ctx.shadowColor = color;
  ctx.shadowBlur = 4;
  ctx.stroke();

  // Draw last price dot
  ctx.beginPath();
  ctx.arc(toX(vals.length-1), toY(last), 3, 0, Math.PI*2);
  ctx.fillStyle = color;
  ctx.shadowBlur = 8;
  ctx.fill();
}

// ── STATUS / COMMAND CENTER ───────────────────────────────────────────────────
async function loadStatus() {
  try {
    const st = await fetch('/status.json?t=' + Date.now()).then(r => r.ok ? r.json() : null);
    if (!st) return;
    const bal = parseFloat(st.balance ?? 0);
    const sessionPnl = parseFloat(st.session_pnl ?? 0);
    const dayPct = parseFloat(st.day_pnl ?? 0);

    // Header stats
    document.getElementById('portfolio-val').textContent = fmtVol(bal);
    const pnlEl = document.getElementById('pnl');
    pnlEl.textContent = (dayPct >= 0 ? '+' : '') + dayPct.toFixed(2) + '%';
    pnlEl.className = 'stat-val ' + (dayPct >= 0 ? 'pos' : 'neg');

    // Command center big display
    const cmdBal = document.getElementById('cmd-balance');
    cmdBal.textContent = fmtVol(bal);
    cmdBal.className = 'cmd-big ' + (bal > 0 ? 'neu' : 'neg');

    const cmdSession = document.getElementById('cmd-session');
    cmdSession.textContent = (sessionPnl >= 0 ? '+' : '') + '$' + Math.abs(sessionPnl).toFixed(2);
    cmdSession.className = 'cmd-sub-val ' + (sessionPnl >= 0 ? 'pos' : 'neg');

    const cmdDay = document.getElementById('cmd-daypct');
    cmdDay.textContent = (dayPct >= 0 ? '+' : '') + dayPct.toFixed(2) + '%';
    cmdDay.className = 'cmd-sub-val ' + (dayPct >= 0 ? 'pos' : 'neg');

    document.getElementById('cmd-trades').textContent = st.trades ?? 0;
    document.getElementById('cmd-meta').textContent = st.updated ?? 'SESSION';

    // Bot stats panel
    const wallet = st.wallet ?? '';
    document.getElementById('bs-wallet').textContent = wallet ? wallet.slice(0,6)+'…'+wallet.slice(-4) : '—';
    document.getElementById('bs-mode').textContent = st.mode ?? '—';
    document.getElementById('bs-pos').textContent = st.positions ?? 0;
    document.getElementById('bs-updated').textContent = st.updated ?? '—';
    const haltEl = document.getElementById('bs-halted');
    haltEl.textContent = st.halted ? 'YES — ' + (st.halt_reason||'') : 'NO';
    haltEl.style.color = st.halted ? 'var(--red)' : 'var(--g)';

    // Equity chart
    if (Array.isArray(st.equity_history) && st.equity_history.length > 1) {
      drawEquityChart(st.equity_history);
    }
  } catch(_) {}
}

Promise.all([ loadPortfolio(), loadMarkets(), loadOrders(), loadTicker(), loadAlerts(), loadStatus() ]);

// Refresh schedule
setInterval(loadPortfolio, 60_000);
setInterval(loadMarkets,   30_000);
setInterval(loadOrders,    60_000);
setInterval(loadTicker,   120_000);  // crypto prices every 2 min
setInterval(loadAlerts,    5_000);   // bot alerts every 5s
setInterval(loadStatus,   30_000);   // bot balance every 30s

setInterval(updateClock,   1000);
setInterval(flickerPrices, 2000);
setInterval(tickProg,       400);
