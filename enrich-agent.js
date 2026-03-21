#!/usr/bin/env node
/**
 * Propyte Enrichment Agent (Node.js)
 *
 * Enriquece desarrollos inmobiliarios en Supabase buscando datos en Internet.
 * Complementa agente_enriquecimiento.py (que trabaja desde lanzamientos.csv).
 * Este agente trabaja directamente contra la BD de Supabase.
 *
 * Estrategias de enriquecimiento:
 * 1. Geocoding via Nominatim (lat/lng) — gratis, 1 req/sec
 * 2. Clasificación por nombre (development_type) — regex local
 * 3. Precios desde inventario_unidades (fuzzy name match)
 * 4. Scraping de source_url (extrae amenidades, tipo, precio)
 * 5. Web search via DuckDuckGo HTML
 * 6. Developer enrichment (website, descripción, ciudad)
 *
 * Requisitos (env vars en .env):
 *   PROPYTE_SUPABASE_URL=https://yjbrynsykkycozeybykj.supabase.co
 *   PROPYTE_SUPABASE_SERVICE_KEY=eyJ... (service_role key para reads)
 *   SUPABASE_MGMT_TOKEN=sbp_... (Management API token para writes)
 *   SUPABASE_PROJECT_REF=yjbrynsykkycozeybykj
 *
 * Nota: REST API PATCH no funciona porque public.developments es una VIEW
 * apuntando a real_estate_hub. Usamos Management API SQL para writes.
 *
 * Usage:
 *   node enrich-agent.js                    # Run all strategies
 *   node enrich-agent.js --geocode          # Only geocoding
 *   node enrich-agent.js --classify         # Classify dev type from name
 *   node enrich-agent.js --prices           # Calculate prices from units
 *   node enrich-agent.js --scrape-source    # Only scrape source URLs
 *   node enrich-agent.js --search           # Only web search enrichment
 *   node enrich-agent.js --developers       # Only developer enrichment
 *   node enrich-agent.js --limit 50         # Process max 50 items
 *   node enrich-agent.js --dry-run          # Don't write to DB, just log
 */

// Load .env file
const fs = require('fs');
const path = require('path');
const envPath = path.join(__dirname, '.env');
if (fs.existsSync(envPath)) {
  fs.readFileSync(envPath, 'utf8').split('\n').forEach(line => {
    line = line.trim();
    if (line && !line.startsWith('#') && line.includes('=')) {
      const [key, ...val] = line.split('=');
      if (!process.env[key.trim()]) process.env[key.trim()] = val.join('=').trim();
    }
  });
}

const SUPABASE_URL = process.env.PROPYTE_SUPABASE_URL;
const SERVICE_KEY = process.env.PROPYTE_SUPABASE_SERVICE_KEY;
const MGMT_TOKEN = process.env.SUPABASE_MGMT_TOKEN;
const PROJECT_REF = process.env.SUPABASE_PROJECT_REF;

if (!SUPABASE_URL || !SERVICE_KEY || !MGMT_TOKEN || !PROJECT_REF) {
  console.error('Missing env vars. Required: PROPYTE_SUPABASE_URL, PROPYTE_SUPABASE_SERVICE_KEY, SUPABASE_MGMT_TOKEN, SUPABASE_PROJECT_REF');
  console.error('Set them in .env or as environment variables.');
  process.exit(1);
}

const HEADERS = {
  'apikey': SERVICE_KEY,
  'Authorization': `Bearer ${SERVICE_KEY}`,
  'Content-Type': 'application/json',
  'Prefer': 'return=minimal',
};

// ============================================================
// UTILITIES
// ============================================================

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

const log = {
  info: (msg) => console.log(`[INFO] ${new Date().toISOString().slice(11,19)} ${msg}`),
  ok: (msg) => console.log(`[OK]   ${new Date().toISOString().slice(11,19)} ${msg}`),
  warn: (msg) => console.log(`[WARN] ${new Date().toISOString().slice(11,19)} ${msg}`),
  err: (msg) => console.error(`[ERR]  ${new Date().toISOString().slice(11,19)} ${msg}`),
};

const stats = {
  geocoded: 0, scraped: 0, searched: 0, developers_enriched: 0,
  updates: 0, errors: 0, skipped: 0,
};

// ============================================================
// SUPABASE CLIENT
// ============================================================

async function supabaseGet(table, params = '') {
  const url = `${SUPABASE_URL}/rest/v1/${table}?${params}`;
  const res = await fetch(url, { headers: { ...HEADERS, 'Prefer': 'count=exact' } });
  const count = res.headers.get('content-range')?.split('/')[1] || '?';
  const data = await res.json();
  return { data, count: parseInt(count) };
}

async function supabaseSQL(query) {
  const url = `https://api.supabase.com/v1/projects/${PROJECT_REF}/database/query`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Authorization': `Bearer ${MGMT_TOKEN}`, 'Content-Type': 'application/json' },
    body: JSON.stringify({ query }),
  });
  if (!res.ok) {
    const err = await res.text();
    throw new Error(`SQL failed: ${res.status} ${err}`);
  }
  return res.json();
}

function buildSetClause(data) {
  return Object.entries(data).map(([k, v]) => {
    if (v === null) return `"${k}" = NULL`;
    if (typeof v === 'number') return `"${k}" = ${v}`;
    if (typeof v === 'boolean') return `"${k}" = ${v}`;
    if (Array.isArray(v)) {
      if (v.length === 0) return `"${k}" = '{}'::text[]`;
      return `"${k}" = ARRAY[${v.map(x => `'${String(x).replace(/'/g, "''")}'`).join(',')}]::text[]`;
    }
    return `"${k}" = '${String(v).replace(/'/g, "''")}'`;
  }).join(', ');
}

async function supabaseUpdate(table, id, updates) {
  const query = `UPDATE public.${table} SET ${buildSetClause(updates)}, updated_at = NOW() WHERE id = '${id}'`;
  return supabaseSQL(query);
}

async function supabaseBatchUpdate(table, updates) {
  if (updates.length === 0) return;
  const chunkSize = 50;
  for (let i = 0; i < updates.length; i += chunkSize) {
    const chunk = updates.slice(i, i + chunkSize);
    const statements = chunk.map(({ id, data }) =>
      `UPDATE public.${table} SET ${buildSetClause(data)}, updated_at = NOW() WHERE id = '${id}'`
    ).join(';\n');
    await supabaseSQL(statements);
    if (i + chunkSize < updates.length) await sleep(500);
  }
}

// ============================================================
// STRATEGY 1: GEOCODING (Nominatim)
// ============================================================

async function geocode(city, state, zone) {
  const queries = [];
  if (zone && city) queries.push(`${zone}, ${city}, ${state}, México`);
  if (city) queries.push(`${city}, ${state}, México`);

  for (const q of queries) {
    try {
      const url = `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(q)}&format=json&limit=1&countrycodes=mx`;
      const res = await fetch(url, {
        headers: { 'User-Agent': 'PropyteEnrichmentAgent/1.0 (contacto@propyte.com)' }
      });
      const data = await res.json();
      if (data.length > 0) {
        return { lat: parseFloat(data[0].lat), lng: parseFloat(data[0].lon) };
      }
    } catch (e) {
      log.warn(`Geocode error for "${q}": ${e.message}`);
    }
    await sleep(1100);
  }
  return null;
}

async function runGeocoding(developments, dryRun) {
  log.info(`=== GEOCODING: ${developments.length} developments sin coordenadas ===`);
  const locationCache = new Map();
  let pendingUpdates = [];

  for (let i = 0; i < developments.length; i++) {
    const dev = developments[i];
    const cacheKey = `${dev.city}|${dev.zone || ''}`;

    let coords;
    if (locationCache.has(cacheKey)) {
      coords = locationCache.get(cacheKey);
    } else {
      coords = await geocode(dev.city, dev.state, dev.zone);
      locationCache.set(cacheKey, coords);
      await sleep(1100);
    }

    if (coords) {
      pendingUpdates.push({ id: dev.id, data: { lat: coords.lat, lng: coords.lng } });
      stats.geocoded++;

      if (pendingUpdates.length >= 50) {
        if (!dryRun) {
          try {
            await supabaseBatchUpdate('developments', pendingUpdates);
            stats.updates += pendingUpdates.length;
          } catch (e) {
            log.err(`Batch update failed: ${e.message}`);
            stats.errors += pendingUpdates.length;
          }
        }
        pendingUpdates = [];
      }

      if (stats.geocoded % 25 === 0 || i === developments.length - 1) {
        log.ok(`Geocoded ${stats.geocoded}/${developments.length} (cache: ${locationCache.size} locations)`);
      }
    } else {
      stats.skipped++;
      log.warn(`No coords for: ${dev.name} (${dev.city}, ${dev.zone})`);
    }
  }

  if (pendingUpdates.length > 0 && !dryRun) {
    try {
      await supabaseBatchUpdate('developments', pendingUpdates);
      stats.updates += pendingUpdates.length;
    } catch (e) {
      log.err(`Final batch failed: ${e.message}`);
      stats.errors += pendingUpdates.length;
    }
  }

  log.ok(`Geocoding done: ${stats.geocoded} enriched, ${stats.skipped} skipped, ${locationCache.size} unique locations`);
}

// ============================================================
// STRATEGY 2: SCRAPE SOURCE URLs
// ============================================================

async function scrapeSourceUrl(url) {
  try {
    const res = await fetch(url, {
      headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' },
      signal: AbortSignal.timeout(15000),
    });
    if (!res.ok) return null;
    const html = await res.text();
    const extracted = {};

    const priceMatch = html.match(/(?:precio|price)[^<]*?[\$USD MXN]*\s*([\d,]+(?:\.\d{2})?)/i);
    if (priceMatch) {
      const price = parseFloat(priceMatch[1].replace(/,/g, ''));
      if (price > 100000) extracted.price_min_mxn = price;
    }

    const typePatterns = [
      { pattern: /(?:departamento|condominio|apartment|condo)/i, type: 'residencial_vertical' },
      { pattern: /(?:casa|house|home|villa)/i, type: 'residencial_horizontal' },
      { pattern: /(?:lote|terreno|land|lot)/i, type: 'lotes' },
      { pattern: /(?:comercial|commercial|oficina|office)/i, type: 'comercial' },
      { pattern: /(?:mixto|mixed.?use)/i, type: 'mixto' },
      { pattern: /(?:hotel|resort|hospitality)/i, type: 'hotelero' },
    ];
    for (const { pattern, type } of typePatterns) {
      if (pattern.test(html)) { extracted.development_type = type; break; }
    }

    const amenityPatterns = [
      { pattern: /(?:alberca|pool|piscina)/i, amenity: 'pool' },
      { pattern: /(?:gym|gimnasio)/i, amenity: 'gym' },
      { pattern: /(?:rooftop|azotea)/i, amenity: 'rooftop' },
      { pattern: /(?:seguridad|security)\s*24/i, amenity: 'security_24h' },
      { pattern: /(?:yoga|meditation)/i, amenity: 'yoga' },
      { pattern: /(?:spa|wellness)/i, amenity: 'spa' },
      { pattern: /(?:coworking|co-working)/i, amenity: 'coworking' },
      { pattern: /(?:restaurante|restaurant)/i, amenity: 'restaurant' },
      { pattern: /(?:elevador|elevator)/i, amenity: 'elevator' },
      { pattern: /(?:estacionamiento|parking)/i, amenity: 'parking' },
      { pattern: /(?:pet.?friendly|mascotas)/i, amenity: 'pet_friendly' },
    ];
    const foundAmenities = amenityPatterns.filter(({ pattern }) => pattern.test(html)).map(({ amenity }) => amenity);
    if (foundAmenities.length > 0) extracted.amenities = foundAmenities;

    return Object.keys(extracted).length > 0 ? extracted : null;
  } catch { return null; }
}

async function runSourceScraping(developments, dryRun) {
  const withSource = developments.filter(d => d.source_url);
  log.info(`=== SOURCE SCRAPING: ${withSource.length} developments con source_url ===`);

  for (let i = 0; i < withSource.length; i++) {
    const dev = withSource[i];
    const data = await scrapeSourceUrl(dev.source_url);
    if (!data) { stats.skipped++; continue; }

    const updates = {};
    if (data.price_min_mxn && !dev.price_min_mxn) updates.price_min_mxn = data.price_min_mxn;
    if (data.development_type && !dev.development_type) updates.development_type = data.development_type;
    if (data.amenities && (!dev.amenities || dev.amenities.length === 0)) updates.amenities = data.amenities;

    if (Object.keys(updates).length > 0) {
      if (!dryRun) {
        try { await supabaseUpdate('developments', dev.id, updates); stats.updates++; }
        catch (e) { log.err(`Update failed: ${dev.name}: ${e.message}`); stats.errors++; continue; }
      }
      stats.scraped++;
      log.ok(`[${i+1}/${withSource.length}] Scraped ${dev.name}: ${Object.keys(updates).join(', ')}`);
    }
    await sleep(2000);
  }
  log.ok(`Source scraping done: ${stats.scraped} enriched`);
}

// ============================================================
// STRATEGY 3: WEB SEARCH (DuckDuckGo HTML)
// ============================================================

async function webSearch(query) {
  try {
    const url = `https://html.duckduckgo.com/html/?q=${encodeURIComponent(query)}`;
    const res = await fetch(url, {
      headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' },
      signal: AbortSignal.timeout(15000),
    });
    const html = await res.text();
    const results = [];
    const resultBlocks = html.match(/<a[^>]+class="result__a"[^>]*href="([^"]+)"[^>]*>([^<]+)<\/a>/gi) || [];
    const snippetBlocks = html.match(/<a[^>]+class="result__snippet"[^>]*>([^<]+(?:<[^>]+>[^<]*)*)<\/a>/gi) || [];

    for (let i = 0; i < Math.min(resultBlocks.length, 5); i++) {
      const urlMatch = resultBlocks[i]?.match(/href="([^"]+)"/);
      const titleMatch = resultBlocks[i]?.match(/>([^<]+)</);
      const snippet = snippetBlocks[i]?.replace(/<[^>]+>/g, '') || '';
      if (urlMatch) results.push({ url: urlMatch[1], title: titleMatch?.[1] || '', snippet });
    }
    return results;
  } catch { return []; }
}

async function runWebSearch(developments, dryRun) {
  const toSearch = developments.filter(d => !d.price_min_mxn || !d.development_type);
  log.info(`=== WEB SEARCH: ${toSearch.length} developments necesitan datos ===`);

  for (let i = 0; i < toSearch.length; i++) {
    const dev = toSearch[i];
    const query = `"${dev.name}" ${dev.city} ${dev.state} México inmobiliaria precio`;
    const results = await webSearch(query);
    if (results.length === 0) { stats.skipped++; continue; }

    const updates = {};
    for (const result of results) {
      const text = `${result.title} ${result.snippet}`.toLowerCase();
      if (!dev.price_min_mxn && !updates.price_min_mxn) {
        const mxnMatch = text.match(/\$\s*([\d,]+(?:\.\d{2})?)\s*(?:mxn|pesos)/);
        if (mxnMatch) { const p = parseFloat(mxnMatch[1].replace(/,/g, '')); if (p > 500000 && p < 100000000) updates.price_min_mxn = p; }
        const usdMatch = text.match(/(?:usd|us\$|dollars?)\s*\$?\s*([\d,]+(?:\.\d{2})?)/);
        if (usdMatch && !updates.price_min_mxn) { const u = parseFloat(usdMatch[1].replace(/,/g, '')); if (u > 50000 && u < 10000000) updates.price_min_mxn = Math.round(u * 17.5); }
      }
      if (!dev.development_type && !updates.development_type) {
        if (/(?:departamento|condominio|apartment|condo)/i.test(text)) updates.development_type = 'residencial_vertical';
        else if (/(?:casa|house|villa)/i.test(text)) updates.development_type = 'residencial_horizontal';
        else if (/(?:lote|terreno|land)/i.test(text)) updates.development_type = 'lotes';
      }
    }

    if (Object.keys(updates).length > 0) {
      if (!dryRun) {
        try { await supabaseUpdate('developments', dev.id, updates); stats.updates++; }
        catch (e) { log.err(`Update failed: ${dev.name}: ${e.message}`); stats.errors++; continue; }
      }
      stats.searched++;
      log.ok(`[${i+1}/${toSearch.length}] Found data for ${dev.name}: ${Object.keys(updates).join(', ')}`);
    } else { stats.skipped++; }
    await sleep(3000);
  }
  log.ok(`Web search done: ${stats.searched} enriched`);
}

// ============================================================
// STRATEGY 4: NAME-BASED CLASSIFICATION
// ============================================================

function classifyByName(name, propertyTypes) {
  const n = name.toLowerCase();
  const pt = (propertyTypes || []).join(' ').toLowerCase();
  const combined = `${n} ${pt}`;

  if (/\blote?\b|\bterreno\b|\bland\b|\blot\b/i.test(combined)) return 'lotes';
  if (/\bpenthouse\b|\bph\b/i.test(n)) return 'residencial_vertical';
  if (/\bstudio\b|\bestudio\b|\bloft\b/i.test(n)) return 'residencial_vertical';
  if (/\bdepa(?:rtamento)?s?\b|\bcondo\b|\bapartment\b|\b\d+\s*(?:bed|br|rec)\b/i.test(combined)) return 'residencial_vertical';
  if (/\btower\b|\btorre\b|\bresidencia[ls]?\b|\bboutique\b/i.test(n)) return 'residencial_vertical';
  if (/\bcasa\b|\bhouse\b|\bvilla\b|\btown\s*house\b/i.test(combined)) return 'residencial_horizontal';
  if (/\bhotel\b|\bresort\b|\bhospitality\b|\bboutique hotel\b/i.test(n)) return 'hotelero';
  if (/\bcomercial\b|\bcommercial\b|\boficina\b|\boffice\b|\blocal\b|\bplaza\b/i.test(n)) return 'comercial';
  if (/\bmixto\b|\bmixed\b/i.test(n)) return 'mixto';

  if (pt.includes('departamento') || pt.includes('condominio')) return 'residencial_vertical';
  if (pt.includes('casa') || pt.includes('villa')) return 'residencial_horizontal';
  if (pt.includes('lote') || pt.includes('terreno')) return 'lotes';

  if (/\d{1,3}\s*(?:m2|m²|sqft)/i.test(n)) return 'residencial_vertical';
  return null;
}

async function runClassification(developments, dryRun) {
  log.info(`=== CLASSIFICATION: ${developments.length} developments sin tipo ===`);
  const batchUpdates = [];

  for (const dev of developments) {
    const devType = classifyByName(dev.name, dev.property_types);
    if (devType) batchUpdates.push({ id: dev.id, data: { development_type: devType } });
  }

  if (batchUpdates.length > 0 && !dryRun) {
    try { await supabaseBatchUpdate('developments', batchUpdates); stats.updates += batchUpdates.length; }
    catch (e) { log.err(`Batch classify failed: ${e.message}`); stats.errors += batchUpdates.length; }
  }

  log.ok(`Classification done: ${batchUpdates.length}/${developments.length} classified`);
  return batchUpdates.length;
}

// ============================================================
// STRATEGY 5: PRICES FROM INVENTARIO + UNITS
// ============================================================

async function runPriceCalculation(dryRun, limit) {
  const { data: inventario } = await supabaseGet('inventario_unidades',
    `select=proyecto,precio,moneda&precio=not.is.null&limit=5000`);

  const priceMap = new Map();
  for (const inv of inventario) {
    const key = inv.proyecto.toUpperCase().trim();
    if (!priceMap.has(key)) priceMap.set(key, []);
    const priceMxn = inv.moneda === 'USD' ? Math.round(inv.precio * 17.5) : inv.precio;
    if (priceMxn > 0) priceMap.get(key).push(priceMxn);
  }

  const { data: noPriceDevs } = await supabaseGet('developments',
    `select=id,name&price_min_mxn=is.null&limit=${limit}`);
  log.info(`=== PRICE CALC: ${noPriceDevs.length} devs sin precio, ${priceMap.size} proyectos en inventario ===`);

  let enriched = 0;
  const batchUpdates = [];

  for (const dev of noPriceDevs) {
    const key = dev.name.toUpperCase().trim();
    let prices = priceMap.get(key);

    if (!prices) {
      for (const [k, v] of priceMap) {
        if (key.includes(k) || k.includes(key)) { prices = v; break; }
      }
    }

    if (!prices || prices.length === 0) {
      const { data: units } = await supabaseGet('units',
        `select=price_mxn,price_usd&development_id=eq.${dev.id}&or=(price_mxn.not.is.null,price_usd.not.is.null)&limit=100`);
      if (units.length > 0) {
        const mxn = units.filter(u => u.price_mxn > 0).map(u => u.price_mxn);
        const usd = units.filter(u => u.price_usd > 0).map(u => Math.round(u.price_usd * 17.5));
        prices = mxn.length > 0 ? mxn : usd;
      }
    }

    if (!prices || prices.length === 0) continue;

    batchUpdates.push({ id: dev.id, data: { price_min_mxn: Math.min(...prices), price_max_mxn: Math.max(...prices) } });
    enriched++;
    if (enriched % 25 === 0) log.ok(`Prices: ${enriched} developments matched so far`);
  }

  if (batchUpdates.length > 0 && !dryRun) {
    try { await supabaseBatchUpdate('developments', batchUpdates); stats.updates += batchUpdates.length; }
    catch (e) { log.err(`Batch price update failed: ${e.message}`); stats.errors += batchUpdates.length; }
  }

  log.ok(`Price calculation done: ${enriched}/${noPriceDevs.length} enriched`);
  return enriched;
}

// ============================================================
// STRATEGY 6: DEVELOPER ENRICHMENT
// ============================================================

async function runDeveloperEnrichment(dryRun, limit) {
  const { data: developers } = await supabaseGet('developers',
    'select=id,name,slug,website,description_es,city,state&website=is.null&limit=' + (limit || 50));

  log.info(`=== DEVELOPER ENRICHMENT: ${developers.length} developers sin website ===`);

  for (let i = 0; i < developers.length; i++) {
    const dev = developers[i];
    const cleanName = dev.name.replace(/([A-Z][a-z]+)\s+([A-Z][a-z]+)\s*$/, '').trim() || dev.name;

    const results = await webSearch(`"${cleanName}" desarrolladora inmobiliaria México sitio web`);
    const updates = {};

    for (const result of results) {
      const text = `${result.title} ${result.snippet}`.toLowerCase();
      const cleanLower = cleanName.toLowerCase();

      if (text.includes(cleanLower.slice(0, 8)) || result.url.toLowerCase().includes(cleanLower.replace(/\s+/g, '').slice(0, 6))) {
        if (!dev.website) { try { updates.website = new URL(result.url).origin; } catch {} }
        if (!dev.description_es && result.snippet.length > 30) updates.description_es = result.snippet.slice(0, 500);
      }

      const cities = ['Cancún', 'Playa del Carmen', 'Tulum', 'Mérida', 'Ciudad de México', 'Guadalajara', 'Monterrey'];
      for (const city of cities) {
        if (text.includes(city.toLowerCase()) && !dev.city) { updates.city = city; break; }
      }
    }

    if (Object.keys(updates).length > 0) {
      if (!dryRun) {
        try { await supabaseUpdate('developers', dev.id, updates); stats.updates++; }
        catch (e) { log.err(`Dev update failed: ${dev.name}: ${e.message}`); stats.errors++; continue; }
      }
      stats.developers_enriched++;
      log.ok(`[${i+1}/${developers.length}] Developer ${dev.name}: ${Object.keys(updates).join(', ')}`);
    }
    await sleep(3000);
  }
  log.ok(`Developer enrichment done: ${stats.developers_enriched} enriched`);
}

// ============================================================
// MAIN
// ============================================================

async function main() {
  const args = process.argv.slice(2);
  const dryRun = args.includes('--dry-run');
  const limitArg = args.indexOf('--limit');
  const limit = limitArg >= 0 ? parseInt(args[limitArg + 1]) : 100;

  const strategies = ['--geocode', '--classify', '--prices', '--scrape-source', '--search', '--developers'];
  const runAll = !args.some(a => strategies.includes(a));
  const runGeocode = runAll || args.includes('--geocode');
  const runClassify = runAll || args.includes('--classify');
  const runPrices = runAll || args.includes('--prices');
  const runScrape = runAll || args.includes('--scrape-source');
  const runSearch = runAll || args.includes('--search');
  const runDevs = runAll || args.includes('--developers');

  console.log('\n╔══════════════════════════════════════════╗');
  console.log('║  PROPYTE ENRICHMENT AGENT v1.0 (Node.js) ║');
  console.log('║  Enriqueciendo datos inmobiliarios       ║');
  console.log('╚══════════════════════════════════════════╝\n');

  if (dryRun) log.warn('DRY RUN MODE - no changes will be written to DB');
  log.info(`Limit: ${limit} items per strategy`);

  if (runGeocode) {
    const { data } = await supabaseGet('developments', `select=id,name,city,state,zone,lat&lat=is.null&limit=${limit}`);
    if (data.length > 0) await runGeocoding(data, dryRun);
    else log.info('All developments already have coordinates');
  }

  if (runClassify) {
    const { data } = await supabaseGet('developments', `select=id,name,property_types&development_type=is.null&limit=${limit}`);
    if (data.length > 0) await runClassification(data, dryRun);
    else log.info('All developments already have a type');
  }

  if (runPrices) await runPriceCalculation(dryRun, limit);

  if (runScrape) {
    const { data } = await supabaseGet('developments',
      `select=id,name,city,source_url,price_min_mxn,development_type,amenities,description_es&source_url=not.is.null&or=(price_min_mxn.is.null,development_type.is.null)&limit=${limit}`);
    if (data.length > 0) await runSourceScraping(data, dryRun);
    else log.info('No developments with source URLs needing enrichment');
  }

  if (runSearch) {
    const { data } = await supabaseGet('developments',
      `select=id,name,city,state,zone,price_min_mxn,development_type,source_url&or=(price_min_mxn.is.null,development_type.is.null)&limit=${limit}`);
    if (data.length > 0) await runWebSearch(data, dryRun);
    else log.info('All developments have price and type');
  }

  if (runDevs) await runDeveloperEnrichment(dryRun, limit);

  console.log('\n╔══════════════════════════════════════════╗');
  console.log('║  ENRICHMENT COMPLETE                     ║');
  console.log('╠══════════════════════════════════════════╣');
  console.log(`║  Geocoded:    ${String(stats.geocoded).padStart(6)}                   ║`);
  console.log(`║  Scraped:     ${String(stats.scraped).padStart(6)}                   ║`);
  console.log(`║  Searched:    ${String(stats.searched).padStart(6)}                   ║`);
  console.log(`║  Developers:  ${String(stats.developers_enriched).padStart(6)}                   ║`);
  console.log(`║  DB Updates:  ${String(stats.updates).padStart(6)}                   ║`);
  console.log(`║  Errors:      ${String(stats.errors).padStart(6)}                   ║`);
  console.log(`║  Skipped:     ${String(stats.skipped).padStart(6)}                   ║`);
  console.log('╚══════════════════════════════════════════╝\n');
}

main().catch(e => { log.err(`Fatal: ${e.message}`); process.exit(1); });
