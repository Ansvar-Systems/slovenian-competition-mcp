/**
 * Ingestion crawler for the AVK (Javna agencija za varstvo konkurence —
 * Slovenian Competition Protection Agency) MCP server.
 *
 * Scrapes competition enforcement decisions, merger control decisions, and
 * sector data from varstvo-konkurence.si and populates the SQLite database.
 *
 * Data sources:
 *   - https://www.varstvo-konkurence.si/en/antitrust/decisions/
 *       Single-page table of antitrust enforcement decisions (restrictive
 *       agreements, abuse of dominance, sector inquiries). ~40 rows.
 *   - https://www.varstvo-konkurence.si/en/concentrations-of-undertakings/notified-concentrations-and-decisions-1/
 *       Single-page table of merger/concentration notifications and decisions.
 *       ~500+ rows dating from 2018 onwards.
 *   - https://www.varstvo-konkurence.si/ostali-dokumenti/arhiv-odlocb/odlocbaNNN/
 *       Individual decision pages with full-text rulings in Slovenian.
 *
 * Table columns (antitrust):
 *   - Datum (date of procedure initiation)
 *   - Krsitev (violation article, e.g. "6. clen ZPOmK-1", "101 PDEU")
 *   - Stranke (parties)
 *   - Sektor oz. dejavnost (sector / activity)
 *   - Opr. st. zadeve (case number, e.g. 3062-13/2019)
 *   - Vrsta in datum odlocitve (decision type and date)
 *   - Izrek (ruling — links to /ostali-dokumenti/arhiv-odlocb/odlocbaNNN/)
 *   - Nezaupno besedilo odlocitve (full decision text — PDF links)
 *   - Opombe (notes)
 *
 * Table columns (mergers):
 *   - Datum priglasitve (notification date)
 *   - Priglasitelj (notifier / acquiring party)
 *   - Prevzeto podjetje (acquired company / target)
 *   - Sektor oz. dejavnost (sector / activity)
 *   - Opr. st. zadeve (case number, e.g. 3061-6/2026)
 *   - Datum sklepa o uvedbi postopka po uradni dolznosti (ex-officio date)
 *   - Datum sklepa o uvedbi 2. faze (phase 2 date)
 *   - Vrsta in datum odlocitve (decision type and date)
 *   - Izrek odlocbe (ruling link)
 *   - Nezaupno besedilo odlocitve (full decision text link)
 *   - Opombe (notes)
 *
 * Usage:
 *   npx tsx scripts/ingest-avk.ts
 *   npx tsx scripts/ingest-avk.ts --dry-run
 *   npx tsx scripts/ingest-avk.ts --resume
 *   npx tsx scripts/ingest-avk.ts --force
 */

import Database from "better-sqlite3";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { dirname, join } from "node:path";
import * as cheerio from "cheerio";
import { SCHEMA_SQL } from "../src/db.js";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const DB_PATH = process.env["AVK_DB_PATH"] ?? "data/avk.db";
const STATE_FILE = join(dirname(DB_PATH), "ingest-state.json");
const BASE_URL = "https://www.varstvo-konkurence.si";
const RATE_LIMIT_MS = 1500;
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 5000;
const USER_AGENT =
  "AnsvarAVKCrawler/1.0 (+https://github.com/Ansvar-Systems/slovenian-competition-mcp)";

/**
 * Listing pages on varstvo-konkurence.si.
 *
 * Both pages are single-page tables (no pagination). The antitrust page
 * lists enforcement decisions; the merger page lists concentration
 * notifications and decisions.
 */
const LISTING_PAGES = [
  {
    id: "antitrust",
    path: "/en/antitrust/decisions/",
    isMerger: false,
  },
  {
    id: "concentrations",
    path: "/en/concentrations-of-undertakings/notified-concentrations-and-decisions-1/",
    isMerger: true,
  },
] as const;

// CLI flags
const dryRun = process.argv.includes("--dry-run");
const resume = process.argv.includes("--resume");
const force = process.argv.includes("--force");

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface IngestState {
  processedUrls: string[];
  lastRun: string;
  decisionsIngested: number;
  mergersIngested: number;
  errors: string[];
}

interface ParsedDecision {
  case_number: string;
  title: string;
  date: string | null;
  type: string | null;
  sector: string | null;
  parties: string | null;
  summary: string | null;
  full_text: string;
  outcome: string | null;
  fine_amount: number | null;
  gwb_articles: string | null;
  status: string;
}

interface ParsedMerger {
  case_number: string;
  title: string;
  date: string | null;
  sector: string | null;
  acquiring_party: string | null;
  target: string | null;
  summary: string | null;
  full_text: string;
  outcome: string | null;
  turnover: number | null;
}

interface SectorAccumulator {
  [id: string]: {
    name: string;
    name_en: string | null;
    description: string | null;
    decisionCount: number;
    mergerCount: number;
  };
}

// ---------------------------------------------------------------------------
// HTTP fetching with rate limiting and retries
// ---------------------------------------------------------------------------

let lastRequestTime = 0;

async function rateLimitedFetch(url: string): Promise<string | null> {
  const now = Date.now();
  const elapsed = now - lastRequestTime;
  if (elapsed < RATE_LIMIT_MS) {
    await sleep(RATE_LIMIT_MS - elapsed);
  }

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      lastRequestTime = Date.now();
      const response = await fetch(url, {
        headers: {
          "User-Agent": USER_AGENT,
          Accept:
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "Accept-Language": "sl,en;q=0.5",
        },
        redirect: "follow",
        signal: AbortSignal.timeout(30_000),
      });

      if (response.status === 403 || response.status === 429) {
        console.warn(
          `  [WARN] HTTP ${response.status} for ${url} (attempt ${attempt}/${MAX_RETRIES})`,
        );
        if (attempt < MAX_RETRIES) {
          await sleep(RETRY_DELAY_MS * attempt);
          continue;
        }
        return null;
      }

      if (!response.ok) {
        console.warn(`  [WARN] HTTP ${response.status} for ${url}`);
        return null;
      }

      return await response.text();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.warn(
        `  [WARN] Fetch error for ${url} (attempt ${attempt}/${MAX_RETRIES}): ${message}`,
      );
      if (attempt < MAX_RETRIES) {
        await sleep(RETRY_DELAY_MS * attempt);
      }
    }
  }

  return null;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// State management (for --resume)
// ---------------------------------------------------------------------------

function loadState(): IngestState {
  if (resume && existsSync(STATE_FILE)) {
    try {
      const raw = readFileSync(STATE_FILE, "utf-8");
      return JSON.parse(raw) as IngestState;
    } catch {
      console.warn("[WARN] Could not read state file, starting fresh.");
    }
  }
  return {
    processedUrls: [],
    lastRun: new Date().toISOString(),
    decisionsIngested: 0,
    mergersIngested: 0,
    errors: [],
  };
}

function saveState(state: IngestState): void {
  state.lastRun = new Date().toISOString();
  const dir = dirname(STATE_FILE);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }
  writeFileSync(STATE_FILE, JSON.stringify(state, null, 2), "utf-8");
}

// ---------------------------------------------------------------------------
// Listing page parsing — extract rows from single-page tables
// ---------------------------------------------------------------------------

interface AntitrustRow {
  date: string | null;
  violation: string | null;
  parties: string | null;
  sector: string | null;
  caseNumber: string | null;
  decisionInfo: string | null;
  rulingUrl: string | null;
  pdfUrl: string | null;
  notes: string | null;
}

interface MergerRow {
  date: string | null;
  acquiringParty: string | null;
  target: string | null;
  sector: string | null;
  caseNumber: string | null;
  exOfficioDate: string | null;
  phase2Date: string | null;
  decisionInfo: string | null;
  rulingUrl: string | null;
  pdfUrl: string | null;
  notes: string | null;
}

/**
 * Parse the antitrust decisions listing table.
 *
 * The page is a single table with 9 columns. Each row is an enforcement
 * case. The 7th column (Izrek) may contain a link to the ruling page
 * at /ostali-dokumenti/arhiv-odlocb/odlocbaNNN/.
 */
function parseAntitrustListing(html: string): AntitrustRow[] {
  const $ = cheerio.load(html);
  const rows: AntitrustRow[] = [];

  $("table tr").each((_i, tr) => {
    const cells = $(tr).find("td");
    if (cells.length < 5) return; // skip header rows or malformed rows

    const cellTexts: string[] = [];
    const cellLinks: Array<string | null> = [];

    cells.each((_j, td) => {
      cellTexts.push($(td).text().trim());
      const link = $(td).find("a[href]").first().attr("href") ?? null;
      cellLinks.push(link);
    });

    // Extract ruling URL from Izrek column (column 7, index 6)
    let rulingUrl: string | null = null;
    if (cellLinks[6]) {
      const href = cellLinks[6];
      if (href.includes("arhiv-odlocb")) {
        rulingUrl = href.startsWith("http") ? href : `${BASE_URL}${href}`;
      }
    }

    // Extract PDF URL from Nezaupno besedilo column (column 8, index 7)
    let pdfUrl: string | null = null;
    if (cellLinks[7]) {
      const href = cellLinks[7];
      if (href.includes(".pdf")) {
        pdfUrl = href.startsWith("http") ? href : `${BASE_URL}${href}`;
      }
    }

    rows.push({
      date: cellTexts[0] ?? null,
      violation: cellTexts[1] ?? null,
      parties: cellTexts[2] ?? null,
      sector: cellTexts[3] ?? null,
      caseNumber: cellTexts[4] ?? null,
      decisionInfo: cellTexts[5] ?? null,
      rulingUrl,
      pdfUrl,
      notes: cellTexts[8] ?? null,
    });
  });

  console.log(`  Parsed ${rows.length} rows from antitrust listing`);
  return rows;
}

/**
 * Parse the merger/concentration listing table.
 *
 * The page is a single table with 11 columns. Each row represents a
 * concentration notification. The ruling link column contains links to
 * /ostali-dokumenti/arhiv-odlocb/odlocbaNNN/.
 */
function parseMergerListing(html: string): MergerRow[] {
  const $ = cheerio.load(html);
  const rows: MergerRow[] = [];

  $("table tr").each((_i, tr) => {
    const cells = $(tr).find("td");
    if (cells.length < 5) return;

    const cellTexts: string[] = [];
    const cellLinks: Array<string | null> = [];

    cells.each((_j, td) => {
      cellTexts.push($(td).text().trim());
      const link = $(td).find("a[href]").first().attr("href") ?? null;
      cellLinks.push(link);
    });

    // Ruling URL — typically in column 9 (index 8)
    let rulingUrl: string | null = null;
    // PDF URL — typically in column 10 (index 9)
    let pdfUrl: string | null = null;

    // Scan all cells for decision links since column count may vary
    cellLinks.forEach((href) => {
      if (!href) return;
      if (href.includes("arhiv-odlocb") && !rulingUrl) {
        rulingUrl = href.startsWith("http") ? href : `${BASE_URL}${href}`;
      }
      if (href.includes(".pdf") && !pdfUrl) {
        pdfUrl = href.startsWith("http") ? href : `${BASE_URL}${href}`;
      }
    });

    rows.push({
      date: cellTexts[0] ?? null,
      acquiringParty: cellTexts[1] ?? null,
      target: cellTexts[2] ?? null,
      sector: cellTexts[3] ?? null,
      caseNumber: cellTexts[4] ?? null,
      exOfficioDate: cellTexts[5] ?? null,
      phase2Date: cellTexts[6] ?? null,
      decisionInfo: cellTexts[7] ?? null,
      rulingUrl,
      pdfUrl,
      notes: cellTexts[10] ?? null,
    });
  });

  console.log(`  Parsed ${rows.length} rows from merger listing`);
  return rows;
}

// ---------------------------------------------------------------------------
// Individual decision page parsing
// ---------------------------------------------------------------------------

/**
 * Parse a single AVK decision page (/ostali-dokumenti/arhiv-odlocb/odlocbaNNN/).
 *
 * These pages contain the full ruling text in Slovenian. The structure
 * varies but typically includes: parties, violation period, penalty
 * (globe), and legal reasoning.
 */
function parseDecisionPage(html: string): {
  fullText: string;
  fineAmount: number | null;
  legalArticles: string[];
} {
  const $ = cheerio.load(html);

  // Remove navigation, headers, footers
  $(
    "nav, footer, header, .menu, .breadcrumb, script, style, .skip-link, .topbar, .navbar",
  ).remove();

  // Extract body text from main content area
  const bodySelectors = [
    ".main-content",
    "main article",
    "main .content",
    ".ce-bodytext",
    ".content-area",
    "main",
    "article",
    "#content",
  ];

  let bodyText = "";
  for (const sel of bodySelectors) {
    const el = $(sel);
    if (el.length > 0) {
      bodyText = el.text().trim();
      if (bodyText.length > 200) break;
    }
  }

  // Fallback: gather all paragraphs
  if (!bodyText || bodyText.length < 200) {
    const paragraphs: string[] = [];
    $("main p, article p, .content p, p").each((_i, el) => {
      const text = $(el).text().trim();
      if (text.length > 20) paragraphs.push(text);
    });
    if (paragraphs.length > 0) {
      bodyText = paragraphs.join("\n\n");
    }
  }

  // Last resort: strip navigation and take remaining content
  if (!bodyText || bodyText.length < 100) {
    bodyText = $("body").text().trim();
  }

  // Clean up whitespace
  bodyText = bodyText.replace(/\s+/g, " ").trim();

  const fineAmount = extractFineAmount(bodyText);
  const legalArticles = extractLegalArticles(bodyText);

  return { fullText: bodyText, fineAmount, legalArticles };
}

// ---------------------------------------------------------------------------
// Date parsing
// ---------------------------------------------------------------------------

/**
 * Parse a Slovenian date string to ISO format (yyyy-MM-dd).
 *
 * Handles formats:
 *   - d.M.yyyy (most common on AVK pages, e.g. "12. 4. 2023")
 *   - d. mesec yyyy (Slovenian textual dates, e.g. "20. december 2023")
 *   - yyyy-MM-dd (already ISO)
 */
function parseSlovenianDate(raw: string): string | null {
  if (!raw) return null;

  // Clean up: remove extra spaces around dots
  const cleaned = raw.replace(/\s+/g, " ").trim();

  // d.M.yyyy or d. M. yyyy (with optional spaces)
  const dotMatch = cleaned.match(
    /(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})/,
  );
  if (dotMatch) {
    const [, day, month, year] = dotMatch;
    return `${year}-${month!.padStart(2, "0")}-${day!.padStart(2, "0")}`;
  }

  // Slovenian textual month: "20. december 2023"
  const slovenianMonths: Record<string, string> = {
    januar: "01",
    januarja: "01",
    februar: "02",
    februarja: "02",
    marec: "03",
    marca: "03",
    april: "04",
    aprila: "04",
    maj: "05",
    maja: "05",
    junij: "06",
    junija: "06",
    julij: "07",
    julija: "07",
    avgust: "08",
    avgusta: "08",
    september: "09",
    septembra: "09",
    oktober: "10",
    oktobra: "10",
    november: "11",
    novembra: "11",
    december: "12",
    decembra: "12",
  };

  const textMatch = cleaned.match(
    /(\d{1,2})\.\s*(\w+)\s+(\d{4})/,
  );
  if (textMatch) {
    const [, day, monthName, year] = textMatch;
    const monthNum = slovenianMonths[monthName!.toLowerCase()];
    if (monthNum) {
      return `${year}-${monthNum}-${day!.padStart(2, "0")}`;
    }
  }

  // Already ISO: yyyy-MM-dd
  const isoMatch = cleaned.match(/(\d{4})-(\d{2})-(\d{2})/);
  if (isoMatch) {
    return isoMatch[0];
  }

  return null;
}

/**
 * Extract a decision date from the "Vrsta in datum odlocitve" cell.
 *
 * These cells contain text like "Odlocba z dne 20.12.2023" or
 * "Zaveze: 22.12.2021" or just a date "10.3.2026".
 */
function extractDecisionDate(text: string): string | null {
  if (!text) return null;

  // Match date patterns within the text
  const dateMatch = text.match(/(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})/);
  if (dateMatch) {
    return parseSlovenianDate(dateMatch[0]);
  }

  return null;
}

// ---------------------------------------------------------------------------
// Fine / penalty extraction
// ---------------------------------------------------------------------------

/**
 * Extract a fine/penalty amount from Slovenian text.
 *
 * Slovenian uses dot as thousands separator and comma as decimal separator.
 * Amounts are in EUR (Slovenia uses the euro since 2007).
 */
function extractFineAmount(text: string): number | null {
  const patterns = [
    // "N milijonov EUR" / "N mio EUR" / "N mio. EUR"
    {
      regex: /([\d,.\s]+)\s*(?:milijon(?:ov|a|e)?|mio\.?)\s*(?:EUR|evrov?)\b/gi,
      multiplier: 1_000_000,
    },
    // "N milijard EUR"
    {
      regex: /([\d,.\s]+)\s*(?:milijard(?:e|a)?)\s*(?:EUR|evrov?)\b/gi,
      multiplier: 1_000_000_000,
    },
    // "globo v visini N EUR" / "globa N EUR" / "globo N EUR"
    {
      regex: /glob[oaeu]\s+(?:v\s+vi[sš]ini\s+)?([\d.,\s]+)\s*(?:EUR|evrov?)\b/gi,
      multiplier: 1,
    },
    // "€ N" or "EUR N" or "N EUR" or "N evrov"
    {
      regex: /(?:€|EUR)\s*([\d\s.]+(?:,\d+)?)/gi,
      multiplier: 1,
    },
    {
      regex: /([\d\s.]+(?:,\d+)?)\s*(?:EUR|evrov?)\b/gi,
      multiplier: 1,
    },
  ];

  for (const { regex, multiplier } of patterns) {
    const match = regex.exec(text);
    if (match?.[1]) {
      // Slovenian number format: dot = thousands, comma = decimal
      let numStr = match[1].trim().replace(/[\s.]/g, "").replace(",", ".");
      const val = parseFloat(numStr);
      if (isNaN(val) || val <= 0) continue;

      const result = val * multiplier;
      // Only return amounts that look like actual fines (> 1000 EUR)
      if (result > 1000 || multiplier > 1) return result;
    }
  }

  return null;
}

// ---------------------------------------------------------------------------
// Legal article extraction
// ---------------------------------------------------------------------------

/**
 * Extract cited Slovenian competition law articles and EU treaty articles.
 *
 * ZPOmK-2 is the current Competition Protection Act (Zakon o preprecevanju
 * omejevanja konkurence, 2022). ZPOmK-1 is its predecessor.
 * PDEU = Pogodba o delovanju Evropske unije (TFEU in Slovenian).
 */
function extractLegalArticles(text: string): string[] {
  const articles: Set<string> = new Set();

  // ZPOmK-2 / ZPOmK-1 articles: "5. clen ZPOmK-2" / "clen 6 ZPOmK-1"
  // Also handles "Art. 5 ZPOmK-2" and "6. člen ZPOmK-1"
  const zpomkPattern =
    /(?:(\d+)\.\s*(?:[čc]len|[čc]l\.)\s*(ZPOmK-[12]))|(?:(?:Art\.?\s*)?(\d+)\s*(?:[čc]len|[čc]l\.)?\s*(ZPOmK-[12]))/gi;
  let m: RegExpExecArray | null;
  while ((m = zpomkPattern.exec(text)) !== null) {
    const artNum = m[1] ?? m[3];
    const law = m[2] ?? m[4];
    if (artNum && law) {
      articles.add(`${law} ${artNum}. clen`);
    }
  }

  // Standalone "N. clen ZPOmK" patterns
  const standalonePattern = /(\d+)\.\s*(?:[čc]len|[čc]l\.)\s*(ZPOmK(?:-[12])?)/gi;
  while ((m = standalonePattern.exec(text)) !== null) {
    articles.add(`${m[2]} ${m[1]}. clen`);
  }

  // EU treaty: PDEU 101/102 (TFEU in Slovenian) or "101. clen PDEU"
  const pdeuPattern =
    /(?:(\d{2,3})\.\s*(?:[čc]len|[čc]l\.?|Art\.?)?\s*(?:PDEU|TFEU))|(?:(?:PDEU|TFEU)\s*(?:Art\.?\s*)?(\d{2,3}))/gi;
  while ((m = pdeuPattern.exec(text)) !== null) {
    const artNum = parseInt(m[1] ?? m[2] ?? "0", 10);
    if (artNum === 101 || artNum === 102) {
      articles.add(`PDEU ${artNum}. clen`);
    }
  }

  // "Art. 101" / "Art. 102" general patterns
  const artPattern = /Art(?:icle|\.?)?\s*(101|102)\b/gi;
  while ((m = artPattern.exec(text)) !== null) {
    articles.add(`PDEU ${m[1]}. clen`);
  }

  return [...articles];
}

// ---------------------------------------------------------------------------
// Decision type and outcome classification
// ---------------------------------------------------------------------------

/**
 * Classify an antitrust decision based on the violation article and
 * content in Slovenian.
 */
function classifyDecisionType(
  violation: string | null,
  title: string,
  bodyText: string,
): {
  type: string | null;
  outcome: string | null;
} {
  const lowerViolation = (violation ?? "").toLowerCase();
  const all = `${title} ${bodyText}`.toLowerCase().slice(0, 4000);

  // --- Type classification ---
  let type: string | null = null;

  if (
    lowerViolation.includes("9.") ||
    lowerViolation.includes("102") ||
    all.includes("zloraba prevladujocega") ||
    all.includes("zloraba prevladujočega") ||
    all.includes("abuse of dominan")
  ) {
    type = "abuse_of_dominance";
  } else if (
    lowerViolation.includes("5.") ||
    lowerViolation.includes("6.") ||
    lowerViolation.includes("101") ||
    all.includes("omejevalni sporazum") ||
    all.includes("kartel") ||
    all.includes("usklajen") ||
    all.includes("dogovarjal") ||
    all.includes("prikrojevala ponudbe")
  ) {
    type = "cartel";
  } else if (
    all.includes("sektorska preiskava") ||
    all.includes("trzna analiza") ||
    all.includes("tržna analiza") ||
    all.includes("sector inquiry")
  ) {
    type = "sector_inquiry";
  } else {
    type = "decision";
  }

  // --- Outcome classification ---
  let outcome: string | null = null;

  if (
    all.includes("globa") ||
    all.includes("globe") ||
    all.includes("globo") ||
    all.includes("denarno kazen")
  ) {
    outcome = "fine";
  } else if (
    all.includes("zaveze") ||
    all.includes("zaveza") ||
    all.includes("pogojno")
  ) {
    outcome = "cleared_with_conditions";
  } else if (
    all.includes("ustavitev postopka") ||
    all.includes("ustavil postopek") ||
    all.includes("postopek ustavljen")
  ) {
    outcome = "closed";
  } else if (
    all.includes("krsitev ni ugotovljena") ||
    all.includes("kršitev ni ugotovljena") ||
    all.includes("ni krsitve") ||
    all.includes("ni kršitve")
  ) {
    outcome = "cleared";
  } else if (
    all.includes("prepovedan") ||
    all.includes("prepovedal") ||
    all.includes("krsitev ugotovljena") ||
    all.includes("kršitev ugotovljena")
  ) {
    outcome = "prohibited";
  }

  return { type, outcome };
}

/**
 * Classify a merger outcome based on the decision info text.
 */
function classifyMergerOutcome(
  decisionInfo: string | null,
  bodyText: string,
): string | null {
  const all = `${decisionInfo ?? ""} ${bodyText}`.toLowerCase();

  if (
    all.includes("prepovedan") ||
    all.includes("ni skladna") ||
    all.includes("prepovedala")
  ) {
    return "prohibited";
  }
  if (
    all.includes("pogoj") ||
    all.includes("zavez") ||
    all.includes("popravni ukrep")
  ) {
    return "cleared_with_conditions";
  }
  if (
    all.includes("2. faz") ||
    all.includes("druga faza") ||
    all.includes("phase 2") ||
    all.includes("phase ii")
  ) {
    return "cleared_phase2";
  }
  if (
    all.includes("skladna") ||
    all.includes("odobrena") ||
    all.includes("odlocba") ||
    all.includes("odločba")
  ) {
    return "cleared_phase1";
  }

  // Default: most mergers are approved in phase 1
  return "cleared_phase1";
}

// ---------------------------------------------------------------------------
// Sector classification
// ---------------------------------------------------------------------------

/**
 * Map Slovenian keywords in title/body to sector IDs.
 */
function classifySector(
  sectorText: string | null,
  title: string,
  bodyText: string,
): string | null {
  const text = `${sectorText ?? ""} ${title} ${bodyText.slice(0, 2000)}`.toLowerCase();

  const sectorMapping: Array<{ id: string; patterns: string[] }> = [
    {
      id: "energy",
      patterns: [
        "energet",
        "elektri",
        "elektro",
        "plin",
        "naft",
        "ogrevanj",
        "sončn",
        "soncn",
        "obnovljiv",
        "energija",
      ],
    },
    {
      id: "retail",
      patterns: [
        "maloprodaj",
        "trgovin",
        "prehrambeni",
        "prodajaln",
        "supermarket",
        "nakupovaln",
        "mercator",
        "spar",
        "tuš",
        "tus",
        "hofer",
        "lidl",
      ],
    },
    {
      id: "telecommunications",
      patterns: [
        "telekomunikacij",
        "telekomun",
        "mobiln",
        "širokopasovn",
        "sirokopasovn",
        "kabelska",
        "telekom",
        "telemach",
        "a1",
        "t-2",
      ],
    },
    {
      id: "pharmaceuticals",
      patterns: [
        "farmacevtsk",
        "lekarn",
        "zdravil",
        "medicinsk",
        "farmacija",
        "distribucij.*zdravil",
      ],
    },
    {
      id: "construction",
      patterns: [
        "gradbenist",
        "gradben",
        "gradbeni",
        "nepremicnin",
        "nepremičnin",
        "cestna",
        "infrastruktur",
        "asfaltn",
        "betonarn",
      ],
    },
    {
      id: "financial_services",
      patterns: [
        "bancn",
        "bančn",
        "financn",
        "finančn",
        "zavarovalnist",
        "zavarovaln",
        "kredit",
        "posojil",
        "investicij",
      ],
    },
    {
      id: "transport",
      patterns: [
        "prevoz",
        "promet",
        "transport",
        "logistik",
        "letalsk",
        "železnisk",
        "zeleznisk",
        "avtobus",
        "taksi",
        "ladijsk",
        "pristanišč",
      ],
    },
    {
      id: "food_industry",
      patterns: [
        "živilsk",
        "zivilsk",
        "prehran",
        "mlekarn",
        "mesar",
        "pekovsk",
        "kmetijsk",
        "mlin",
        "pšenic",
        "psenic",
        "olje",
      ],
    },
    {
      id: "media",
      patterns: [
        "medij",
        "založnist",
        "zaloznist",
        "časopis",
        "casopis",
        "televizij",
        "radio",
        "oglaševanj",
        "oglasevan",
        "tisk",
      ],
    },
    {
      id: "automotive",
      patterns: [
        "avtomobil",
        "motornih vozil",
        "avto",
        "vozil",
        "avtohiša",
        "avtohisa",
      ],
    },
    {
      id: "healthcare",
      patterns: [
        "zdravstv",
        "bolnišnic",
        "bolnisnic",
        "zdravnik",
        "laboratorij",
        "diagnostik",
      ],
    },
    {
      id: "digital_economy",
      patterns: [
        "digital",
        "informacijsk",
        "programsk",
        "informacijske",
        "internet",
        "spletna",
        "e-trgovin",
      ],
    },
    {
      id: "waste_management",
      patterns: [
        "odpadk",
        "odpadni",
        "reciklaž",
        "reciklaz",
        "okoljevarstven",
        "komunaln",
      ],
    },
    {
      id: "veterinary",
      patterns: [
        "veterinar",
      ],
    },
    {
      id: "education",
      patterns: [
        "izobraževanj",
        "izobrazevanj",
        "usposabljan",
        "šol",
        "sol",
      ],
    },
  ];

  for (const { id, patterns } of sectorMapping) {
    for (const p of patterns) {
      if (text.includes(p)) return id;
    }
  }

  return null;
}

// ---------------------------------------------------------------------------
// Sector metadata (Slovenian names with English translations)
// ---------------------------------------------------------------------------

const SECTOR_META: Record<string, { name: string; name_en: string }> = {
  energy: { name: "Energetika", name_en: "Energy" },
  retail: { name: "Maloprodaja", name_en: "Retail" },
  telecommunications: {
    name: "Telekomunikacije",
    name_en: "Telecommunications",
  },
  pharmaceuticals: { name: "Farmacija", name_en: "Pharmaceuticals" },
  construction: { name: "Gradbeništvo", name_en: "Construction" },
  financial_services: {
    name: "Finančne storitve",
    name_en: "Financial Services",
  },
  transport: { name: "Promet in logistika", name_en: "Transport & Logistics" },
  food_industry: {
    name: "Živilska industrija",
    name_en: "Food Industry",
  },
  media: { name: "Mediji", name_en: "Media" },
  automotive: {
    name: "Avtomobilska industrija",
    name_en: "Automotive",
  },
  healthcare: { name: "Zdravstvo", name_en: "Healthcare" },
  digital_economy: {
    name: "Digitalno gospodarstvo",
    name_en: "Digital Economy",
  },
  waste_management: {
    name: "Ravnanje z odpadki",
    name_en: "Waste Management",
  },
  veterinary: {
    name: "Veterinarska dejavnost",
    name_en: "Veterinary Services",
  },
  education: {
    name: "Izobraževanje",
    name_en: "Education & Training",
  },
};

// ---------------------------------------------------------------------------
// Build ParsedDecision / ParsedMerger from listing rows + detail pages
// ---------------------------------------------------------------------------

/**
 * Build a ParsedDecision from an antitrust listing row and optional
 * detail page content.
 */
function buildDecision(
  row: AntitrustRow,
  detailText: string | null,
  detailFine: number | null,
  detailArticles: string[],
): ParsedDecision | null {
  const caseNumber = row.caseNumber?.trim();
  if (!caseNumber) return null;

  const parties = row.parties?.trim();
  if (!parties) return null;

  // Use parties as the title (AVK does not have separate title fields)
  const title = `${parties} — ${row.sector ?? ""}`.trim().replace(/\s*—\s*$/, "");

  const date =
    extractDecisionDate(row.decisionInfo ?? "") ??
    parseSlovenianDate(row.date ?? "");

  const sector = classifySector(row.sector, title, detailText ?? "");

  // Full text: prefer detail page, fall back to listing row data
  const fullText = detailText && detailText.length > 100
    ? detailText
    : [
        `Zadeva: ${caseNumber}`,
        `Stranke: ${parties}`,
        row.violation ? `Kršitev: ${row.violation}` : null,
        row.sector ? `Sektor: ${row.sector}` : null,
        row.decisionInfo ? `Odločitev: ${row.decisionInfo}` : null,
        row.notes ? `Opombe: ${row.notes}` : null,
      ]
        .filter(Boolean)
        .join("\n");

  const { type, outcome } = classifyDecisionType(
    row.violation,
    title,
    fullText,
  );

  // Legal articles: from detail page or from violation column
  let legalArticles = detailArticles.length > 0
    ? detailArticles
    : extractLegalArticles(`${row.violation ?? ""} ${fullText}`);

  const fineAmount = detailFine ?? extractFineAmount(fullText);

  // Summary: first 500 chars of full text
  const summary = fullText.slice(0, 500).replace(/\s+/g, " ").trim();

  // Determine status from notes
  let status = "final";
  const notesLower = (row.notes ?? "").toLowerCase();
  const decisionLower = (row.decisionInfo ?? "").toLowerCase();
  if (
    notesLower.includes("pritožb") ||
    notesLower.includes("pritozb") ||
    notesLower.includes("sodišč") ||
    notesLower.includes("sodisc") ||
    decisionLower.includes("v obravnav")
  ) {
    status = "appealed";
  }
  if (!row.decisionInfo || row.decisionInfo.trim() === "") {
    status = "pending";
  }

  return {
    case_number: caseNumber,
    title,
    date,
    type,
    sector,
    parties: JSON.stringify(
      parties
        .split(/[,;]/)
        .map((p) => p.trim())
        .filter(Boolean),
    ),
    summary,
    full_text: fullText,
    outcome: outcome ?? (fineAmount ? "fine" : null),
    fine_amount: fineAmount,
    gwb_articles:
      legalArticles.length > 0 ? JSON.stringify(legalArticles) : null,
    status,
  };
}

/**
 * Build a ParsedMerger from a merger listing row and optional
 * detail page content.
 */
function buildMerger(
  row: MergerRow,
  detailText: string | null,
): ParsedMerger | null {
  const caseNumber = row.caseNumber?.trim();
  if (!caseNumber) return null;

  const acquiringParty = row.acquiringParty?.trim() ?? null;
  const target = row.target?.trim() ?? null;

  if (!acquiringParty && !target) return null;

  // Build title from acquiring party and target
  const titleParts = [acquiringParty, target].filter(Boolean);
  const title = titleParts.join(" / ");

  const date =
    extractDecisionDate(row.decisionInfo ?? "") ??
    parseSlovenianDate(row.date ?? "");

  const sector = classifySector(
    row.sector,
    title,
    detailText ?? "",
  );

  // Full text: prefer detail page, fall back to listing row data
  const fullText = detailText && detailText.length > 100
    ? detailText
    : [
        `Zadeva: ${caseNumber}`,
        acquiringParty ? `Priglasitelj: ${acquiringParty}` : null,
        target ? `Prevzeto podjetje: ${target}` : null,
        row.sector ? `Sektor: ${row.sector}` : null,
        row.decisionInfo ? `Odločitev: ${row.decisionInfo}` : null,
        row.exOfficioDate
          ? `Uvedba postopka po uradni dolžnosti: ${row.exOfficioDate}`
          : null,
        row.phase2Date ? `Uvedba 2. faze: ${row.phase2Date}` : null,
        row.notes ? `Opombe: ${row.notes}` : null,
      ]
        .filter(Boolean)
        .join("\n");

  const outcome = classifyMergerOutcome(row.decisionInfo, fullText);

  const summary = fullText.slice(0, 500).replace(/\s+/g, " ").trim();

  return {
    case_number: caseNumber,
    title,
    date,
    sector,
    acquiring_party: acquiringParty,
    target,
    summary,
    full_text: fullText,
    outcome,
    turnover: null, // Turnover not available from HTML listings
  };
}

// ---------------------------------------------------------------------------
// Database operations
// ---------------------------------------------------------------------------

function initDb(): Database.Database {
  const dir = dirname(DB_PATH);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
    console.log(`Created data directory: ${dir}`);
  }

  if (force && existsSync(DB_PATH)) {
    unlinkSync(DB_PATH);
    console.log(`Deleted existing database (--force)`);
  }

  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  db.exec(SCHEMA_SQL);

  return db;
}

function prepareStatements(db: Database.Database) {
  const insertDecision = db.prepare(`
    INSERT OR IGNORE INTO decisions
      (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, gwb_articles, status)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const upsertDecision = db.prepare(`
    INSERT INTO decisions
      (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, gwb_articles, status)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(case_number) DO UPDATE SET
      title = excluded.title,
      date = excluded.date,
      type = excluded.type,
      sector = excluded.sector,
      parties = excluded.parties,
      summary = excluded.summary,
      full_text = excluded.full_text,
      outcome = excluded.outcome,
      fine_amount = excluded.fine_amount,
      gwb_articles = excluded.gwb_articles,
      status = excluded.status
  `);

  const insertMerger = db.prepare(`
    INSERT OR IGNORE INTO mergers
      (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const upsertMerger = db.prepare(`
    INSERT INTO mergers
      (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(case_number) DO UPDATE SET
      title = excluded.title,
      date = excluded.date,
      sector = excluded.sector,
      acquiring_party = excluded.acquiring_party,
      target = excluded.target,
      summary = excluded.summary,
      full_text = excluded.full_text,
      outcome = excluded.outcome,
      turnover = excluded.turnover
  `);

  const upsertSector = db.prepare(`
    INSERT INTO sectors (id, name, name_en, description, decision_count, merger_count)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(id) DO UPDATE SET
      decision_count = excluded.decision_count,
      merger_count = excluded.merger_count
  `);

  return {
    insertDecision,
    upsertDecision,
    insertMerger,
    upsertMerger,
    upsertSector,
  };
}

// ---------------------------------------------------------------------------
// Main ingestion pipeline
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log("=== AVK Competition Decisions Crawler ===");
  console.log(`  Database:    ${DB_PATH}`);
  console.log(`  Dry run:     ${dryRun}`);
  console.log(`  Resume:      ${resume}`);
  console.log(`  Force:       ${force}`);
  console.log("");

  // Load resume state
  const state = loadState();
  const processedSet = new Set(state.processedUrls);

  // Step 1: Fetch and parse listing pages
  console.log("Step 1: Fetching listing pages...");

  let antitrustRows: AntitrustRow[] = [];
  let mergerRows: MergerRow[] = [];

  for (const page of LISTING_PAGES) {
    const url = `${BASE_URL}${page.path}`;
    console.log(`\n  Fetching ${page.id}: ${url}`);

    const html = await rateLimitedFetch(url);
    if (!html) {
      console.error(`  ERROR: Could not fetch ${page.id} listing page`);
      state.errors.push(`fetch_failed: ${url}`);
      continue;
    }

    if (page.isMerger) {
      mergerRows = parseMergerListing(html);
    } else {
      antitrustRows = parseAntitrustListing(html);
    }
  }

  console.log(
    `\nTotal listing rows: ${antitrustRows.length} antitrust + ${mergerRows.length} mergers`,
  );

  // Step 2: Collect detail page URLs to fetch
  const detailUrls: Array<{
    url: string;
    caseNumber: string;
    type: "antitrust" | "merger";
    rowIndex: number;
  }> = [];

  antitrustRows.forEach((row, i) => {
    if (row.rulingUrl) {
      detailUrls.push({
        url: row.rulingUrl,
        caseNumber: row.caseNumber ?? `antitrust-${i}`,
        type: "antitrust",
        rowIndex: i,
      });
    }
  });

  mergerRows.forEach((row, i) => {
    if (row.rulingUrl) {
      detailUrls.push({
        url: row.rulingUrl,
        caseNumber: row.caseNumber ?? `merger-${i}`,
        type: "merger",
        rowIndex: i,
      });
    }
  });

  // Filter already-processed URLs (for --resume)
  const urlsToProcess = resume
    ? detailUrls.filter(({ url }) => !processedSet.has(url))
    : detailUrls;

  console.log(`\nDetail page URLs:     ${detailUrls.length}`);
  console.log(`URLs to process:      ${urlsToProcess.length}`);
  if (resume && detailUrls.length !== urlsToProcess.length) {
    console.log(
      `  Skipping ${detailUrls.length - urlsToProcess.length} already-processed URLs`,
    );
  }

  // Step 3: Fetch detail pages and store parsed content
  console.log("\nStep 2: Fetching detail pages...");

  const detailResults: Map<
    string,
    { fullText: string; fineAmount: number | null; legalArticles: string[] }
  > = new Map();

  for (let i = 0; i < urlsToProcess.length; i++) {
    const { url, caseNumber } = urlsToProcess[i]!;
    const progress = `[${i + 1}/${urlsToProcess.length}]`;

    console.log(`${progress} ${caseNumber} | ${url}`);

    const html = await rateLimitedFetch(url);
    if (!html) {
      console.log(`  SKIP — could not fetch detail page`);
      state.errors.push(`detail_fetch_failed: ${url}`);
      continue;
    }

    try {
      const result = parseDecisionPage(html);
      detailResults.set(url, result);
      processedSet.add(url);
      state.processedUrls.push(url);

      if (dryRun) {
        console.log(
          `  Parsed: ${result.fullText.length} chars, fine=${result.fineAmount}, articles=${result.legalArticles.join(", ") || "none"}`,
        );
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.error(`  ERROR: ${message}`);
      state.errors.push(`detail_parse_error: ${url}: ${message}`);
    }

    // Save state periodically (every 25 URLs)
    if ((i + 1) % 25 === 0) {
      saveState(state);
      console.log(`  [checkpoint] State saved after ${i + 1} detail pages`);
    }
  }

  // Step 4: Build and insert records
  console.log("\nStep 3: Building and inserting records...");

  let db: Database.Database | null = null;
  let stmts: ReturnType<typeof prepareStatements> | null = null;

  if (!dryRun) {
    db = initDb();
    stmts = prepareStatements(db);
  }

  let decisionsIngested = 0;
  let mergersIngested = 0;
  let skipped = 0;
  let errors = 0;

  // Process antitrust decisions
  console.log(`\n  Processing ${antitrustRows.length} antitrust rows...`);

  for (const row of antitrustRows) {
    const detail = row.rulingUrl ? detailResults.get(row.rulingUrl) : null;
    const decision = buildDecision(
      row,
      detail?.fullText ?? null,
      detail?.fineAmount ?? null,
      detail?.legalArticles ?? [],
    );

    if (!decision) {
      skipped++;
      continue;
    }

    if (dryRun) {
      console.log(
        `  DECISION: ${decision.case_number} — ${decision.title.slice(0, 80)}`,
      );
      console.log(
        `    type=${decision.type}, sector=${decision.sector}, outcome=${decision.outcome}, fine=${decision.fine_amount}, status=${decision.status}`,
      );
      decisionsIngested++;
    } else {
      try {
        const stmt = force
          ? stmts!.upsertDecision
          : stmts!.insertDecision;
        stmt.run(
          decision.case_number,
          decision.title,
          decision.date,
          decision.type,
          decision.sector,
          decision.parties,
          decision.summary,
          decision.full_text,
          decision.outcome,
          decision.fine_amount,
          decision.gwb_articles,
          decision.status,
        );
        console.log(`  INSERTED decision: ${decision.case_number}`);
        decisionsIngested++;
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        console.error(
          `  ERROR inserting decision ${decision.case_number}: ${message}`,
        );
        errors++;
      }
    }
  }

  // Process merger/concentration rows
  console.log(`\n  Processing ${mergerRows.length} merger rows...`);

  for (const row of mergerRows) {
    const detail = row.rulingUrl ? detailResults.get(row.rulingUrl) : null;
    const merger = buildMerger(row, detail?.fullText ?? null);

    if (!merger) {
      skipped++;
      continue;
    }

    if (dryRun) {
      console.log(
        `  MERGER: ${merger.case_number} — ${merger.title.slice(0, 80)}`,
      );
      console.log(
        `    sector=${merger.sector}, outcome=${merger.outcome}, acquiring=${merger.acquiring_party?.slice(0, 50)}`,
      );
      mergersIngested++;
    } else {
      try {
        const stmt = force
          ? stmts!.upsertMerger
          : stmts!.insertMerger;
        stmt.run(
          merger.case_number,
          merger.title,
          merger.date,
          merger.sector,
          merger.acquiring_party,
          merger.target,
          merger.summary,
          merger.full_text,
          merger.outcome,
          merger.turnover,
        );
        console.log(`  INSERTED merger: ${merger.case_number}`);
        mergersIngested++;
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        console.error(
          `  ERROR inserting merger ${merger.case_number}: ${message}`,
        );
        errors++;
      }
    }
  }

  // Step 5: Update sector counts from the database
  if (!dryRun && db && stmts) {
    const decisionSectorCounts = db
      .prepare(
        "SELECT sector, COUNT(*) as cnt FROM decisions WHERE sector IS NOT NULL GROUP BY sector",
      )
      .all() as Array<{ sector: string; cnt: number }>;
    const mergerSectorCounts = db
      .prepare(
        "SELECT sector, COUNT(*) as cnt FROM mergers WHERE sector IS NOT NULL GROUP BY sector",
      )
      .all() as Array<{ sector: string; cnt: number }>;

    const finalSectorCounts: Record<
      string,
      { decisions: number; mergers: number }
    > = {};
    for (const row of decisionSectorCounts) {
      if (!finalSectorCounts[row.sector])
        finalSectorCounts[row.sector] = { decisions: 0, mergers: 0 };
      finalSectorCounts[row.sector]!.decisions = row.cnt;
    }
    for (const row of mergerSectorCounts) {
      if (!finalSectorCounts[row.sector])
        finalSectorCounts[row.sector] = { decisions: 0, mergers: 0 };
      finalSectorCounts[row.sector]!.mergers = row.cnt;
    }

    const updateSectors = db.transaction(() => {
      for (const [id, counts] of Object.entries(finalSectorCounts)) {
        const meta = SECTOR_META[id];
        stmts!.upsertSector.run(
          id,
          meta?.name ?? id,
          meta?.name_en ?? null,
          null,
          counts.decisions,
          counts.mergers,
        );
      }
    });
    updateSectors();

    console.log(
      `\nUpdated ${Object.keys(finalSectorCounts).length} sector records`,
    );
  }

  // Step 6: Final state save
  state.decisionsIngested += decisionsIngested;
  state.mergersIngested += mergersIngested;
  saveState(state);

  // Step 7: Summary
  if (!dryRun && db) {
    const decisionCount = (
      db
        .prepare("SELECT count(*) as cnt FROM decisions")
        .get() as { cnt: number }
    ).cnt;
    const mergerCount = (
      db.prepare("SELECT count(*) as cnt FROM mergers").get() as {
        cnt: number;
      }
    ).cnt;
    const sectorCount = (
      db.prepare("SELECT count(*) as cnt FROM sectors").get() as {
        cnt: number;
      }
    ).cnt;

    console.log("\n=== Ingestion Complete ===");
    console.log(`  Decisions in DB:  ${decisionCount}`);
    console.log(`  Mergers in DB:    ${mergerCount}`);
    console.log(`  Sectors in DB:    ${sectorCount}`);
    console.log(`  New decisions:    ${decisionsIngested}`);
    console.log(`  New mergers:      ${mergersIngested}`);
    console.log(`  Errors:           ${errors}`);
    console.log(`  Skipped:          ${skipped}`);
    console.log(`  State saved to:   ${STATE_FILE}`);

    db.close();
  } else {
    console.log("\n=== Dry Run Complete ===");
    console.log(`  Decisions found:  ${decisionsIngested}`);
    console.log(`  Mergers found:    ${mergersIngested}`);
    console.log(`  Errors:           ${errors}`);
    console.log(`  Skipped:          ${skipped}`);
  }

  console.log(`\nDone.`);
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
