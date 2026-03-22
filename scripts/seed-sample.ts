/**
 * Seed the AVK (Agencija za varstvo konkurence — Slovenian Competition Protection Agency) database.
 * Usage: npx tsx scripts/seed-sample.ts [--force]
 */
import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

const DB_PATH = process.env["AVK_DB_PATH"] ?? "data/avk.db";
const force = process.argv.includes("--force");
const dir = dirname(DB_PATH);
if (!existsSync(dir)) { mkdirSync(dir, { recursive: true }); }
if (force && existsSync(DB_PATH)) { unlinkSync(DB_PATH); console.log(`Deleted ${DB_PATH}`); }
const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");
db.exec(SCHEMA_SQL);
console.log(`Database initialised at ${DB_PATH}`);

// --- Sectors ---
const sectors = [
  { id: "energy", name: "Energetika", name_en: "Energy", description: "Elektroenergetika, zemeljski plin, daljinsko ogrevanje in naftni derivati.", decision_count: 2, merger_count: 1 },
  { id: "retail", name: "Maloprodaja", name_en: "Retail", description: "Prehrambena maloprodaja, nakupovalna sredicca in posrednistvi.", decision_count: 2, merger_count: 1 },
  { id: "telecommunications", name: "Telekomunikacije", name_en: "Telecommunications", description: "Mobilne komunikacije, sirokokvalni internet, kabelska televizija.", decision_count: 1, merger_count: 1 },
  { id: "pharmaceuticals", name: "Farmacija", name_en: "Pharmaceuticals", description: "Farmacevtska distribucija, lekarni in medicinsko-tehnicni pripomocki.", decision_count: 1, merger_count: 0 },
  { id: "construction", name: "Gradbenistvo", name_en: "Construction", description: "Gradbeni material, nepremicninski razvoj in javno narocanje.", decision_count: 1, merger_count: 0 },
];
const insS = db.prepare("INSERT OR IGNORE INTO sectors (id, name, name_en, description, decision_count, merger_count) VALUES (?, ?, ?, ?, ?, ?)");
for (const s of sectors) insS.run(s.id, s.name, s.name_en, s.description, s.decision_count, s.merger_count);
console.log(`Inserted ${sectors.length} sectors`);

// --- Decisions ---
const decisions = [
  {
    case_number: "306-35/2023", title: "Mercator d.d. — Zloraba prevladujocega polozaja v maloprodaji",
    date: "2023-06-20", type: "abuse_of_dominance", sector: "retail",
    parties: JSON.stringify(["Mercator d.d."]),
    summary: "AVK je ugotovila, da je Mercator d.d. zlorabljal prevladujoci polozaj v maloprodajnem sektorju z diskriminatornimi pogoji dobave za dolocene kategorije dobaviteljev.",
    full_text: "Agencija za varstvo konkurence je po pritozbi vecih dobaviteljev zacela preiskavo zoper Mercator d.d. Mercator je z veckratnim prevzetom stevilnih maloprodajnih verig dosegel priblizno 30% trzni delez v slovenskem prehrambenem maloprodajnem sektorju. Ugotovitve postopka: (1) Diskriminatorni pogoji dobave — Mercator je dolocenim kategorijam dobaviteljev nalozel neenake lstribucijske marze in pogoje placila; (2) Retroaktivne zahteve — Mercator je naknadno zahteval doplacila in bonuse, ki niso bili dogovorjeni ob sklenitvi pogodbe; (3) Odvracanje od alternativnih kanalov — dobaviteljem, ki so blago prodajali prek konkurencnih maloprodajnih omrezij, so bile naknadno znizane maloprodajne police. AVK je ugotovila krsitev 9. clena ZPOmK-2 (enakovrednost clenu 102 PDEU) in Mercatorju izrekla globo v visini 2,8 milijona EUR.",
    outcome: "fine", fine_amount: 2_800_000, gwb_articles: JSON.stringify(["ZPOmK-2 clen 9"]), status: "appealed",
  },
  {
    case_number: "306-12/2022", title: "Elektro Ljubljana — Omejevanje dostopa do distribucijskega omrezja",
    date: "2022-08-10", type: "abuse_of_dominance", sector: "energy",
    parties: JSON.stringify(["Elektro Ljubljana d.d."]),
    summary: "Preiskava glede pogojev priklopa obnovljivih virov energije na distribucijsko omrezje. AVK je ugotovila diskriminatorno obravnavo pri podeljevanju priklopnih dovoljenj.",
    full_text: "Agencija za varstvo konkurence je preiskala pritozbe ponudnikov soncne energije zoper Elektro Ljubljana d.d., ki upravlja distribucijsko elektricno omrezje v Osrednjeslovenski regiji. Elektro Ljubljana je regionalni distribucijski operater brez konkurencnega nadomestila. Ugotovitve: (1) Nepravicne zamude — pridobitev priklopnega dovoljenja za soncne elektrarne je v povprecju trajala 14 mesecev v primerjavi z 4 meseci za primerljive intalacije v sosednjih regijah; (2) Diskriminatorni tehnicki pogoji — Elektro Ljubljana je nekaterim soncnim elektrarnam nalozila nepotrebne zahteve po tehnicnih posodobitvah, ki niso bile zahtevane za primerljive intalacije; (3) Informacijska asimetrija — podatki o kapaciteti omrezja niso bili javno dostopni. AVK je izdala odlocbo o opustitvi krsitve in predpisala standardizirane postopke za obdelavo zahtev za priklop.",
    outcome: "prohibited", fine_amount: null, gwb_articles: JSON.stringify(["ZPOmK-2 clen 9", "Energetski zakon"]), status: "final",
  },
  {
    case_number: "306-28/2022", title: "Gradbeni kartel — Javni razpisi za cestno infrastrukturo",
    date: "2022-11-15", type: "cartel", sector: "construction",
    parties: JSON.stringify(["Primorje d.d.", "CGP d.d.", "Gorenjska gradbena druzba d.d."]),
    summary: "AVK je ugotovila kartelni sporazum med tremi gradbenimi podjetji pri javnih razpisih za gradnjo drzavnih cest v obdobju 2018-2022.",
    full_text: "Agencija za varstvo konkurence je po prijavi enega od udelezencev programa prizanesljivosti zacela preiskavo domnevnega usklajevanja ponudb pri javnih razpisih za gradnjo drzavnih cest. Preiskava je zajela 12 javnih razpisov v vrednosti skupaj priblizno 180 milijonov EUR. Ugotovitve: (1) Rotacija razpisov — podjetja so se vnaprej dogovorila, katero podjetje bo nizalo ponudbo in pridobilo razpis; (2) Koordinacija ponudb — odzivajoca podjetja so nameskcala visje namerno povecane ponudbe, da bi podprla predhodno dogovorjenega zmagovalca; (3) Kompenzacijski mehanizem — podjetja so se med seboj kompenzirala z subkontraktiranjem dela na projektih, ki jih ni pridobilo. Skupna globa: 4,1 milijona EUR (Primorje 2,1 mio, CGP 1,2 mio, Gorenjska gradbena druzba 0,8 mio). Primorje je prejel 40% znianje globe v okviru programa prizanesljivosti.",
    outcome: "fine", fine_amount: 4_100_000, gwb_articles: JSON.stringify(["ZPOmK-2 clen 6"]), status: "final",
  },
  {
    case_number: "306-04/2023", title: "Telekom Slovenije — Snopicev ucinki na trgu poslovnih komunikacij",
    date: "2023-03-30", type: "abuse_of_dominance", sector: "telecommunications",
    parties: JSON.stringify(["Telekom Slovenije d.d."]),
    summary: "AVK je preiskala prakse vezanja storitev Telekoma Slovenije na trgu poslovnih komunikacij, kjer je ugotovila, da vezanje MPLS in glasovnih storitev krsi ZPOmK-2.",
    full_text: "Agencija za varstvo konkurence je preiskala taktike vezanja storitev Telekoma Slovenije na trgu poslovnih komunikacij za srednje in velike podjetja. Telekom Slovenije ima na tem segmentu priblizno 55% trzni delez. Ugotovitve: (1) Vezanje MPLS in glasovnih storitev — Telekom je zahteval, da stranke, ki zelijo pridobiti najugodnejse cene za MPLS, zakupijo tudi glasovne storitve pri Telekomu; (2) Ekskluzivne pogodbe — podzboritev vec storitev je bila pogojena s triletno ekskluzivnostjo za celoten portfelj; (3) Verz cenovnega stiskanja — cene za ene storitve posredno diskriminirajo stranke, ki zelijo pridobiti le posamezne storitve. AVK je Telekomu narocila ukinitev vezanih ponudb in ekskluzivnih pogodbenih pogojev ter mu nalozila globo 1,4 milijona EUR.",
    outcome: "fine", fine_amount: 1_400_000, gwb_articles: JSON.stringify(["ZPOmK-2 clen 9(2)"]), status: "final",
  },
  {
    case_number: "306-17/2022", title: "Farmacevtska distribucija — Sektorska preiskava lekarniskega trga",
    date: "2022-04-25", type: "sector_inquiry", sector: "pharmaceuticals",
    parties: JSON.stringify(["Salus d.o.o.", "Kemofarmacija d.o.o.", "Phoenix farmacija d.o.o."]),
    summary: "Sektorska preiskava AVK na trgu farmacevtske distribucije. Agencija je ugotovila oligopolno trzno strukturo, a nezadostne dokaze za formalni kartelni postopek.",
    full_text: "Agencija za varstvo konkurence je izvedla sektorsko preiskavo trga veletrgine distribucije farmacevtskih izdelkov. Trg je visoko koncentriran: trije distributerji skupaj pokrivajo priblizno 95% dobave lekarnam. Ugotovitve: (1) Cenovni vzorci — distributerji redko tekmujejo na cenah za istovrstne zdravilne ucinkovine; (2) Izmenjava informacij — prek panoznih zdruzenj prihaja do izmenjave podatkov o trznih delezih in pogojih dobave; (3) Neoddane lokacije — ponudba lekarniskkiv distribucij je geografsko razdeljena brez vidnega tekmovanja. Agencija ni sprouzila formalnega postopka za krsitev zakonodaje, temvec je izdala priporocila za izboljsanje trzne transparentnosti in spodbudila vstop novih akterjev s poenostavitvijo pogojev licenciranja.",
    outcome: "cleared", fine_amount: null, gwb_articles: JSON.stringify(["ZPOmK-2 clen 25"]), status: "final",
  },
];

const insD = db.prepare("INSERT OR IGNORE INTO decisions (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, gwb_articles, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
const insDAll = db.transaction(() => { for (const d of decisions) insD.run(d.case_number, d.title, d.date, d.type, d.sector, d.parties, d.summary, d.full_text, d.outcome, d.fine_amount, d.gwb_articles, d.status); });
insDAll();
console.log(`Inserted ${decisions.length} decisions`);

// --- Mergers ---
const mergers = [
  {
    case_number: "306-K-1/2023", title: "GEN-I / Petrol Energija — Prevzem energetskega prodajnega portfelja",
    date: "2023-09-15", sector: "energy", acquiring_party: "GEN-I d.o.o.", target: "Petrol Energija d.o.o.",
    summary: "AVK je z razpisnimi pogoji odobrila prevzem energetskega prodajnega portfelja druzbe Petrol Energija s strani GEN-I. Agencija je zahtevala prenos dela poslovnih strank na tretjo stranko.",
    full_text: "GEN-I d.o.o. je predlagal prevzem celotnega energetskega prodajnega portfelja Petrol Energija d.o.o., ki obsega priblizno 45.000 poslovnih in 120.000 gospodinjskih odjemalcev elektrike in zemeljskega plina. GEN-I ze ima priblizno 20% trzni delez pri poslovnih odjemalcih v Sloveniji. Ocena AVK: (1) Poslovni odjemalci — kombiniran trzni delez bi dosegel 35% pri dobavi elektrike malim in srednje velikim podjetjem; (2) Gospodinjski odjemalci — trzni delezi ostajajo pod kriticknimi pragovi; (3) Regionalna koncentracija — v Gorenjski regiji bi kombiniran delez pri poslovnih odjemalcih dosegel 45%. Pogoj: GEN-I mora v 18 mesecih prenesti pogodbe s priblizno 5.000 poslovnimi odjemalci v Gorenjski regiji na tretjo stranko.",
    outcome: "cleared_with_conditions", turnover: 2_100_000_000,
  },
  {
    case_number: "306-K-3/2022", title: "Mercator / Engrotuš — Prevzem 12 prodajaln",
    date: "2022-06-10", sector: "retail", acquiring_party: "Mercator d.d.", target: "Engrotuš d.d. (12 prodajaln)",
    summary: "AVK je odobrila prevzem 12 prodajaln Engrotusa s strani Mercatorja z zahtevo po odsvojitvi treh prodajaln v obmocjih, kjer bi koncentracija ogrozila konkurenco.",
    full_text: "Mercator d.d. je predlagal prevzem 12 prodajaln verige Engrotuš, ki se je znasla v financnih tezavah. AVK je izvedla obsezno analizo lokalnih trgovskih obmocij: (1) Regionalna analiza — v vecini obmocij Mercator in Engrotuš nista bila neposredna konkurenta, ker so bila njuna prodajalna medsebojno oddaljena; (2) Tri kriticna obmocja — v Kranju, Celju in Velenju bi kombiniran deleze presegel 40% z omejenim stevilom alternativ; (3) Strukturni pogoj — Mercator mora prodati tri prodajalne v teh obmocjih nevpleteni stranki v 12 mesecih, da se vzpostavi ali ohrani konkurencni alternativni ponudnik. Prevzem je bil sicer odobren, ker bi propad Engrotusa brez prevzema zmanjsal konkurenco se bolj.",
    outcome: "cleared_with_conditions", turnover: 1_800_000_000,
  },
  {
    case_number: "306-K-2/2023", title: "A1 Slovenija / Telemach — Skupno podjetje za pasivno infrastrukturo",
    date: "2023-07-20", sector: "telecommunications", acquiring_party: "A1 Slovenija d.d.", target: "Telemach d.o.o. (infrastrukturno skupno podjetje)",
    summary: "AVK je odobrila skupno podjetje A1 in Telecamha za skupno lastnistvo pasivne telekomunikacijske infrastrukture (stolpi, mastel). Skupno podjetje ne obsega aktivnih omreznih komponent.",
    full_text: "A1 Slovenija in Telemach sta predlagala ustanovitev skupnega podjetja za skupno lastnistvo in upravljanje pasivne telekomunikacijske infrastrukture — stolpov, mastelov in antenih stavb. Skupno podjetje bi pokrivalo priblizno 2.200 lokacij po vsej Sloveniji. Ocena AVK: (1) Obseg skupnega podjetja — omejeno na pasivno infrastrukturo; vsak operater ohrani lastne aktivne omrezne komponente in postopke odlocanja glede omrezne politike; (2) Ucinek na trg — skupno podjetje ne zmanjsuje sposobnosti ali spodbude A1 in Telecamha za konkurencno tekmovanje pri mobilnih in sirokokovnih storitvah; (3) Ekskuzivnost — skupno podjetje mora tretjim operaterjem ponuditi dostop do stolpov pod nediskriminatornimi pogoji. AVK je skupno podjetje odobrila v fazi 1 brez pogojev, z opazko o dostopnosti tretjim operaterjem.",
    outcome: "cleared_phase1", turnover: 900_000_000,
  },
];

const insM = db.prepare("INSERT OR IGNORE INTO mergers (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
const insMAll = db.transaction(() => { for (const m of mergers) insM.run(m.case_number, m.title, m.date, m.sector, m.acquiring_party, m.target, m.summary, m.full_text, m.outcome, m.turnover); });
insMAll();
console.log(`Inserted ${mergers.length} mergers`);

const dCnt = (db.prepare("SELECT count(*) as cnt FROM decisions").get() as { cnt: number }).cnt;
const mCnt = (db.prepare("SELECT count(*) as cnt FROM mergers").get() as { cnt: number }).cnt;
const sCnt = (db.prepare("SELECT count(*) as cnt FROM sectors").get() as { cnt: number }).cnt;
console.log(`\nSummary: ${sCnt} sectors, ${dCnt} decisions, ${mCnt} mergers`);
console.log(`Done. Database ready at ${DB_PATH}`);
db.close();
