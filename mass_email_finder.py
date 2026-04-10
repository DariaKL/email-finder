"""
Mass Email Finder — Airtable Integration
==========================================
Масовий пошук email-адрес для доменів з Airtable.

Функціонал:
  - Працює з вкладками: Dasha, Anna, Mykola, Khrystia
  - Рівномірна обробка: 10 доменів з однієї вкладки → 10 з наступної (round-robin)
  - Прямий пошук на сайті (головна, Contact, About, Editorial, Team тощо)
  - Пріоритезація: editor, marketing, advertising, sales, admin, pr, webmaster, news
  - Мультимовна підтримка: EN, IT, DE, RO, PL, CZ
  - Google Search fallback: site:{domain} e-mail
  - Фільтрація junk-пошт: support@, help@, subscriptions@
  - Multi-threading, User-Agent rotation, Proxy support
  - Cloudflare email decryption

Використання:
  python mass_email_finder.py
  python mass_email_finder.py --no-google
  python mass_email_finder.py --proxy proxies.txt
  python mass_email_finder.py --filter-junk
  python mass_email_finder.py --workers 10
  python mass_email_finder.py --batch-size 5
"""

import gc
import re
import sys
import os
import time
import random
import logging
import threading
import argparse
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------------------------------------------------------------------------
# Dependency check
# ---------------------------------------------------------------------------
try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Встановіть залежності: pip install requests beautifulsoup4")
    print("Або: pip install -r requirements.txt")
    sys.exit(1)

try:
    from pyairtable import Api
except ImportError:
    print("Встановіть pyairtable: pip install pyairtable")
    sys.exit(1)

try:
    from googlesearch import search as _google_search
    GOOGLE_AVAILABLE = True
except ImportError:
    GOOGLE_AVAILABLE = False
    print("⚠ googlesearch-python не встановлено — Google fallback вимкнено.")
    print("  Встановіть: pip install googlesearch-python")

# ---------------------------------------------------------------------------
# .env support
# ---------------------------------------------------------------------------
def _load_env():
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and value:
                    os.environ.setdefault(key, value)

_load_env()

# ---------------------------------------------------------------------------
# Airtable Configuration (з .env або значення за замовчуванням)
# ---------------------------------------------------------------------------
AIRTABLE_TOKEN = os.environ.get("AIRTABLE_TOKEN", "")
AIRTABLE_BASE_ID = os.environ.get("AIRTABLE_BASE_ID", "")
TABLE_NAMES = ["Dasha", "Anna", "Mykola", "Khrystia"]
BATCH_SIZE = 10  # Скільки доменів обробляти з кожної вкладки за раунд

FIELD_DOMAIN = "Domain"
FIELD_EMAILS = "Emails"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("MassEmailFinder")

# ---------------------------------------------------------------------------
# User-Agent rotation
# ---------------------------------------------------------------------------
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.2365.80",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

# ---------------------------------------------------------------------------
# Email Regex & Filters
# ---------------------------------------------------------------------------
EMAIL_REGEX = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
    re.IGNORECASE,
)

FALSE_POSITIVE_EXTENSIONS = {
    ".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico",
    ".js", ".css", ".scss", ".less",
    ".woff", ".woff2", ".ttf", ".eot",
    ".mp4", ".mp3", ".avi", ".mov",
    ".zip", ".rar", ".gz", ".tar",
    ".pdf", ".doc", ".docx",
}

FALSE_POSITIVE_DOMAINS = {
    "example.com", "email.com", "domain.com", "yoursite.com",
    "yourdomain.com", "sentry.io", "wixpress.com",
    "webpack.js.org", "w3.org", "schema.org",
    "googleusercontent.com", "placeholder.com",
    "mysite.com", "yourwebsite.com", "company.com",
}

FALSE_POSITIVE_PREFIXES = {
    "noreply@", "no-reply@", "donotreply@", "mailer-daemon@", "postmaster-",
}

# Junk prefixes — пошти, які не мають цінності для outreach
JUNK_PREFIXES = {"support@", "help@", "helpdesk@", "ticket@", "billing@", "subscriptions@"}

# Irrelevant — автоматичні, рекрутингові, юридичні
IRRELEVANT_PREFIXES = {
    "subscribe", "subscriptions", "subscription", "unsubscribe",
    "newsletter", "newsletters", "digest",
    "careers", "career", "recruitment", "recruit", "hiring", "jobs", "job", "hr",
    "talent", "people", "apply",
    "legal", "compliance", "privacy", "gdpr", "dpo", "dmca", "copyright",
    "abuse", "spam", "phishing", "security",
    "noreply", "no-reply", "donotreply", "do-not-reply", "mailer-daemon",
    "postmaster", "bounce", "notifications", "notification", "alert", "alerts",
    "system", "automated", "auto", "daemon", "cron", "devops",
    "hostmaster",
    "accounting", "finance", "invoices", "invoice", "payments", "payment",
    "receipts", "receipt",
    "support", "help", "helpdesk", "ticket", "tickets", "billing",
    "customerservice", "customer-service", "customercare", "customer-care",
    "feedback", "survey", "surveys", "nps",
}

# High-value keywords — пріоритетні пошти
PRIORITY_KEYWORDS = [
    "editor", "editorial", "marketing", "advertising", "advert",
    "sales", "admin", "journal", "pr", "news",
    "press", "media", "content", "publish", "contributor",
    "guest", "submission", "pitch", "story", "article",
    "partner", "collaborate", "sponsored", "native", "webmaster",
    # DE
    "redaktion", "werbung", "anzeigen",
    # IT
    "redazione", "pubblicita",
    # PL
    "redakcja", "reklama",
    # RO
    "redactie", "publicitate",
    # CZ
    "redakce", "inzerce",
]

INFO_KEYWORDS = ["info", "contact", "office", "hello", "general", "enquir"]

# ---------------------------------------------------------------------------
# Contact paths — multi-language
# ---------------------------------------------------------------------------

# Tier 1: Основні сторінки (перевіряються завжди)
CONTACT_PATHS_TIER1 = [
    "/", "/contact", "/contact-us", "/about", "/about-us",
    "/advertise", "/advertising", "/editorial", "/team",
    "/sponsored-content", "/media-kit", "/write-for-us",
    "/guest-post", "/contribute", "/submit",
]

# Мовні шляхи
LANG_PATHS = {
    "de": ["/kontakt", "/uber-uns", "/ueber-uns", "/redaktion", "/werbung",
           "/impressum", "/mitarbeiter", "/mediadaten", "/gastbeitrag"],
    "it": ["/contatti", "/chi-siamo", "/redazione", "/pubblicita",
           "/collabora", "/scrivi-per-noi"],
    "pl": ["/kontakt", "/o-nas", "/redakcja", "/reklama",
           "/zespol", "/wspolpraca"],
    "ro": ["/contact", "/despre-noi", "/redactie", "/publicitate",
           "/echipa", "/colaborare"],
    "cs": ["/kontakt", "/o-nas", "/redakce", "/inzerce",
           "/tym", "/spoluprace"],
    "fr": ["/a-propos", "/equipe", "/publicite", "/redaction",
           "/contribuer", "/soumettre", "/partenariats"],
    "es": ["/contacto", "/sobre-nosotros", "/equipo", "/publicidad",
           "/redaccion", "/colaborar"],
    "nl": ["/over-ons", "/redactie", "/adverteren"],
    "pt": ["/contato", "/sobre-nos", "/publicidade"],
}

# Tier 2: Додаткові (перевіряються якщо в Tier 1 немає пріоритетних пошт)
CONTACT_PATHS_TIER2 = [
    "/contacts", "/contactus", "/aboutus", "/our-team",
    "/editorial-team", "/editors", "/staff",
    "/mediakit", "/sponsor", "/sponsors", "/sponsorship",
    "/pricing", "/rates", "/ad-rates", "/media-buying",
    "/imprint", "/impressum", "/masthead",
    "/authors", "/contributors", "/press", "/press-room",
    "/newsroom", "/partnerships", "/feedback", "/work-with-us",
    "/guest-posting", "/guest-blogging", "/submissions",
    "/submission-guidelines", "/contributor-guidelines",
    "/editorial-guidelines", "/collaborate",
    "/become-a-contributor", "/write-for-us-guidelines", "/pitch",
]

# Keywords для виявлення внутрішніх посилань на контактні сторінки
LINK_DISCOVERY_KEYWORDS = {
    "contact", "about", "team", "staff", "editorial", "editor",
    "advertis", "sponsor", "media-kit", "mediakit", "pricing",
    "partner", "press", "feedback", "work-with", "collaborate",
    "masthead", "imprint", "rates", "ad-rate", "media-buying",
    "newsroom", "author", "contributor",
    "write-for", "guest-post", "guest-blog", "contribute", "submit",
    "pitch", "submission", "guideline", "become-a",
    # Multi-language
    "kontakt", "redakti", "redakci", "redazion", "reklam",
    "publicite", "pubblicita", "publicidad", "publicidade", "publicitate",
    "iletisim", "uber-uns", "a-propos", "chi-siamo", "sobre",
    "equip", "werbung", "adverter", "inzerce", "spoluprace",
    "contatti", "o-nas", "despre", "echipa", "colabor",
}

PAGES_CONCURRENT = 10
REQUEST_TIMEOUT = 8
GOOGLE_DELAY = 3.0

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def random_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,de;q=0.8,it;q=0.7,pl;q=0.6,ro;q=0.5,cs;q=0.4,uk;q=0.3",
    }


def normalize_domain(raw: str) -> str:
    raw = raw.strip().rstrip("/")
    if not raw:
        return ""
    if "://" not in raw:
        raw = "https://" + raw
    parsed = urlparse(raw)
    domain = parsed.netloc or parsed.path
    domain = domain.split("/")[0].split(":")[0]
    return domain.lower()


def is_valid_email(email: str) -> bool:
    email_lower = email.lower()
    ext = "." + email_lower.rsplit(".", 1)[-1]
    if ext in FALSE_POSITIVE_EXTENSIONS:
        return False
    domain = email_lower.split("@")[1] if "@" in email_lower else ""
    if domain in FALSE_POSITIVE_DOMAINS:
        return False
    # Also check subdomains: e.g. sentry-next.wixpress.com
    for fp_domain in FALSE_POSITIVE_DOMAINS:
        if domain.endswith("." + fp_domain):
            return False
    for prefix in FALSE_POSITIVE_PREFIXES:
        if email_lower.startswith(prefix):
            return False
    local = email_lower.split("@")[0]
    if local in {"your", "user", "name", "email", "test"}:
        return False
    return True


def is_junk_email(email: str) -> bool:
    email_lower = email.lower()
    for prefix in JUNK_PREFIXES:
        if email_lower.startswith(prefix):
            return True
    return False


def is_irrelevant_email(email: str) -> bool:
    local = email.lower().split("@")[0]
    return local in IRRELEVANT_PREFIXES


def filter_irrelevant(emails: set) -> set:
    relevant = {e for e in emails if not is_irrelevant_email(e)}
    return relevant if relevant else emails


def extract_emails(text: str) -> set:
    raw = EMAIL_REGEX.findall(text)
    return {e.lower() for e in raw if is_valid_email(e)}


# Cloudflare email decryption
CF_EMAIL_ATTR_RE = re.compile(r'data-cfemail="([a-f0-9]+)"', re.IGNORECASE)
CF_EMAIL_URL_RE = re.compile(r'/cdn-cgi/l/email-protection#([a-f0-9]+)', re.IGNORECASE)


def _decode_cf_email(encoded: str) -> str:
    try:
        key = int(encoded[:2], 16)
        return "".join(chr(int(encoded[i:i+2], 16) ^ key) for i in range(2, len(encoded), 2))
    except (ValueError, IndexError):
        return ""


def decode_cf_emails(text: str) -> set:
    emails = set()
    for match in CF_EMAIL_ATTR_RE.finditer(text):
        decoded = _decode_cf_email(match.group(1))
        if decoded and EMAIL_REGEX.match(decoded) and is_valid_email(decoded):
            emails.add(decoded.lower())
    for match in CF_EMAIL_URL_RE.finditer(text):
        decoded = _decode_cf_email(match.group(1))
        if decoded and EMAIL_REGEX.match(decoded) and is_valid_email(decoded):
            emails.add(decoded.lower())
    return emails


def decode_mailto(soup: BeautifulSoup) -> set:
    emails = set()
    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if href.lower().startswith("mailto:"):
            addr = href[7:].split("?")[0].strip()
            if EMAIL_REGEX.match(addr) and is_valid_email(addr):
                emails.add(addr.lower())
    return emails


def prioritize_emails(emails: set) -> list:
    priority = []
    info_list = []
    general = []
    for e in emails:
        local = e.split("@")[0].lower()
        if any(kw in local for kw in PRIORITY_KEYWORDS):
            priority.append(e)
        elif any(kw in local for kw in INFO_KEYWORDS):
            info_list.append(e)
        else:
            general.append(e)
    return sorted(priority) + sorted(info_list) + sorted(general)


def read_proxy_list(filepath: str) -> list:
    proxies = []
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "://" not in line:
                    line = "http://" + line
                proxies.append(line)
    except FileNotFoundError:
        logger.warning(f"Файл проксі не знайдено: {filepath}")
    return proxies


# ---------------------------------------------------------------------------
# Scraping Engine
# ---------------------------------------------------------------------------

class EmailScraper:
    """Повнофункціональний скрапер з proxy rotation, UA rotation, CF decoding."""

    def __init__(self, proxies=None, use_google=True, filter_junk=False,
                 stop_event=None):
        self.proxies = proxies or []
        self.proxy_index = 0
        self.proxy_lock = threading.Lock()
        self.use_google = use_google
        self.filter_junk = filter_junk
        self._stop = stop_event or threading.Event()

    def stopped(self):
        return self._stop.is_set()

    def _get_proxy(self):
        if not self.proxies:
            return None
        with self.proxy_lock:
            proxy_url = self.proxies[self.proxy_index % len(self.proxies)]
            self.proxy_index += 1
        return {"http": proxy_url, "https": proxy_url}

    def _make_session(self):
        session = requests.Session()
        session.headers.update(random_headers())
        proxy = self._get_proxy()
        if proxy:
            session.proxies.update(proxy)
        return session

    def _fetch_page(self, session, url):
        try:
            resp = session.get(
                url,
                timeout=REQUEST_TIMEOUT,
                headers=random_headers(),
                allow_redirects=True,
            )
            if resp.status_code == 200 and "text/html" in resp.headers.get("Content-Type", ""):
                soup = BeautifulSoup(resp.text, "html.parser")
                return soup, resp.text
        except requests.Timeout:
            logger.error(f"TIMEOUT при запиті: {url}")
        except requests.RequestException as e:
            logger.error(f"RequestException для {url}: {e}")
        except Exception as e:
            logger.error(f"Невідома помилка для {url}: {e}")
        return None, ""

    def _discover_links(self, soup, domain):
        discovered = []
        if soup is None:
            return discovered
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"].strip()
            parsed = urlparse(href)
            link_host = (parsed.netloc or "").lower()
            if link_host and link_host.replace("www.", "") != domain.replace("www.", ""):
                continue
            path = parsed.path.rstrip("/").lower()
            if not path or path == "/":
                continue
            if any(kw in path for kw in LINK_DISCOVERY_KEYWORDS):
                full_url = urljoin(f"https://{domain}", parsed.path)
                if full_url not in discovered:
                    discovered.append(full_url)
        return discovered

    def _has_priority_emails(self, emails):
        for e in emails:
            local = e.split("@")[0].lower()
            if any(kw in local for kw in PRIORITY_KEYWORDS):
                return True
        return False

    def _fetch_batch(self, session, urls):
        results = []
        with ThreadPoolExecutor(max_workers=PAGES_CONCURRENT) as pool:
            future_map = {}
            for url in urls:
                if self.stopped():
                    break
                future_map[pool.submit(self._fetch_page, session, url)] = url
            for future in as_completed(future_map):
                if self.stopped():
                    break
                url = future_map[future]
                try:
                    soup, text = future.result(timeout=REQUEST_TIMEOUT + 5)
                except Exception:
                    soup, text = None, ""
                results.append((url, soup, text))
        return results

    def _extract_all(self, soup, text):
        emails = set()
        if text:
            emails |= extract_emails(text)
            emails |= decode_cf_emails(text)
        if soup:
            emails |= decode_mailto(soup)
            soup.decompose()  # Free DOM tree memory
        return emails

    def scrape_site(self, domain):
        session = self._make_session()
        try:
            return self._scrape_site_inner(session, domain)
        finally:
            session.close()

    def _scrape_site_inner(self, session, domain):
        all_emails = set()
        base_url = f"https://{domain}"
        visited = set()
        discovered = []

        # Phase 1: Homepage
        home_url = base_url + "/"
        visited.add(home_url)
        site_lang = ""
        soup, text = self._fetch_page(session, home_url)
        if soup:
            all_emails |= self._extract_all(soup, text)
            discovered = self._discover_links(soup, domain)
            html_tag = soup.find("html")
            if html_tag and html_tag.get("lang"):
                site_lang = html_tag["lang"].lower().split("-")[0].strip()

        # Phase 2: Tier 1 + language paths
        tier1_urls = []
        for path in CONTACT_PATHS_TIER1:
            url = urljoin(base_url, path)
            if url not in visited:
                tier1_urls.append(url)
                visited.add(url)

        if site_lang and site_lang != "en":
            lang_paths = LANG_PATHS.get(site_lang, [])
            for path in lang_paths:
                url = urljoin(base_url, path)
                if url not in visited:
                    tier1_urls.append(url)
                    visited.add(url)

        if tier1_urls and not self.stopped():
            for url, soup, text in self._fetch_batch(session, tier1_urls):
                all_emails |= self._extract_all(soup, text)

        if self._has_priority_emails(all_emails):
            return all_emails

        # Phase 3: Discovered links
        disc_urls = [u for u in discovered if u not in visited]
        for u in disc_urls:
            visited.add(u)

        if disc_urls and not self.stopped():
            for url, soup, text in self._fetch_batch(session, disc_urls):
                all_emails |= self._extract_all(soup, text)

        # Phase 4: Tier 2
        if not self._has_priority_emails(all_emails) and not self.stopped():
            tier2_urls = []
            for path in CONTACT_PATHS_TIER2:
                url = urljoin(base_url, path)
                if url not in visited:
                    tier2_urls.append(url)
                    visited.add(url)
            if tier2_urls:
                for url, soup, text in self._fetch_batch(session, tier2_urls):
                    all_emails |= self._extract_all(soup, text)

        # Phase 5: HTTP fallback
        if not all_emails and not self.stopped():
            base_http = f"http://{domain}"
            http_urls = []
            for path in ["/", "/contact", "/contact-us", "/about"]:
                url = urljoin(base_http, path)
                if url not in visited:
                    http_urls.append(url)
                    visited.add(url)
            if http_urls:
                for url, soup, text in self._fetch_batch(session, http_urls):
                    all_emails |= self._extract_all(soup, text)

        return all_emails

    def google_search_emails(self, domain):
        if not GOOGLE_AVAILABLE:
            return set()
        session = self._make_session()
        try:
            return self._google_search_inner(session, domain)
        finally:
            session.close()

    def _google_search_inner(self, session, domain):
        emails = set()
        queries = [
            f"site:{domain} e-mail",
            f"site:{domain} contact email",
            f'"{domain}" email OR mail OR contact',
        ]

        visited = set()

        for query in queries:
            if self.stopped():
                break
            try:
                results = list(_google_search(query, num_results=5, lang="en"))
                urls_to_fetch = [u for u in results if u not in visited]
                for u in urls_to_fetch:
                    visited.add(u)
                if urls_to_fetch and not self.stopped():
                    for url, soup, text in self._fetch_batch(session, urls_to_fetch):
                        emails |= self._extract_all(soup, text)
            except Exception as e:
                logger.debug(f"Google search failed for '{query}': {e}")
            time.sleep(GOOGLE_DELAY)

        return emails

    def process_domain(self, domain):
        if self.stopped():
            return {"domain": domain, "emails": [], "status": "stopped"}

        # Step 1: Direct site scraping
        emails = self.scrape_site(domain)

        # Step 2: Filter irrelevant
        emails = filter_irrelevant(emails)

        # Step 3: Filter junk if enabled
        junk_only = False
        if self.filter_junk:
            non_junk = {e for e in emails if not is_junk_email(e)}
            junk_only = len(non_junk) == 0 and len(emails) > 0
            emails = non_junk

        # Step 4: Google fallback
        google_used = False
        if not emails and self.use_google and not self.stopped():
            logger.info(f"    ↳ Google fallback для {domain}...")
            google_emails = self.google_search_emails(domain)
            google_emails = filter_irrelevant(google_emails)
            if self.filter_junk:
                google_emails = {e for e in google_emails if not is_junk_email(e)}
            emails |= google_emails
            if google_emails:
                google_used = True

        sorted_emails = prioritize_emails(emails)

        if sorted_emails:
            status = "found"
        elif junk_only:
            status = "not found"
        else:
            status = "not found"

        return {
            "domain": domain,
            "emails": sorted_emails,
            "status": status,
            "google_used": google_used,
        }


# ---------------------------------------------------------------------------
# Airtable Integration
# ---------------------------------------------------------------------------

def fetch_pending_records(table):
    """Отримує записи де Domain заповнено, а Emails — порожнє."""
    try:
        pending = table.all(formula="AND({Domain} != '', {Emails} = '')")
        return pending
    except Exception as e:
        logger.error(f"Помилка читання Airtable: {e}")
        return []


def batch_update_airtable(table, updates):
    """Оновлює записи в Airtable пачками по 10."""
    for i in range(0, len(updates), 10):
        batch = updates[i:i+10]
        try:
            table.batch_update([
                {"id": rid, "fields": {FIELD_EMAILS: val}}
                for rid, val in batch
            ])
        except Exception:
            # Fallback: по одному запису
            for rid, val in batch:
                try:
                    table.update(rid, {FIELD_EMAILS: val})
                except Exception as e:
                    logger.error(f"  Не вдалося оновити {rid}: {e}")


# ---------------------------------------------------------------------------
# Round-Robin Processing
# ---------------------------------------------------------------------------

def process_round_robin(scraper, api, batch_size, workers, stop_event):
    """
    Обробляє домени рівномірно по всіх вкладках: 
    batch_size з першої → batch_size з другої → ... → повтор.
    """
    tables = {}
    for name in TABLE_NAMES:
        tables[name] = api.table(AIRTABLE_BASE_ID, name)

    round_num = 0


    # New logic: always process 1-20 per tab, switch tab after each batch, loop until all tabs empty
    tab_count = len(TABLE_NAMES)
    tab_idx = 0
    round_num = 0
    while not stop_event.is_set():
        all_pending = 0
        for i in range(tab_count):
            tab_name = TABLE_NAMES[tab_idx]
            tab_idx = (tab_idx + 1) % tab_count
            table = tables[tab_name]
            pending = fetch_pending_records(table)
            n_pending = len(pending)
            all_pending += n_pending
            if n_pending == 0:
                logger.info(f"[{tab_name}] Немає нових донорів.")
                continue
            batch_n = min(max(1, n_pending), 20)
            batch = pending[:batch_n]
            logger.info(f"[{tab_name}] Всього донорів: {n_pending}. Взято в роботу: {len(batch)}.")
            results = []
            def _process_one(record):
                fields = record.get("fields", {})
                raw_domain = fields.get(FIELD_DOMAIN, "").strip()
                record_id = record["id"]
                if not raw_domain:
                    return None
                domain = normalize_domain(raw_domain)
                if not domain:
                    return (record_id, "not found")
                try:
                    logger.info(f"[{tab_name}] Починаю: {domain}")
                    result = scraper.process_domain(domain)
                    emails = result.get("emails", [])
                    email_str = ", ".join(emails) if emails else "not found"
                    logger.info(f"[{tab_name}] {domain}: {email_str}")
                    return (record_id, email_str)
                except Exception as e:
                    logger.error(f"[{tab_name}] {domain}: помилка — {e}")
                    return (record_id, "not found")
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {executor.submit(_process_one, rec): rec for rec in batch}
                for future in as_completed(futures):
                    if stop_event.is_set():
                        break
                    try:
                        res = future.result(timeout=15)
                        if res:
                            results.append(res)
                    except Exception as e:
                        rec = futures[future]
                        fields = rec.get("fields", {})
                        raw_domain = fields.get(FIELD_DOMAIN, "").strip()
                        domain = normalize_domain(raw_domain)
                        logger.error(f"[{tab_name}] {domain}: TIMEOUT (15s)")
                        results.append((rec["id"], "not found"))
            if results:
                batch_update_airtable(table, results)
            logger.info(f"[{tab_name}] Записано: {len(results)}")
            break
        if all_pending == 0:
            gc.collect()
            logger.info("\nВсі вкладки оброблено. Очікування 10 сек...")
            if stop_event.wait(timeout=10):
                break
        else:
            gc.collect()
            if stop_event.wait(timeout=1):
                break


def process_once(scraper, api, batch_size, workers):
    """Одноразовий прогін всіх вкладок."""
    tables = {}
    for name in TABLE_NAMES:
        tables[name] = api.table(AIRTABLE_BASE_ID, name)

    total = 0

    for tab_name in TABLE_NAMES:
        table = tables[tab_name]
        pending = fetch_pending_records(table)

        if not pending:
            logger.info(f"[{tab_name}] Немає нових доменів.")
            continue

        batch = pending[:batch_size]
        logger.info(f"[{tab_name}] Обробка {len(batch)} з {len(pending)} доменів...")

        results = []

        def _process_one(record):
            fields = record.get("fields", {})
            raw_domain = fields.get(FIELD_DOMAIN, "").strip()
            record_id = record["id"]
            if not raw_domain:
                return None
            domain = normalize_domain(raw_domain)
            if not domain:
                return (record_id, "not found")
            try:
                result = scraper.process_domain(domain)
                emails = result.get("emails", [])
                email_str = ", ".join(emails) if emails else "not found"
                google_mark = " [G]" if result.get("google_used") else ""
                logger.info(f"  [{tab_name}] {domain}: {email_str}{google_mark}")
                return (record_id, email_str)
            except Exception as e:
                logger.error(f"  [{tab_name}] {domain}: помилка — {e}")
                return (record_id, "not found")

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_process_one, rec): rec for rec in batch}
            for future in as_completed(futures):
                try:
                    res = future.result(timeout=15)
                    if res:
                        results.append(res)
                except Exception as e:
                    rec = futures[future]
                    fields = rec.get("fields", {})
                    raw_domain = fields.get(FIELD_DOMAIN, "").strip()
                    domain = normalize_domain(raw_domain)
                    logger.error(f"  [{tab_name}] {domain}: TIMEOUT (15s)")
                    results.append((rec["id"], "not found"))

        if results:
            batch_update_airtable(table, results)
            total += len(results)
        logger.info(f"[{tab_name}] ✓ Записано: {len(results)}")

    logger.info(f"\nВсього оброблено: {total} доменів.")
    return total


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Mass Email Finder — пошук email для доменів з Airtable"
    )
    parser.add_argument(
        "--once", action="store_true",
        help="Одноразовий прогін (без режиму loop)",
    )
    parser.add_argument(
        "--no-google", action="store_true",
        help="Вимкнути Google Search fallback",
    )
    parser.add_argument(
        "--filter-junk", action="store_true",
        help="Фільтрувати support@, help@, subscriptions@ (ставити not found)",
    )
    parser.add_argument(
        "--proxy", default=None,
        help="Шлях до файлу з проксі (один на рядок: ip:port або protocol://ip:port)",
    )
    parser.add_argument(
        "--workers", type=int, default=5,
        help="Кількість паралельних потоків (за замовчуванням: 5)",
    )
    parser.add_argument(
        "--batch-size", type=int, default=10,
        help="Скільки доменів обробляти з кожної вкладки за раунд (за замовчуванням: 10)",
    )
    parser.add_argument(
        "--tables", nargs="+", default=None,
        help="Список вкладок (за замовчуванням: Dasha Anna Mykola Khrystia)",
    )

    args = parser.parse_args()

    # Override tables if specified
    global TABLE_NAMES
    if args.tables:
        TABLE_NAMES = args.tables

    # Load proxies
    proxies = []
    if args.proxy:
        proxies = read_proxy_list(args.proxy)
        if proxies:
            logger.info(f"Завантажено {len(proxies)} проксі.")
        else:
            logger.warning("Проксі не знайдено у файлі.")

    # Check for proxies.txt in script directory
    if not proxies:
        default_proxy = os.path.join(os.path.dirname(os.path.abspath(__file__)), "proxies.txt")
        if os.path.exists(default_proxy):
            proxies = read_proxy_list(default_proxy)
            if proxies:
                logger.info(f"Автоматично завантажено {len(proxies)} проксі з proxies.txt")

    use_google = not args.no_google and GOOGLE_AVAILABLE

    stop_event = threading.Event()

    scraper = EmailScraper(
        proxies=proxies,
        use_google=use_google,
        filter_junk=args.filter_junk,
        stop_event=stop_event,
    )

    api = Api(AIRTABLE_TOKEN)

    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║          MASS EMAIL FINDER — AIRTABLE                   ║")
    print("╠══════════════════════════════════════════════════════════╣")
    print(f"║  Вкладки:     {', '.join(TABLE_NAMES):<42}║")
    print(f"║  Batch:       1-20 доменів/вкладка{' ':<22}║")
    print(f"║  Потоків:     {args.workers:<43}║")
    print(f"║  Google:      {'✓ увімк.' if use_google else '✗ вимк.':<43}║")
    print(f"║  Junk фільтр: {'✓ увімк.' if args.filter_junk else '✗ вимк.':<43}║")
    print(f"║  Проксі:      {len(proxies) if proxies else 'немає':<43}║")
    print(f"║  Режим:       {'одноразовий' if args.once else 'безперервний (Ctrl+C для зупинки)':<43}║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()

    if args.once:
        process_once(scraper, api, args.batch_size, args.workers)
    else:
        try:
            process_round_robin(scraper, api, args.batch_size, args.workers, stop_event)
        except KeyboardInterrupt:
            logger.info("\n⏹ Зупинка... Очікуйте завершення поточних задач.")
            stop_event.set()
            time.sleep(2)
            logger.info("✓ Завершено.")


if __name__ == "__main__":
    main()
