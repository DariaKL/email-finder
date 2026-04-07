#!/usr/bin/env python3
# ============================================================
# Email Finder API вЂ” Python (Flask/standalone)
# Converted from Cloudflare Worker JS
# ============================================================
# Usage standalone: python email_finder.py example.com
# Usage as server:  python email_finder.py --serve --port 8080
# ============================================================

import re
import time
import asyncio
import smtplib
import socket
import aiohttp
from urllib.parse import urlparse, urljoin

# в”Ђв”Ђ CONSTANTS в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ

MAX_HTML = 250_000
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}', re.IGNORECASE)

TIER1 = [
    'advertising','advertise','advertorial',
    'publicitate','publicite','publicidad','pubblicita','publicidade','publicitat',
    'reklama','reklaam','reklame','werbung','anzeige',
    'annons','annonsera','annonse','annonce',
    'hirdetes','hirdetmeny','mainonta','ilmoitus',
    'advertentie','adverteren','inzerce','inzercia',
    'oglasavanje','oglas','ilan','diafimisi',
    'editorial','redaktion','redaction','redakcja','redakce','redakcia',
    'redakcija','redakciya','redactie','redactia','redazione','redaccion','redacao',
    'szerkesztoseg','toimitus','redaksjon','editoryal','yayin',
    'press','presse','prensa','stampa','imprensa','presa','prasa','pers',
    'sajto','lehdisto','tisk','tlac','tisak','basin','spauda','prese',
    'commercial','commerciale','comercial','kommerziell','handels','commercieel',
    'sales','verkauf','vertrieb','ventes','ventas','vendite','vendas','verkoop',
    'sprzedaz','vanzari','ertekesites','myynti','forsaljning',
    'salg','prodej','predaj','prodaja','satis','pardavimai',
    'marketing',
    'partnership','collab','cooperation','sponsored',
    'partnerschaft','kooperation','zusammenarbeit',
    'partenariat','collaboration','colaboracion','collaborazione',
    'parceria','colaboracao','samenwerking','partnerschap',
    'wspolpraca','parteneriat','colaborare','egyuttmukodes',
    'yhteistyo','samarbete','samarbeid','samarbejde','spolupraca','suradnja',
    'mediakit','media-kit','pitch','outreach',
    'webmaster','administrator',
    'pr',
]

TIER2 = [
    'editor','redakteur','herausgeber','editeur','redacteur','redactor',
    'editore','redattore','redator','redaktor','szerkeszto',
    'toimittaja','paatoimittaja','urednik',
    'newsroom','newsdesk','managingeditor','senioreditor','chiefeditor',
    'news','noticias','notizie','actualites','nachrichten','nieuws',
    'wiadomosci','stiri','hirek','uutiset','nyheter','nyheder',
    'zpravy','spravy','vijesti','haberler',
    'journalist','journaliste','periodista','giornalista','jornalista',
    'dziennikarz','jurnalist','ziarist','ujsagiro','novinari','novinar','gazeteci',
    'publish','publishing','publisher','verlag','publication',
    'publicacion','pubblicazione','publicacao','publicatie','uitgeverij',
    'publikacja','wydawnictwo','publicare','editura',
    'desk','submission','submit','contribute','contributor',
    'einreichung','soumission',
    'author','writers','writer','autor','auteur','autore','szerzo',
    'kirjoittaja','forfattare',
    'content','inhalt','contenu','contenido','contenuto',
    'media','communications','kommunikation',
    'comunicazione','comunicacion','comunicacao','communicatie',
    'komunikacja','comunicare','kozlemenyek','viestinta',
    'guest','guest-post','gastbeitrag','invite','artikel-gast',
    'ads',
]

GENERIC = [
    'info','contact','general','enquiries','enquiry','admin','sales','marketing',
    'kontakt','contatto','contatti','contacto','contato',
    'kapcsolat','yhteys','yhteystiedot','iletisim','kontakty',
    'anfrage','renseignements','informacion','informazioni',
    'informacao','informatie','informacja','tiedustelut',
    'allgemein','generale','algemeenheden',
]

BLACKLIST = [
    'support','help','helpdesk','ticket',
    'hilfe','kundenservice','aide','assistance','ayuda','soporte',
    'aiuto','supporto','assistenza','ajuda','suporte',
    'hulp','ondersteuning','pomoc','wsparcie',
    'segitseg','ugyfelszolgalat','tuki','asiakaspalvelu','destek',
    'subscribe','unsubscribe','newsletter','abonnement','abonneren',
    'nieuwsbrief','biuletyn','hirlevel','uutiskirje','nyhetsbrev',
    'noreply','no-reply','no_reply','donotreply','do-not-reply',
    'mailer-daemon','postmaster','bounce','abuse','spam',
    'signup','sign-up','register','confirm','verification','verify',
    'notification','alert','digest','welcome','onboarding',
    'automated','auto','system','bot','daemon','cron',
    'test','dev','staging','debug','example','demo','sample',
    'nobody','null','void','root','sysadmin','gdpr','dpo',
    'compliance','cookie',
    'billing','invoice','payment','order','receipt',
    'rechnung','zahlung','bestellung','facture','paiement','commande',
    'factura','pago','pedido','fattura','pagamento','ordine',
    'fatura','encomenda','factuur','betaling','bestelling',
    'faktura','platnosc','zamowienie','szamla','fizetes','rendeles',
    'lasku','maksu','tilaus',
    'hr','careers','jobs','recruitment','hiring','vacancy',
    'karriere','stellenangebot','carrieres','emploi','recrutement',
    'empleo','vacante','lavoro','carriere','emprego','vagas',
    'vacature','werken-bij','praca','kariera','rekrutacja',
    'allasok','karrier','tyopaikat','rekrytointi',
    'jobb','karriar','lediga-tjanster','stilling',
    'informatyk','nwsp',
    'accounting','finance','legal','security','privacy',
    'buchhaltung','finanzen','recht','comptabilite','juridique',
    'contabilidad','juridico','contabilita','legale','contabilidade',
    'boekhouding','financien','juridisch',
    'ksiegowosc','finanse','prawny','konyveles','penzugy','jogi',
    'kirjanpito','talous','lakiasiat',
    'it','tech',
    'edu','education','training','school','university','student',
    'ausbildung','schule','formation','ecole','formacion','escuela',
    'shop','store','ecommerce','webshop','boutique','tienda',
    'negozio','sklep','magazin',
]

THIRD_PARTY = [
    'wordpress.org','wordpress.com','google.com','googleapis.com',
    'cloudflare.com','github.com','sentry.io','wixpress.com',
    'w3.org','schema.org','facebook.com','twitter.com','x.com',
    'instagram.com','linkedin.com','youtube.com','apple.com',
    'microsoft.com','mozilla.org','jquery.com','bootstrapcdn.com',
    'gravatar.com','disqus.com','akismet.com','automattic.com',
    'amazonaws.com','mailchimp.com','sendgrid.net','mailgun.org',
    'example.com','example.org','example.net','test.com',
    'creativecommons.org','ogp.me','wix.com','gstatic.com',
    'googleusercontent.com','fbcdn.net','twimg.com',
    'duckduckgo.com','bing.com','yahoo.com','yandex.ru',
    'baidu.com','wrightsmedia.com',
]

BAD_RE = [
    re.compile(r'\.(png|jpg|jpeg|gif|svg|webp|bmp|ico)$', re.I),
    re.compile(r'\.(css|js|html|htm|php|asp|json|xml)$', re.I),
    re.compile(r'@\d+x\.', re.I),
    re.compile(r'^[0-9a-f]{32,}@', re.I),
    re.compile(r'localhost$', re.I),
    re.compile(r'\.local$', re.I),
    re.compile(r'^(m|sp|c|s|d)\d+_', re.I),
    re.compile(r'^[a-z]{1,3}[_-]\d{3,}', re.I),
    re.compile(r'^[a-z]{2,4}\d{4,}', re.I),
    re.compile(r'[_-][0-9a-f]{8,}$', re.I),
]

SUBPAGES = [
    '/contact','/contact-us','/contactus','/contacts','/contacte','/contactos','/contatos','/kontakte','/kontaktai','/kontakti','/kontakter','/yhteystiedot','/epikoinonia',
    '/service','/services',
    '/service/contact-us','/service/contact','/services/contact',
    '/page/contact','/pages/contact','/pages/contact-us',
    '/en/contact','/en/contact-us','/info/contact',
    '/help/contact','/support/contact-us',
    '/kontakt','/kontakty',
    '/contatti','/contatto','/contacto','/contato',
    '/kapcsolat','/yhteystiedot','/yhteys','/palaute','/iletisim',
    '/kontakta-oss','/contactez-nous','/get-in-touch','/enquiries',
    '/editorial','/editorial-team',
    '/redactia','/redactie','/redaction','/redakcja','/redakce','/redakcija',
    '/redaktion','/redazione','/redaccion','/szerkesztoseg','/toimitus','/redaksjon',
    '/advertise','/advertising',
    '/publicitate','/publicite','/publicidad','/pubblicita','/publicidade',
    '/reklama','/reklame','/werbung','/hirdetes','/mainonta',
    '/annonsera','/annonsere','/annoncering','/adverteren','/inzerce','/oglasavanje',
    '/press','/presse','/prensa','/stampa','/imprensa',
    '/presa','/prasa','/sajto','/lehdisto','/basin','/spauda',
    '/about','/about-us',
    '/uber-uns','/ueber-uns','/a-propos','/qui-sommes-nous',
    '/chi-siamo','/sobre','/sobre-nos','/sobre-nosotros',
    '/over-ons','/o-nas','/despre','/despre-noi',
    '/rolunk','/meista','/om-oss','/om-os','/o-nama','/hakkimizda',
    '/impressum','/imprint','/impresszum','/mentions-legales',
    '/wspolpraca','/zusammenarbeit','/partenariat','/samenwerking',
    '/samarbete','/samarbeid','/samarbejde','/yhteistyo',
    '/parteneriat','/colaborare','/spolupraca','/suradnja',
    '/newsroom','/news/contact',
]

TLD_PATHS = {
    'ro': ['/contacte','/redactia-noastra','/redactie-noastra','/publicitate','/despre','/despre-noi','/ro/contact','/ro/contacte','/ro/redactia','/echipa','/echipa-redactionala'],
    'fi': ['/palaute','/yhteystiedot','/toimitus','/ilmoittaminen','/fi/yhteys','/fi/yhteystiedot','/mainonta','/mediakortti','/toimituskunta','/meista'],
    'hu': ['/kapcsolat','/szerkesztoseg','/hirdetes','/rolunk','/impresszum','/hu/kapcsolat','/mediaajanlat','/csapat'],
    'de': ['/uber-uns','/ueber-uns','/redaktion','/werbung','/anzeige','/mediadaten','/de/kontakt','/de/impressum','/de/redaktion','/team','/mitarbeiter','/verlag'],
    'at': ['/uber-uns','/ueber-uns','/redaktion','/werbung','/mediadaten','/de/kontakt','/team'],
    'ch': ['/uber-uns','/redaktion','/werbung','/a-propos','/redaction','/de/kontakt','/fr/contact','/it/contatti'],
    'pl': ['/kontakty','/redakcja','/reklama','/o-nas','/wspolpraca','/pl/kontakt','/zespol','/wydawca','/dla-reklamodawcow'],
    'fr': ['/contactez-nous','/a-propos','/qui-sommes-nous','/redaction','/publicite','/fr/contact','/equipe','/equipe-editoriale','/mentions-legales','/partenariat'],
    'be': ['/redactie','/adverteren','/over-ons','/nl/contact','/fr/contact','/equipe','/team'],
    'nl': ['/redactie','/adverteren','/over-ons','/nl/contact','/team','/samenwerking','/mediakit'],
    'it': ['/contatto','/contatti','/redazione','/pubblicita','/chi-siamo','/it/contatti','/team','/collabora','/scrivi-per-noi'],
    'es': ['/redaccion','/publicidad','/sobre-nosotros','/quienes-somos','/es/contacto','/equipo','/colabora','/escribe-para-nosotros'],
    'pt': ['/contacto','/contato','/sobre','/sobre-nos','/quem-somos','/publicidade','/pt/contacto','/equipa','/equipe','/redacao'],
    'br': ['/contato','/sobre','/quem-somos','/publicidade','/redacao','/equipe','/anuncie'],
    'se': ['/kontakta-oss','/om-oss','/annonsera','/sv/kontakt','/redaktion','/medarbetare','/team'],
    'no': ['/kontakt','/om-oss','/annonsere','/redaksjon','/presse','/medarbeidere','/team'],
    'dk': ['/kontakt','/om-os','/annoncering','/redaktion','/presse','/medarbejdere','/team'],
    'cz': ['/kontakt','/kontakty','/redakce','/inzerce','/o-nas','/tym','/spoluprace'],
    'sk': ['/kontakt','/redakcia','/inzercia','/o-nas','/tym','/spolupraca'],
    'hr': ['/kontakt','/redakcija','/oglasavanje','/o-nama','/tim','/suradnja'],
    'rs': ['/kontakt','/redakcija','/oglasavanje','/o-nama','/tim'],
    'si': ['/kontakt','/urednistvo','/oglasevanje','/o-nas','/ekipa'],
    'bg': ['/kontakti','/redakciya','/reklama','/za-nas','/ekip'],
    'tr': ['/iletisim','/hakkimizda','/reklam','/basin','/yayin','/ekip','/kurumsal'],
    'gr': ['/epikoinonia','/diafimisi','/schedia-mas','/omada'],
    'lt': ['/kontaktai','/redakcija','/reklama','/apie-mus','/komanda'],
    'lv': ['/kontakti','/redakcija','/reklama','/par-mums','/komanda'],
    'ee': ['/kontakt','/toimetus','/reklaam','/meist','/meeskond'],
    'ua': ['/kontakty','/ua/kontakty','/uk/contact','/pro-nas','/redakciya','/reklama'],
    'ru': ['/kontakty','/ru/kontakty','/o-nas','/redakciya','/reklama'],
    'uk': ['/get-in-touch','/enquiries','/en/contact','/en/about','/team','/write-for-us','/media-pack','/media-kit','/service/contact-us','/services/contact','/help/contact-us','/newsroom','/news/contact'],
    'ie': ['/get-in-touch','/enquiries','/en/contact','/en/about','/team','/write-for-us'],
    'com': ['/write-for-us','/guest-post','/contribute','/media-kit','/team','/staff','/newsroom'],
}

LINK_KW = [
    'contact','contacte','kontakt','contatti','contatto','contacto','contato',
    'kapcsolat','yhteystiedot','yhteys','iletisim','kontakty','kontakte','kontaktai','kontakti','palaute','anfrage',
    'about','uber-uns','ueber-uns','chi-siamo','a-propos','qui-sommes',
    'o-nas','sobre','sobre-nos','quem-somos','sobre-nosotros','quienes-somos',
    'over-ons','om-oss','om-os','rolunk','meista','tietoa',
    'despre','despre-noi','o-nama','hakkimizda',
    'editorial','redakc','redaction','redactie','redazione','redaccion',
    'redactia','redaksjon','redakce','szerkesztoseg','toimitus',
    'advertis','reklama','reklame','werbung','publicite','pubblicita',
    'publicidad','publicidade','publicitate','adverteren',
    'hirdetes','mainonta','annons','annonse','annonce','inzerce','oglasavanje',
    'press','presse','prensa','stampa','imprensa','presa','prasa',
    'pers','sajto','lehdisto','tisk','basin','spauda',
    'impressum','imprint','impresszum','mentions-legales',
    'wspolpraca','zusammenarbeit','kooperation','partenariat',
    'samenwerking','samarbete','samarbeid','samarbejde','yhteistyo',
    'parteneriat','colaborare','egyuttmukodes','spolupraca','suradnja',
    'service','dienst','servicio','servizio',
    'newsroom','newsdesk','news-desk',
]

SITEMAP_KW = [
    'contact','kontakt','contatti','contacto','contato','kapcsolat',
    'yhteystiedot','iletisim','editorial','redact','redakc',
    'advertis','publicitate','publicite','pubblicita','publicidad','publicidade',
    'reklama','werbung','press','presse','prensa','stampa',
    'about','uber-uns','chi-siamo','sobre','impressum','newsroom',
    'service/contact',
]

UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'

HEADERS = {
    'User-Agent': UA,
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
}


# в”Ђв”Ђ HELPERS в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ

def domain_of(d):
    d = re.sub(r'^(https?://)?(www\.)?', '', d)
    return d.split('/')[0].lower()


def norm(d):
    d = d.strip().rstrip('/')
    if not d.startswith('http'):
        d = 'https://' + d
    return d


def is_third_party(dp):
    for t in THIRD_PARTY:
        if dp == t or dp.endswith('.' + t):
            return True
    return False


def wb_match(local, kw):
    idx = local.find(kw)
    if idx == -1:
        return False
    b = local[idx - 1] if idx > 0 else ''
    end = idx + len(kw)
    a = local[end] if end < len(local) else ''
    sep_b = not b or b in '._ -'
    sep_a = not a or a in '._ -'
    return sep_b and sep_a


def kw_match(local, kw):
    if len(kw) <= 3:
        return wb_match(local, kw)
    return kw in local


# в”Ђв”Ђ EMAIL VALIDATION в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ

def is_valid(email):
    if not email or len(email) < 5 or len(email) > 100:
        return False
    for pat in BAD_RE:
        if pat.search(email):
            return False
    parts = email.split('@')
    if len(parts) != 2:
        return False
    local, dp = parts
    if not dp:
        return False
    tld = dp.rsplit('.', 1)[-1]
    if not tld or len(tld) < 2 or len(tld) > 10:
        return False
    if len(local) > 40 or re.fullmatch(r'\d+', local):
        return False
    if is_third_party(dp):
        return False
    labels = dp.split('.')
    # For ccSLDs like .co.uk, .com.au, .co.jp вЂ” use the label before the ccSLD
    CCSLD = {'co', 'com', 'net', 'org', 'ac', 'gov', 'edu', 'mil', 'gen', 'nom', 'or', 'ne', 'go'}
    if len(labels) >= 3 and labels[-2] in CCSLD and len(labels[-1]) <= 3:
        main = labels[-3]
    else:
        main = labels[-2] if len(labels) >= 2 else ''
    if len(main) <= 3:
        return False
    if re.fullmatch(r'\d+x\d+', main) or re.fullmatch(r'[a-z0-9]{2,6}x[a-z0-9]{2,6}', main):
        return False
    for lbl in labels[:-1]:
        if re.fullmatch(r'[0-9a-f]{5,}', lbl, re.I):
            return False
    if not re.search(r'[aeiou]', main, re.I):
        return False
    if re.search(r'\d', main):
        return False
    if len(local) > 30:
        return False
    if re.search(r'\d', local):
        return False
    if len(re.findall(r'[._-]', local)) > 3:
        return False
    return True


# в”Ђв”Ђ EMAIL CLASSIFICATION в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ

def classify(email):
    local = email.split('@')[0].lower()
    for kw in BLACKLIST:
        if wb_match(local, kw):
            return 'bl'
    for kw in TIER1:
        if kw_match(local, kw):
            return 't1'
    for k in GENERIC:
        if local == k or local.startswith(k + '.') or local.startswith(k + '_') or local.startswith(k + '-'):
            return 'gen'
    for kw in TIER2:
        if kw_match(local, kw):
            return 't2'
    return 'oth'


# в”Ђв”Ђ EMAIL EXTRACTION в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ

def decode_cf(enc):
    try:
        if not enc or len(enc) < 4:
            return ''
        key = int(enc[:2], 16)
        out = ''
        for i in range(2, len(enc), 2):
            out += chr(int(enc[i:i + 2], 16) ^ key)
        return out
    except Exception:
        return ''


def extract(text):
    if not text:
        return []
    dec = text
    for old, new in [('&#64;', '@'), ('&#x40;', '@'), ('&#46;', '.'), ('&#x2e;', '.')]:
        dec = dec.replace(old, new)
    dec = re.sub(r'\[at\]', '@', dec, flags=re.I)
    dec = re.sub(r'\(at\)', '@', dec, flags=re.I)
    dec = re.sub(r'\[dot\]', '.', dec, flags=re.I)
    dec = re.sub(r'\(dot\)', '.', dec, flags=re.I)

    res = {}

    # Cloudflare email protection
    for m in re.finditer(r'data-cfemail\s*=\s*["\']([0-9a-fA-F]+)["\']', text):
        d = decode_cf(m.group(1))
        if d and '@' in d:
            res[d.lower()] = True
    for m in re.finditer(r'/cdn-cgi/l/email-protection#([0-9a-fA-F]+)', text):
        d = decode_cf(m.group(1))
        if d and '@' in d:
            res[d.lower()] = True

    # mailto:
    for m in re.finditer(r'mailto:([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})', text):
        res[m.group(1).lower()] = True

    # general regex
    for e in EMAIL_RE.findall(dec):
        res[e.lower()] = True

    return [e for e in res if is_valid(e)]


# в”Ђв”Ђ LINK DISCOVERY в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ

HREF_RE = re.compile(r'href\s*=\s*["\']([^"\'\s]{5,500})["\']', re.I)

# Hub pages: when we land on these, we collect ALL internal links (not just keyword-filtered)
HUB_PATTERNS = re.compile(r'/(?:service|services|help|support|about|editorial|newsroom)/?$', re.I)


def find_links(html, base_url, all_links=False):
    """Find relevant links in HTML. If all_links=True, collect ALL same-domain links."""
    links = {}
    parsed_base = urlparse(base_url)
    for m in HREF_RE.finditer(html):
        href = m.group(1)
        if href.startswith(('mailto:', '#', 'javascript:')):
            continue
        lo = href.lower()
        if not all_links:
            relevant = any(kw in lo for kw in LINK_KW)
            if not relevant:
                continue
        try:
            if href.startswith('http'):
                abs_url = href
            elif href.startswith('/'):
                abs_url = f'{parsed_base.scheme}://{parsed_base.netloc}{href}'
            else:
                abs_url = base_url.rstrip('/') + '/' + href
            parsed_link = urlparse(abs_url)
            if (parsed_link.hostname == parsed_base.hostname or
                    (parsed_link.hostname and parsed_link.hostname.endswith('.' + parsed_base.hostname))):
                clean = abs_url.split('#')[0].split('?')[0]
                # Skip obvious non-page resources
                if re.search(r'\.(css|js|png|jpg|jpeg|gif|svg|woff|ico|pdf|zip|mp[34])$', clean, re.I):
                    continue
                links[clean] = True
        except Exception:
            pass
    return list(links.keys())


def is_hub_page(url):
    """Check if a URL is a hub/index page whose ALL links should be followed."""
    path = urlparse(url).path
    return bool(HUB_PATTERNS.search(path))


def parse_sitemap(xml, dn):
    urls = []
    for m in re.finditer(r'<loc>\s*(https?://[^<]+)\s*</loc>', xml, re.I):
        u = m.group(1).strip()
        if dn not in u:
            continue
        lo = u.lower()
        if any(kw in lo for kw in SITEMAP_KW):
            urls.append(u)
    return urls


# в”Ђв”Ђ RANK & SELECT в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ

def rank(email, db, contact_emails=None):
    local = email.split('@')[0].lower()
    d = email.split('@')[1]
    dp = 0 if (d == db or d.endswith('.' + db)) else 1000
    for i, kw in enumerate(BLACKLIST):
        if wb_match(local, kw):
            return 99999
    # Contact page bonus: -50 if email was found on a contact page
    contact_bonus = -50 if (contact_emails and email in contact_emails) else 0
    for i, kw in enumerate(TIER1):
        if kw_match(local, kw):
            return max(0, dp + i + contact_bonus)
    for i, kw in enumerate(TIER2):
        if kw_match(local, kw):
            return max(0, dp + 100 + i + contact_bonus)
    for i, k in enumerate(GENERIC):
        if local == k or local.startswith(k + '.') or local.startswith(k + '_') or local.startswith(k + '-'):
            return max(0, dp + 300 + i + contact_bonus)
    main = db.split('.')[0]
    if local == main and dp == 0:
        return 360
    if dp == 0 and re.fullmatch(r'[a-z]{2,}(\.[a-z]{2,})?', local) and len(local) <= 30:
        return 250
    return 99999


def select_best(emails, domain, contact_emails=None):
    db = domain_of(domain)
    valid = [e for e in emails if rank(e, db, contact_emails) < 99999]
    valid.sort(key=lambda e: rank(e, db, contact_emails))
    if not valid:
        return []
    res = [valid[0]]
    if len(valid) < 2:
        return res
    r0 = rank(valid[0], db, contact_emails)
    if r0 < 200:
        for e in valid[1:]:
            r = rank(e, db, contact_emails)
            if 300 <= r < 500:
                res.append(e)
                break
        if len(res) == 1 and len(valid) > 1:
            res.append(valid[1])
    else:
        return valid[:2]
    return res[:2]


# в”Ђв”Ђ SMTP EMAIL VERIFICATION в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ

def _get_mx_host(domain):
    """Get the primary MX host for a domain."""
    try:
        import dns.resolver
        answers = dns.resolver.resolve(domain, 'MX')
        mx_records = sorted(answers, key=lambda r: r.preference)
        if mx_records:
            return str(mx_records[0].exchange).rstrip('.')
    except Exception:
        pass
    # Fallback: try connecting to the domain directly
    try:
        socket.getaddrinfo(domain, 25)
        return domain
    except Exception:
        return None


def _verify_one(email, timeout=10):
    """
    Verify single email via SMTP RCPT TO.
    Returns:
      True  вЂ” server confirmed the mailbox exists
      False вЂ” server explicitly rejected (550)
      None  вЂ” can't determine (timeout, catch-all, connection refused, etc.)
    """
    domain = email.split('@')[1]
    mx = _get_mx_host(domain)
    if not mx:
        return None

    try:
        with smtplib.SMTP(mx, 25, timeout=timeout) as smtp:
            smtp.ehlo_or_helo_if_needed()
            code_from, _ = smtp.mail('check@example.com')
            code_rcpt, msg_rcpt = smtp.rcpt(email)

            # Check for catch-all: send a clearly fake address
            fake = f'xyznonexistent.{int(time.time())}@{domain}'
            fake_code, _ = smtp.rcpt(fake)
            if fake_code == 250:
                # Catch-all domain вЂ” can't verify individual addresses
                return None

            return code_rcpt == 250
    except smtplib.SMTPServerDisconnected:
        return None
    except (smtplib.SMTPConnectError, socket.timeout, OSError):
        return None
    except Exception:
        return None


async def verify_emails(emails, ctx):
    """
    Verify a list of emails via SMTP.
    Returns only emails that are confirmed or unverifiable.
    Emails explicitly rejected (550) are removed.
    """
    if not emails:
        return emails

    ctx.log.append(f'[SMTP] Verifying {len(emails)} email(s)...')
    loop = asyncio.get_event_loop()
    verified = []

    for email in emails:
        if ctx.expired():
            # Out of time вЂ” keep remaining unverified
            verified.append(email)
            ctx.log.append(f'  SMTP: {email} -> kept (timeout)')
            continue

        result = await loop.run_in_executor(None, _verify_one, email)

        if result is False:
            ctx.log.append(f'  SMTP: {email} -> REJECTED (removed)')
        elif result is True:
            ctx.log.append(f'  SMTP: {email} -> EXISTS')
            verified.append(email)
        else:
            ctx.log.append(f'  SMTP: {email} -> unknown (kept)')
            verified.append(email)

    ctx.log.append(f'[SMTP] Result: {len(verified)}/{len(emails)} passed')
    return verified


# в”Ђв”Ђ ASYNC HTTP в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ

class CrawlContext:
    def __init__(self, max_reqs=60, deadline_sec=28):
        self.reqs = 0
        self.max_reqs = max_reqs
        self.deadline = time.time() + deadline_sec
        self.log = []
        self.blocked = False

    def expired(self):
        return time.time() > self.deadline


async def safe_fetch(session, url, timeout_sec, ctx):
    if ctx.reqs >= ctx.max_reqs or ctx.expired():
        return None
    ctx.reqs += 1
    try:
        timeout = aiohttp.ClientTimeout(total=timeout_sec)
        async with session.get(url, headers=HEADERS, timeout=timeout,
                               allow_redirects=True, ssl=False) as resp:
            if resp.status != 200:
                ctx.log.append(f'  [{resp.status}] {url[:90]}')
                if resp.status in (403, 503):
                    ctx.blocked = True
                return None
            ct = resp.headers.get('content-type', '')
            if ct and 'text' not in ct and 'html' not in ct and 'xml' not in ct:
                return None
            text = await resp.text(errors='replace')
            if len(text) > MAX_HTML:
                text = text[:MAX_HTML]
            return text
    except Exception:
        return None


async def fetch_multi(session, urls, timeout_sec, ctx):
    tasks = [safe_fetch(session, u, timeout_sec, ctx) for u in urls]
    return await asyncio.gather(*tasks)


# в”Ђв”Ђ CRAWL в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ

from contextlib import asynccontextmanager

@asynccontextmanager
async def _session_guard(session, should_close):
    """Close session only if we own it."""
    try:
        yield session
    finally:
        if should_close:
            await session.close()


async def crawl(domain, ctx, session=None, smtp=True):
    all_emails = {}
    contact_emails = set()  # emails found on contact/contacte pages get rank bonus
    base = norm(domain)
    dn = domain_of(domain)
    tld = dn.rsplit('.', 1)[-1]
    has_prio = False
    scanned = {}
    _own_session = session is None  # True if we create session here

    def add(email_list, source_url=''):
        nonlocal has_prio
        is_contact = bool(source_url and re.search(
            r'/(?:contact|contacte|contatti|contatto|contacto|contato|kontakt|kapcsolat|iletisim|yhteystiedot)[^/]*$',
            source_url, re.I))
        for e in email_list:
            if e not in all_emails:
                all_emails[e] = True
            if is_contact:
                contact_emails.add(e)
            cls = classify(e)
            ed = e.split('@')[1]
            if cls in ('t1', 't2') and (ed == dn or ed.endswith('.' + dn)):
                has_prio = True

    if _own_session:
        connector = aiohttp.TCPConnector(limit=10, ssl=False)
        session = aiohttp.ClientSession(connector=connector)

    try:

        # в”Ђв”Ђ PHASE 1: Homepage
        hp_variants = [base]
        if '://www.' not in base:
            hp_variants.append(base.replace('://', '://www.'))
        else:
            hp_variants.append(base.replace('://www.', '://'))

        ctx.log.append(f'[1/4] Homepage ({len(hp_variants)} variants)...')
        w_base = None
        hp_html = None

        for u in hp_variants:
            if ctx.expired():
                break
            html = await safe_fetch(session, u, 12, ctx)
            if html:
                hp_html = html
                w_base = u
                ctx.log.append(f'  OK: {u} ({len(html)}b)')
                break

        if not w_base:
            w_base = base
            ctx.log.append('  Homepage not reachable')

        if hp_html:
            add(extract(hp_html), w_base)
            if has_prio:
                ctx.log.append('  -> Priority on homepage!')
                return await _finish(all_emails, ctx, dn, domain, contact_emails, smtp=smtp)

        if ctx.expired():
            return await _finish(all_emails, ctx, dn, domain, contact_emails, smtp=smtp)

        # в”Ђв”Ђ PHASE 1.5: Quick contact pages + spider
        ctx.log.append('[1.5] Quick contact pages...')
        quick_paths = ['/contact', '/contacte', '/contact-us', '/service', '/services',
                       '/editorial', '/advertise', '/about', '/about-us', '/impressum',
                       '/help', '/help/contact', '/service/contact-us']
        b_clean = w_base.rstrip('/')
        # Only use the working base (no alt variants) to save requests

        quick_urls = []
        seen_quick = set()
        for p in quick_paths:
            u = b_clean + p
            if u not in seen_quick:
                seen_quick.add(u)
                quick_urls.append(u)

        quick_res = await fetch_multi(session, quick_urls, 8, ctx)
        spider_queue = {}
        for i, html in enumerate(quick_res):
            if not html:
                continue
            scanned[quick_urls[i]] = True
            ctx.log.append(f'  OK: {quick_urls[i]} ({len(html)}b)')
            add(extract(html), quick_urls[i])
            # Hub page detection: if this is /service, /help, etc. - collect ALL links
            use_all = is_hub_page(quick_urls[i])
            if use_all:
                ctx.log.append(f'    -> Hub page detected, collecting all links')
            for lnk in find_links(html, quick_urls[i], all_links=use_all):
                if lnk not in scanned:
                    spider_queue[lnk] = True

        # Spider: contact-URLs first, then other hub-discovered links
        if not has_prio and not ctx.expired():
            def spider_sort_key(url):
                lo = url.lower()
                if 'contact' in lo:
                    return 0
                if any(kw in lo for kw in ('editorial', 'advertis', 'press', 'newsroom')):
                    return 1
                return 2
            sp_list = sorted(spider_queue.keys(), key=spider_sort_key)[:15]
            if sp_list:
                ctx.log.append(f'  Spider: {len(sp_list)} inner links')
                sp_res = await fetch_multi(session, sp_list, 6, ctx)
                for i, html in enumerate(sp_res):
                    if not html:
                        continue
                    scanned[sp_list[i]] = True
                    ctx.log.append(f'    OK: {sp_list[i]} ({len(html)}b)')
                    add(extract(html), sp_list[i])

        if has_prio:
            ctx.log.append('  -> Priority via quick contact!')
            return await _finish(all_emails, ctx, dn, domain, contact_emails, smtp=smtp)
        if ctx.expired():
            return await _finish(all_emails, ctx, dn, domain, contact_emails, smtp=smtp)

        # в”Ђв”Ђ PHASE 1.75: Web Archive (early, if site blocks us)
        if ctx.blocked and not has_prio and not ctx.expired():
            ctx.log.append('[1.75] Site blocked (403/503) -> Web Archive early...')
            ar_base = ('www.' + dn) if '://www.' in w_base else dn
            ar_paths = ['/contacte', '/contact', '/contact-us', '/editorial',
                        '/advertise', '/about', '/impressum', '/press', '/newsroom', '/service']
            ar_extra = TLD_PATHS.get(tld, [])[:8]
            all_ar = list(dict.fromkeys(ar_paths + ar_extra))

            for p in all_ar:
                if ctx.expired() or has_prio:
                    break
                a_url = f'https://web.archive.org/web/2/{ar_base}{p}'
                if a_url in scanned:
                    continue
                a_html = await safe_fetch(session, a_url, 10, ctx)
                if not a_html:
                    continue
                scanned[a_url] = True
                ctx.log.append(f'  [archive] {p} ({len(a_html)}b)')
                add(extract(a_html), f'https://{ar_base}{p}')
                if not has_prio:
                    a_links = find_links(a_html, f'https://{ar_base}')
                    c_links = [l for l in a_links if any(
                        kw in l.lower() for kw in ('contact', 'editorial', 'advertis', 'redact', 'publicitate')
                    )][:5]
                    for cl in c_links:
                        if ctx.expired() or has_prio:
                            break
                        cl_ar = 'https://web.archive.org/web/2/' + re.sub(r'^https?://', '', cl)
                        if cl_ar in scanned:
                            continue
                        cl_html = await safe_fetch(session, cl_ar, 8, ctx)
                        if cl_html:
                            scanned[cl_ar] = True
                            ctx.log.append(f'    [archive spider] {cl[:80]}')
                            add(extract(cl_html), cl)

            if has_prio:
                ctx.log.append('  -> Priority via archive!')
                return await _finish(all_emails, ctx, dn, domain, contact_emails, smtp=smtp)
            if ctx.expired():
                return await _finish(all_emails, ctx, dn, domain, contact_emails, smtp=smtp)

        # в”Ђв”Ђ PHASE 2: Homepage links + spider (also collect all links from homepage for hub discovery)
        if hp_html:
            # Get keyword-filtered links AND all links from homepage
            hp_links_kw = find_links(hp_html, w_base, all_links=False)
            hp_links_all = find_links(hp_html, w_base, all_links=True)
            # Prioritize keyword links, then add remaining all-links that contain useful patterns
            hp_link_set = dict.fromkeys(hp_links_kw)
            for lnk in hp_links_all:
                lo = lnk.lower()
                # Include links from /service/, /help/, /about/ sections even without strict keywords
                if any(seg in lo for seg in ('/service/', '/help/', '/about/', '/editorial/', '/newsroom/')):
                    hp_link_set[lnk] = True
            hp_links = list(hp_link_set.keys())

            def link_weight(url):
                lo = url.lower()
                if 'contact' in lo or 'service' in lo:
                    return -2
                if any(kw in lo for kw in ('editorial', 'advertis', 'press', 'newsroom')):
                    return -1
                return 0

            hp_links.sort(key=link_weight)
            hp_links = [u for u in hp_links if u not in scanned][:15]

            if hp_links:
                ctx.log.append(f'[2/4] Homepage links: {len(hp_links)}')
                hp_pages = await fetch_multi(session, hp_links, 8, ctx)
                spider2 = {}
                for i, html in enumerate(hp_pages):
                    scanned[hp_links[i]] = True
                    if not html:
                        continue
                    ctx.log.append(f'  OK: {hp_links[i]} ({len(html)}b)')
                    add(extract(html), hp_links[i])
                    if not has_prio:
                        for lnk in find_links(html, hp_links[i]):
                            if lnk not in scanned:
                                spider2[lnk] = True

                # Spider level 2
                if not has_prio and not ctx.expired():
                    sp2 = sorted(spider2.keys(),
                                 key=lambda x: 0 if 'contact' in x.lower() else 1)[:8]
                    if sp2:
                        ctx.log.append(f'  Spider: {len(sp2)} inner links')
                        sp2_pages = await fetch_multi(session, sp2, 6, ctx)
                        for i, html in enumerate(sp2_pages):
                            scanned[sp2[i]] = True
                            if html:
                                ctx.log.append(f'    OK: {sp2[i]} ({len(html)}b)')
                                add(extract(html), sp2[i])

            if has_prio:
                ctx.log.append('  -> Priority in HP links!')
                return await _finish(all_emails, ctx, dn, domain, contact_emails, smtp=smtp)

        if ctx.expired():
            return await _finish(all_emails, ctx, dn, domain, contact_emails, smtp=smtp)

        # в”Ђв”Ђ PHASE 2.5: Sitemap
        ctx.log.append('[2.5] Sitemap...')
        sm_urls = [b_clean + '/sitemap.xml', b_clean + '/sitemap_index.xml']
        if '://www.' not in b_clean:
            sm_urls.append(b_clean.replace('://', '://www.') + '/sitemap.xml')

        sm_links = []
        for su in sm_urls:
            if ctx.expired() or sm_links:
                break
            xml = await safe_fetch(session, su, 8, ctx)
            if not xml:
                continue
            ctx.log.append(f'  Found: {su}')
            sm_links = parse_sitemap(xml, dn)
            # Sub-sitemaps
            sub_sms = re.findall(r'<loc>\s*(https?://[^<]*sitemap[^<]*\.xml)\s*</loc>', xml, re.I)
            for ss_url in sub_sms[:3]:
                if ctx.expired():
                    break
                sub_xml = await safe_fetch(session, ss_url.strip(), 6, ctx)
                if sub_xml:
                    sm_links.extend(parse_sitemap(sub_xml, dn))

        sm_uniq = list(dict.fromkeys(u.split('#')[0].split('?')[0] for u in sm_links))
        sm_uniq = [u for u in sm_uniq if u not in scanned][:10]

        if sm_uniq:
            ctx.log.append(f'  {len(sm_uniq)} contact URLs from sitemap')
            sm_pages = await fetch_multi(session, sm_uniq, 8, ctx)
            for i, html in enumerate(sm_pages):
                scanned[sm_uniq[i]] = True
                if html:
                    ctx.log.append(f'    OK: {sm_uniq[i]}')
                    add(extract(html), sm_uniq[i])

        if has_prio:
            ctx.log.append('  -> Priority via sitemap!')
            return await _finish(all_emails, ctx, dn, domain, contact_emails, smtp=smtp)
        if ctx.expired():
            return await _finish(all_emails, ctx, dn, domain, contact_emails, smtp=smtp)

        # в”Ђв”Ђ PHASE 3: Static subpages
        extra = TLD_PATHS.get(tld, [])
        hi = ['/contact', '/contact-us', '/service', '/service/contact', '/service/contact-us',
              '/help/contact', '/editorial', '/advertise', '/advertising', '/press', '/newsroom',
              '/about', '/about-us', '/impressum']
        combined = extra + SUBPAGES
        seen = {}
        ordered = []
        for p in hi:
            if p not in seen:
                seen[p] = True
                ordered.append(p)
        for p in combined:
            if p not in seen:
                seen[p] = True
                ordered.append(p)

        url_set = {}
        # Only use working base, no slash duplication
        for p in ordered:
            u1 = b_clean + p
            if u1 not in scanned:
                url_set[u1] = True

        url_list = list(url_set.keys())[:60]

        if url_list and not ctx.expired():
            ctx.log.append(f'[3/4] Subpages: {len(url_list)} URLs')
            for j in range(0, len(url_list), 10):
                if ctx.expired() or has_prio:
                    break
                batch = url_list[j:j + 10]
                pages = await fetch_multi(session, batch, 6, ctx)
                sp3 = {}
                for p_idx, html in enumerate(pages):
                    scanned[batch[p_idx]] = True
                    if not html:
                        continue
                    ctx.log.append(f'  OK: {batch[p_idx]} ({len(html)}b)')
                    add(extract(html), batch[p_idx])
                    if not has_prio:
                        for lnk in find_links(html, batch[p_idx]):
                            if lnk not in scanned:
                                sp3[lnk] = True
                # Spider from subpages
                if not has_prio and not ctx.expired():
                    sp3_list = sorted(sp3.keys(),
                                      key=lambda x: 0 if 'contact' in x.lower() else 1)[:5]
                    if sp3_list:
                        sp3_res = await fetch_multi(session, sp3_list, 6, ctx)
                        for i, html in enumerate(sp3_res):
                            scanned[sp3_list[i]] = True
                            if html:
                                ctx.log.append(f'    SPIDER: {sp3_list[i]} ({len(html)}b)')
                                add(extract(html), sp3_list[i])
                if has_prio:
                    break

            if has_prio:
                ctx.log.append('  -> Priority in subpages!')

        if has_prio or ctx.expired():
            return await _finish(all_emails, ctx, dn, domain, contact_emails, smtp=smtp)

        # в”Ђв”Ђ PHASE 4: Web Archive
        ctx.log.append('[4/4] Web Archive...')
        ar_paths = ['/contacte', '/contact', '/contact-us', '/service', '/editorial',
                    '/advertise', '/about', '/impressum', '/press', '/newsroom']
        ar_extra = TLD_PATHS.get(tld, [])[:5]
        all_ar_paths = ar_paths + ar_extra
        ar_base = ('www.' + dn) if '://www.' in w_base else dn

        for p in all_ar_paths:
            if ctx.expired() or has_prio:
                break
            a_url = f'https://web.archive.org/web/2/{ar_base}{p}'
            if a_url in scanned:
                continue
            a_html = await safe_fetch(session, a_url, 10, ctx)
            if not a_html:
                continue
            scanned[a_url] = True
            ctx.log.append(f'  [archive] {p} ({len(a_html)}b)')
            add(extract(a_html), f'https://{ar_base}{p}')
            # Spider archive
            if not has_prio:
                a_links = find_links(a_html, f'https://{ar_base}')
                contact_links = [l for l in a_links if any(
                    kw in l.lower() for kw in ('contact', 'editorial', 'advertis')
                )][:5]
                for cl in contact_links:
                    if ctx.expired() or has_prio:
                        break
                    cl_html = await safe_fetch(session, cl, 6, ctx)
                    if not cl_html:
                        cl_ar = 'https://web.archive.org/web/2/' + re.sub(r'^https?://', '', cl)
                        cl_html = await safe_fetch(session, cl_ar, 8, ctx)
                    if cl_html:
                        ctx.log.append(f'    [archive spider] {cl[:80]}')
                        add(extract(cl_html), cl)

        return await _finish(all_emails, ctx, dn, domain, contact_emails, smtp=smtp)
    finally:
        if _own_session:
            await session.close()


async def _finish(all_emails, ctx, dn, domain, contact_emails=None, smtp=True):
    emails = list(all_emails.keys())
    # Pre-select top candidates for SMTP verification (verify more than we need for fallback)
    db = domain_of(domain)
    candidates = [e for e in emails if rank(e, db, contact_emails) < 99999]
    candidates.sort(key=lambda e: rank(e, db, contact_emails))
    top_candidates = candidates[:10]  # verify top 10 to allow fallback

    # SMTP verification: remove emails that are confirmed non-existent
    if smtp and top_candidates:
        verified = await verify_emails(top_candidates, ctx)
        # Remove rejected emails from the full list too
        rejected = set(top_candidates) - set(verified)
        if rejected:
            emails = [e for e in emails if e not in rejected]
    elif not smtp:
        ctx.log.append('[SMTP] Skipped (disabled for speed)')

    best = select_best(emails, domain, contact_emails)
    ctx.log.append(f'Total emails found: {len(emails)} | Best: {len(best)} | Requests: {ctx.reqs}')
    return {
        'domain': dn,
        'best': best,
        'all': emails,
        'contact_emails': list(contact_emails or []),
        'log': ctx.log,
        'requests': ctx.reqs,
    }


# в”Ђв”Ђ HTTP SERVER (Flask-like, optional) в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ

def make_app():
    """Create a Flask app for serving the API."""
    from flask import Flask, request as req, jsonify

    app = Flask(__name__)

    @app.route('/')
    def handle():
        if 'health' in req.args:
            return jsonify({'status': 'ok', 'version': '1.0-py'})

        api_key = app.config.get('API_KEY', '')
        if api_key and req.args.get('key') != api_key:
            return jsonify({'error': 'Unauthorized'}), 401

        domain = req.args.get('domain')
        if not domain:
            return jsonify({'error': 'Missing domain parameter'}), 400

        ctx = CrawlContext(max_reqs=60, deadline_sec=28)
        result = asyncio.run(crawl(domain, ctx))
        return jsonify(result)

    return app


# в”Ђв”Ђ CLI в”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђв”Ђ

async def test_domain(domain):
    print('\n' + '=' * 70)
    print(f'TESTING: {domain}')
    print('=' * 70)

    ctx = CrawlContext(max_reqs=60, deadline_sec=55)

    try:
        result = await crawl(domain, ctx)
        ce = set(result.get('contact_emails', []))

        print('\n--- BEST EMAILS ---')
        if not result['best']:
            print('  (none found)')
        else:
            for i, e in enumerate(result['best']):
                on_contact = ' [CONTACT PAGE]' if e in ce else ''
                print(f'  {i + 1}. {e}  [class: {classify(e)}, rank: {rank(e, result["domain"], ce)}]{on_contact}')

        print('\n--- ALL EMAILS (sorted by rank) ---')
        db = domain_of(domain)
        classified = sorted(
            [(e, classify(e), rank(e, db, ce), e.split('@')[1] == db or e.split('@')[1].endswith('.' + db), e in ce)
             for e in result['all']],
            key=lambda x: x[2]
        )
        for email, cls, rnk, on_domain, on_contact in classified:
            cmark = ' *CONTACT*' if on_contact else ''
            print(f'  {email:<40} class={cls:<4} rank={str(rnk):<6} onDomain={on_domain}{cmark}')

        print('\n--- LOG ---')
        for line in result['log']:
            print(f'  {line}')

        print(f'\nRequests: {result["requests"]}')

    except Exception as e:
        print(f'ERROR: {e}')
        import traceback
        traceback.print_exc()


def main():
    import sys

    if '--serve' in sys.argv:
        port = 8080
        for i, arg in enumerate(sys.argv):
            if arg == '--port' and i + 1 < len(sys.argv):
                port = int(sys.argv[i + 1])
        app = make_app()
        app.run(host='0.0.0.0', port=port)
        return

    domains = [a for a in sys.argv[1:] if not a.startswith('--')]
    if not domains:
        print('Usage:')
        print('  python email_finder.py example.com [example2.com ...]')
        print('  python email_finder.py --serve --port 8080')
        sys.exit(1)

    async def run_all():
        for d in domains:
            await test_domain(d)

    asyncio.run(run_all())


if __name__ == '__main__':
    main()
