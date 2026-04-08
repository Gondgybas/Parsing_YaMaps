import sys, os, datetime, json, threading, time, re, urllib3, pandas as pd
from queue import Queue, Empty
from threading import Event
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QLabel, QPushButton, QVBoxLayout, QHBoxLayout, QLineEdit, QTextEdit, QDialog, QTabWidget, QListWidget, QFormLayout, QMessageBox, QTableWidget, QTableWidgetItem, QHeaderView
)
from PySide6.QtCore import Qt, QThread, Signal

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(BASE_DIR)

SETTINGS_FILE = os.path.join(BASE_DIR, "settings.json")
DEFAULT_SETTINGS = {
    "timings": {"after_search_enter": 5,"loading_card": 4,"yandex_search_wait": 8,"captcha_wait": 15,"between_site_pages": 2.5,"site_request_timeout": 8,"selenium_wait_timeout": 15,},
    "black_domains": ["vk.com", "avito.ru", "avito.com", "hh.ru", "ok.ru", "youtube.com","facebook.com", "instagram.com", "twitter.com", "t.me", "2gis.ru", ".yandex.", "ya.ru", "mail.ru", "rb.ru", "google.com", "zoon.ru", "orgpage.ru", "google.ru", "yandex.ru/maps", "rusprofile.ru",".clients.site", ".orgsinfo.ru", ".jsprav.ru", "yandex.ru/profile","ruscatalog.org"],
    "messenger_links": ["wa.me/", "t.me/", "viber.me/", "viber://", "telegram.me/", "telegram.org/"],
    "forbidden_emails": ["support@maps.yandex.ru","webmaps-revolution@yandex-team.ru","m-maps@support.yandex.ru"],
    "contact_pages": ["", "/contacts", "/contact", "/kontakty", "/kontakt","/about", "/about-us", "/company", "/info", "/contact-us"]
}
def load_settings():
    if os.path.exists(SETTINGS_FILE):
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                saved = json.load(f)
            merged = DEFAULT_SETTINGS.copy()
            for key in DEFAULT_SETTINGS:
                if key in saved:
                    if isinstance(DEFAULT_SETTINGS[key], dict):
                        merged[key] = {**DEFAULT_SETTINGS[key], **saved[key]}
                    else:
                        merged[key] = saved[key]
            return merged
        except Exception:
            pass
    return DEFAULT_SETTINGS.copy()
def save_settings(sett):
    try:
        with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
            json.dump(sett, f, ensure_ascii=False, indent=2)
    except Exception as e:
        QMessageBox.critical(None, "Ошибка", f"Не удалось сохранить настройки:\n{e}")

settings = load_settings()
EXCEL_FILENAME = os.path.join(BASE_DIR, "contacts_database.xlsx")
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
parser_stop_event = Event()
parser_pause_event = Event()
log_queue = Queue()

APP_FONT = "Segoe UI,Arial,sans-serif"
ACCENT = "#2565AE"
DARK_BG = "#161719"
DARK_BG2 = "#1e2023"
DARK_BG3 = "#232428"
DARK_PANEL = "#22232B"
DARK_FG = "#e7e7ed"
QSS = f"""
* {{
    font-family: "{APP_FONT}";
    font-size: 13pt;
}}
QMainWindow, QDialog, QWidget {{
    background: {DARK_BG};
    color: {DARK_FG};
}}
QPushButton {{
    background: {ACCENT};
    color: #fff;
    border-radius: 7px;
    border: none;
    padding: 6px 18px;
    font-size: 13.5pt;
}}
QPushButton:hover {{ background: #4482CC; }}
QPushButton:pressed {{ background: #1e304f; }}
QLineEdit, QTextEdit, QListWidget, QTableWidget, QTabWidget, QHeaderView, QFormLayout {{
    background: {DARK_BG2};
    color: {DARK_FG};
    border-radius: 5px;
    font-weight: 450;
    selection-background-color: {ACCENT};
}}
QLabel {{
    color: #d7d7eb;
    font-size: 13pt;
}}
QTabWidget::pane {{
    background: {DARK_BG3};
    border: 1px solid #22242c;
    border-radius: 8px;
}}
QTabBar::tab {{
    background: {DARK_BG3};
    color: #eee;
    padding: 7px 22px;
    font-size: 13pt;
    border: 1px solid #232430;
    border-bottom: none;
    border-top-left-radius: 8px;
    border-top-right-radius: 8px;
    margin-right: 3px;
}}
QTabBar::tab:selected {{
    background: {DARK_PANEL};
    color: {ACCENT};
}}
QHeaderView::section {{
    background: {DARK_BG2};
    color: {ACCENT};
    font-size: 12pt;
    font-family: "{APP_FONT}";
}}
QTableWidget {{
    gridline-color: #444;
    selection-background-color: {ACCENT};
}}
"""

def cut_to_main_yamaps_card(link):
    m = re.match(r"^(https://yandex\.[^/]+/maps/org/[^/]+/\d+)", link)
    if m:
        return m.group(1)
    return link.split("?")[0].split("/reviews")[0].split("/photos")[0].split("/gallery")[0]
def is_valid_email(email):
    if not isinstance(email, str) or "@" not in email or email.count("@") != 1:
        return False
    if email.lower() in [x.lower() for x in settings["forbidden_emails"]]:
        return False
    username, domain = email.split("@", 1)
    if not username or not domain:
        return False
    allowed_tlds = (".ru", ".com", ".bk", ".net", ".org", ".by", ".ua")
    if not any(domain.lower().endswith(ad) for ad in allowed_tlds):
        return False
    tld = domain.rsplit('.', 1)[-1]
    if not tld.isalpha() or len(tld) < 2:
        return False
    return True
def normalize_site(site_url):
    s = (site_url or '').replace("https://", "").replace("http://", "").replace("www.", "").strip().rstrip('/')
    return s.lower()
def black_domain(site):
    if not site:
        return False
    return any(domain in site for domain in settings["black_domains"])
def join_unique(items, limit=3):
    uniq = []
    for x in items:
        x = x.strip()
        if x and x not in uniq:
            uniq.append(x)
        if len(uniq) >= limit:
            break
    return '; '.join(uniq)
def log_to_queue(msg):
    log_queue.put(msg)

class ParserThread(QThread):
    log_signal = Signal(str)
    finished_signal = Signal(int)
    show_scroll = Signal()

    def __init__(self, search_query, company_limit, owner):
        super().__init__()
        self.search_query = search_query
        self.company_limit = company_limit
        self.owner = owner
        self.resume_event = threading.Event()

    def log_func(self, msg):
        self.log_signal.emit(msg)

    def on_scroll_dialog_done(self):
        self.resume_event.set()

    def run(self):
        import requests, urllib.parse
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.keys import Keys
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from bs4 import BeautifulSoup
        try:
            from webdriver_manager.chrome import ChromeDriverManager
            service = Service(ChromeDriverManager().install())
        except Exception:
            service = Service()
        global parser_stop_event, parser_pause_event
        T = settings["timings"]
        MESSENGER = settings["messenger_links"]
        CONTACT_PAGES = settings["contact_pages"]
        if self.company_limit and str(self.company_limit).strip().isdigit():
            company_limit = int(self.company_limit)
        else:
            company_limit = None
        log_filename = os.path.join(LOG_DIR, f"log_{datetime.datetime.now():%Y%m%d_%H%M%S}.txt")
        log_file = open(log_filename, "w", encoding="utf-8")
        if os.path.exists(EXCEL_FILENAME):
            try: df_main = pd.read_excel(EXCEL_FILENAME)
            except Exception: df_main = pd.DataFrame()
        else: df_main = pd.DataFrame()
        tri_index = set()
        if (not df_main.empty and "Название" in df_main.columns and "Адрес" in df_main.columns and "Сайт ЯндексКарты" in df_main.columns):
            for _, row in df_main.iterrows():
                n = str(row.get("Название", "")).strip().lower()
                a = str(row.get("Адрес", "")).strip().lower()
                s = normalize_site(str(row.get("Сайт ЯндексКарты", "")))
                tri_index.add((n, a, s))
        now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        def find_sites_from_yandex_via_selenium(driver, company_name, city):
            self.log_func("=== Поиск сайта через Яндекс (selenium) ===")
            search_variants = [f"{company_name} {city} сайт",f"{company_name.replace('-', ' ')} {city} сайт"]
            for query in search_variants:
                self.log_func(f"\nYandex search query: {query}")
                url = "https://yandex.ru/search/?text=" + urllib.parse.quote_plus(query)
                self.log_func(f"Открываю браузер с Яндекс-поиском: {url}")
                try:
                    driver.execute_script("window.open('');")
                    driver.switch_to.window(driver.window_handles[-1])
                    driver.get(url)
                    time.sleep(T["yandex_search_wait"])
                    html = driver.page_source
                    if "smart-captcha" in html or "Капча" in html or "captcha" in html:
                        self.log_func(f"⚠ Обнаружена капча! Решите вручную, жду {T['captcha_wait']} сек.")
                        time.sleep(T["captcha_wait"])
                        html = driver.page_source
                    soup = BeautifulSoup(html, "html.parser")
                    all_links = []
                    for a in soup.find_all('a', href=True):
                        href = a['href']
                        if any(x in href for x in MESSENGER): continue
                        if (href.startswith("http") and not black_domain(href) and ".jpg" not in href and ".png" not in href):
                            if href not in all_links and len(all_links) < 3:
                                all_links.append(href)
                    driver.close()
                    driver.switch_to.window(driver.window_handles[0])
                    if all_links:
                        for i, site in enumerate(all_links, 1):
                            self.log_func(f"{i}) {site}")
                        return all_links
                except Exception as e:
                    self.log_func(f"Ошибка поиска через Яндекс: {e}")
                    try:
                        if len(driver.window_handles) > 1: driver.close()
                        driver.switch_to.window(driver.window_handles[0])
                    except Exception: pass
            self.log_func("! Нет ни одной нормальной ссылки")
            return []
        def parse_contacts_from_site(site_url):
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                     "AppleWebKit/537.36 (KHTML, like Gecko) "
                                     "Chrome/120.0.0.0 Safari/537.36"}
            pages_to_check = [""] if black_domain(site_url) else list(CONTACT_PAGES)
            found_emails, found_phones = [], []
            for pageurl in pages_to_check:
                if black_domain(site_url) and pageurl != "": continue
                try:
                    url = site_url.rstrip("/") + pageurl
                    self.log_func(f"Загружаем: {url}")
                    time.sleep(T["between_site_pages"])
                    r = requests.get(url, timeout=T["site_request_timeout"], headers=headers, verify=False)
                    r.encoding = r.apparent_encoding
                    text = r.text
                    emails = re.findall(r'[\w\.-]+@[\w\.-]+', text)
                    for e in emails:
                        if is_valid_email(e) and e not in found_emails:
                            found_emails.append(e)
                    phones = re.findall(r'\+7[\d\-\(\) ]{10,15}', text)
                    for p in phones:
                        p = p.strip()
                        if p not in found_phones:
                            found_phones.append(p)
                    if found_emails: break
                except Exception as e:
                    self.log_func(f"Ошибка запроса: {e}")
                    continue
            return join_unique(found_emails), join_unique(found_phones)
        driver = None
        try:
            options = webdriver.ChromeOptions()
            options.add_argument("--start-maximized")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_experimental_option('excludeSwitches', ['enable-logging'])
            driver = webdriver.Chrome(service=service, options=options)
            wait = WebDriverWait(driver, T["selenium_wait_timeout"])
            driver.get("https://yandex.ru/maps")
            self.log_func("Открыт Яндекс.Карты")
            search_input = wait.until(EC.presence_of_element_located((By.TAG_NAME, "input")))
            self.log_func("Нашли поле поиска")
            search_input.send_keys(self.search_query)
            search_input.send_keys(Keys.ENTER)
            time.sleep(T["after_search_enter"])
            self.resume_event.clear()
            self.show_scroll.emit()
            self.resume_event.wait()
            cards = driver.find_elements(By.CSS_SELECTOR, "a[href*='/org/']")
            self.log_func(f"После прокрутки найдено карточек: {len(cards)}")
            seen_links = set()
            unique_card_pairs = []
            for card in cards:
                try:
                    link = card.get_attribute("href")
                    name = card.text.strip() if card.text else ""
                    if link and "/org/" in link and not any(x in link for x in MESSENGER):
                        normalized_link = cut_to_main_yamaps_card(link)
                        if normalized_link not in seen_links:
                            seen_links.add(normalized_link)
                            unique_card_pairs.append((name, normalized_link))
                except Exception: pass
            self.log_func(f"Уникальных карточек: {len(unique_card_pairs)}")
            if df_main.shape[0] > 0 and "Название" in df_main.columns:
                names_in_db = set(str(n).strip().lower() for n in df_main["Название"].dropna().unique())
            else: names_in_db = set()
            new_pairs, dupe_count = [], 0
            for name, link in unique_card_pairs:
                if name and name.strip().lower() in names_in_db: dupe_count += 1
                else: new_pairs.append((name, link))
            self.log_func("\n====== СТАТИСТИКА ПАРСИНГА ======")
            self.log_func(f"Уникальных компаний для парсинга: {len(new_pairs)}")
            self.log_func(f"Дубликатов (по имени в базе): {dupe_count}")
            if company_limit:
                self.log_func(f"Лимит парсинга: {company_limit} компаний")
            self.log_func("==========================\n")
            parser_stop_event.clear()
            companies_count = 0
            for idx, (name, link) in enumerate(new_pairs, 1):
                if parser_stop_event.is_set():
                    self.log_func("Операция остановлена оператором!")
                    break
                if company_limit and companies_count >= company_limit:
                    self.log_func(f"Достигнут лимит: {company_limit} компаний.")
                    break
                while parser_pause_event.is_set():
                    self.log_func("ПАРСЕР на паузе...")
                    time.sleep(1)
                    if parser_stop_event.is_set():
                        self.log_func("Операция остановлена оператором на паузе!")
                        break
                if parser_stop_event.is_set(): break
                self.log_func(f"\n=== Парсим карточку {idx}/{len(new_pairs)} ===\nСсылка: {link}")
                try:
                    driver.get(link)
                    time.sleep(T["loading_card"])
                    soup = BeautifulSoup(driver.page_source, "html.parser")
                    try:
                        actual_name = driver.find_element(By.TAG_NAME, "h1").text.strip()
                    except Exception: actual_name = name or ""
                    actual_address = ""
                    try:
                        address_elem = soup.find("a", class_="business-contacts-view__address-link")
                        if address_elem: actual_address = address_elem.text.strip()
                    except Exception: pass
                    yacards_site = ""
                    try:
                        url_div = soup.find("div", class_="business-urls-view__url")
                        if url_div:
                            a_tag = url_div.find("a", class_="business-urls-view__link")
                            if a_tag and a_tag.has_attr("href"):
                                yacards_site = a_tag["href"].strip()
                    except Exception: pass
                    norm_name = (actual_name or '').strip().lower()
                    norm_addr = (actual_address or '').strip().lower()
                    norm_yacards = normalize_site(yacards_site)
                    key_tri = (norm_name, norm_addr, norm_yacards)
                    if key_tri in tri_index:
                        self.log_func(f"Пропущено по триплет-дублю: '{norm_name}' / '{norm_addr}' / '{norm_yacards}'")
                        continue
                    self.log_func(f"Проходит контроль дублей: '{norm_name}' / '{norm_addr}'")
                    phone = ""
                    try:
                        phone_elem = driver.find_element(By.CSS_SELECTOR, ".orgpage-phones-view__phone-number")
                        phone = phone_elem.text.strip()
                    except Exception:
                        try: phone = driver.find_element(By.XPATH, "//a[contains(@href,'tel')]").text.strip()
                        except Exception: phone = ""
                    self.log_func(f"Телефон (Яндекс): {phone or 'не найден'}")
                    self.log_func(f"Адрес: {actual_address or 'не найден'}")
                    occupation = ""
                    try:
                        occupation_items = []
                        cats_div = soup.find("div", class_="orgpage-categories-info-view")
                        if cats_div:
                            for span in cats_div.find_all("span", class_="button__text"):
                                txt = span.get_text(strip=True)
                                if txt and txt not in occupation_items:
                                    occupation_items.append(txt)
                        occupation = "; ".join(occupation_items)
                    except Exception: pass
                    self.log_func(f"Деятельность: {occupation or 'не найдена'}")
                    email = ""
                    website = ""
                    site_phones = ""
                    try:
                        page_source = driver.page_source
                        emails = re.findall(r'[\w\.-]+@[\w\.-]+', page_source)
                        found_good = [e for e in emails if is_valid_email(e)]
                        seen = set(); deduped = []
                        for e in found_good:
                            if e.lower() not in seen:
                                seen.add(e.lower())
                                deduped.append(e)
                        email = join_unique(deduped)
                        if email:
                            self.log_func(f"Email на Я.Картах: {email}")
                        else:
                            self.log_func("Email на Яндекс.Картах не найден.")
                    except Exception as e:
                        self.log_func(f"Ошибка поиска email: {e}")
                    if not email:
                        try:
                            site_element = driver.find_element(
                                By.XPATH,
                                "//a[contains(@href,'http') and not(contains(@href,'yandex'))]"
                            )
                            website = site_element.get_attribute("href")
                            if website and (any(x in website for x in MESSENGER) or black_domain(website)):
                                self.log_func(f"Сайт исключён (мессенджер/чёрный список): {website}")
                                website = ""
                            elif website:
                                self.log_func(f"Парсим email с сайта: {website}")
                        except Exception: website = ""
                        if website:
                            try:
                                email_site, phones_site = parse_contacts_from_site(website)
                                if email_site:
                                    email = email_site
                                    site_phones = phones_site
                                    self.log_func(f"Email с сайта: {email_site}")
                                elif phones_site:
                                    site_phones = phones_site
                            except Exception as e:
                                self.log_func(f"Ошибка обхода сайта: {e}")
                    if not email:
                        self.log_func("Ищем по top-3 сайтов из Яндекс.Поиска...")
                        city = self.search_query.split()[-1] if self.search_query.strip() else ""
                        found_sites = find_sites_from_yandex_via_selenium(driver, actual_name or name, city)
                        for i, site in enumerate(found_sites, 1):
                            if any(x in site for x in MESSENGER) or black_domain(site):
                                self.log_func(f"Пропущена ссылка-исключение: {site}")
                                continue
                            self.log_func(f"Пробуем сайт #{i}: {site}")
                            try:
                                email_candidate, phone_candidate = parse_contacts_from_site(site)
                                if email_candidate:
                                    email = email_candidate
                                    website = site
                                    self.log_func(f"Найден email на сайте #{i}: {email}")
                                    break
                                if phone_candidate and not site_phones:
                                    site_phones = phone_candidate
                            except Exception as e:
                                self.log_func(f"Ошибка парсинга сайта: {e}")
                        if not email:
                            self.log_func("Email не найден ни на одном сайте.")
                    new_info = {
                        "Дата поиска": now_str, "Запрос": self.search_query, "Название": actual_name, "Телефон (Яндекс)": phone,
                        "Телефон (сайт)": site_phones, "Email": email,
                        "Сайт": website, "Адрес": actual_address,
                        "Описание деятельности": occupation, "Сайт ЯндексКарты": yacards_site
                    }
                    try:
                        df_add = pd.DataFrame([new_info])
                        if os.path.exists(EXCEL_FILENAME):
                            df_existing = pd.read_excel(EXCEL_FILENAME)
                            df_final = pd.concat([df_existing, df_add], ignore_index=True)
                        else:
                            df_final = df_add
                        df_final.to_excel(EXCEL_FILENAME, index=False)
                        self.log_func("✅ Сохранено в базу.")
                    except Exception as e:
                        self.log_func(f"⚠ Ошибка сохранения в Excel: {e}")
                    tri_index.add(key_tri)
                    companies_count += 1
                    self.log_func(
                        f"--- Итог по карточке ---\n"
                        f"Email: {email}\nТелефон (Яндекс): {phone}\n"
                        f"Телефон (сайт): {site_phones}\nСайт: {website}\n"
                        f"Адрес: {actual_address}\nОписание: {occupation}"
                    )
                except Exception as e:
                    self.log_func(f"ОШИБКА ГЛАВНОГО ЦИКЛА: {e}")
            self.log_func(f"\n{'=' * 40}")
            self.log_func(f"Парсинг завершён. Добавлено компаний: {companies_count}")
            if companies_count == 0:
                self.log_func("Ни одной новой компании не добавлено (всё дубли или пусто)")
            self.log_func(f"{'=' * 40}")
        except Exception as e:
            self.log_func(f"КРИТИЧЕСКАЯ ОШИБКА: {e}")
        finally:
            if driver:
                try: driver.quit()
                except Exception: pass
            if log_file:
                try: log_file.close()
                except Exception: pass
            self.finished_signal.emit(0)

class SettingsDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Настройки")
        self.setMinimumSize(560, 400)
        layout = QVBoxLayout(self)
        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)
        timings_tab = QWidget()
        timings_layout = QFormLayout(timings_tab)
        self.timing_inputs = {}
        for key, title in {
            "after_search_enter":    "Ожидание после поиска (сек)",
            "loading_card":          "З��грузка карточки (сек)",
            "yandex_search_wait":    "Ожидание Яндекс-поиска (сек)",
            "captcha_wait":          "Ждать прохождения капчи (сек)",
            "between_site_pages":    "Пауза между страницами сайта (сек)",
            "site_request_timeout":  "Таймаут HTTP-запроса (сек)",
            "selenium_wait_timeout": "Таймаут ожидания Selenium (сек)",
        }.items():
            le = QLineEdit(str(settings["timings"].get(key, DEFAULT_SETTINGS["timings"][key])))
            self.timing_inputs[key] = le
            timings_layout.addRow(QLabel(title), le)
        self.tabs.addTab(timings_tab, "Тайминги")
        def list_editor(tabname, key, label):
            tab = QWidget()
            v = QVBoxLayout(tab)
            lw = QListWidget()
            lw.addItems([str(i) for i in settings[key]])
            v.addWidget(QLabel(label))
            v.addWidget(lw)
            h = QHBoxLayout()
            le = QLineEdit()
            h.addWidget(le)
            add_btn = QPushButton("Добавить")
            del_btn = QPushButton("Удалить")
            h.addWidget(add_btn)
            h.addWidget(del_btn)
            v.addLayout(h)
            add_btn.clicked.connect(lambda: lw.addItem(le.text()) or le.setText(""))
            del_btn.clicked.connect(lambda: lw.takeItem(lw.currentRow()))
            setattr(self, f'{key}_lw', lw)
            self.tabs.addTab(tab, tabname)
        list_editor("Чёрный список", "black_domains", "Black domains:")
        list_editor("Мессенджеры", "messenger_links", "Messenger links:")
        list_editor("Запр. email", "forbidden_emails", "Запрещённые email:")
        list_editor("Страницы", "contact_pages", "Страницы контактов:")
        btn_box = QHBoxLayout()
        save_btn = QPushButton("Сохранить")
        cancel_btn = QPushButton("Отмена")
        btn_box.addWidget(save_btn)
        btn_box.addWidget(cancel_btn)
        layout.addLayout(btn_box)
        save_btn.clicked.connect(self.save_all)
        cancel_btn.clicked.connect(self.reject)
    def save_all(self):
        for k, w in self.timing_inputs.items():
            try: t = float(w.text().replace(",", ".")); assert t >= 0
            except Exception:
                QMessageBox.warning(self, "Ошибка", f"Некорректное значение для '{k}'")
                return
        def lw_to_list(n): return [self.__dict__[f'{n}_lw'].item(i).text() for i in range(self.__dict__[f'{n}_lw'].count())]
        settings["timings"] = {k: float(w.text().replace(",", ".")) for k, w in self.timing_inputs.items()}
        settings["black_domains"] = lw_to_list('black_domains')
        settings["messenger_links"] = lw_to_list('messenger_links')
        settings["forbidden_emails"] = lw_to_list('forbidden_emails')
        settings["contact_pages"] = lw_to_list('contact_pages')
        save_settings(settings)
        self.accept()

class DatabaseViewer(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Просмотр базы")
        self.resize(1200, 700)
        self.table = QTableWidget()
        ly = QVBoxLayout(self)
        ly.addWidget(self.table)
        self.load_db()
    def load_db(self):
        if not os.path.exists(EXCEL_FILENAME): return
        df = pd.read_excel(EXCEL_FILENAME)
        self.table.setRowCount(len(df))
        self.table.setColumnCount(len(df.columns))
        self.table.setHorizontalHeaderLabels(df.columns)
        for rownr, row in df.iterrows():
            for colnr, col in enumerate(df.columns):
                val = '' if pd.isna(row[col]) else str(row[col])
                self.table.setItem(rownr, colnr, QTableWidgetItem(val))
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.table.verticalHeader().setVisible(False)

class MainWin(QMainWindow):
    def __init__(self, app):
        super().__init__()
        self.app = app
        self.setWindowTitle("Яндекс-Карты КонтактПарсер")
        self.resize(1000, 800)
        cw = QWidget()
        self.setCentralWidget(cw)
        v_main = QVBoxLayout(cw)

        # ==================== МЕНЮ ФАЙЛ ====================
        menu_row = QHBoxLayout()
        file_btn = QPushButton("Файл")
        file_btn.setMinimumWidth(70)
        file_btn.clicked.connect(self.open_menu)
        menu_row.addWidget(file_btn)
        menu_row.addStretch()
        v_main.addLayout(menu_row)

        # =============== Поисковый блок ==============
        form = QFormLayout()
        self.query_in = QLineEdit("металлообработка Подольск")
        form.addRow(QLabel("Поисковый запрос:"), self.query_in)
        hl = QHBoxLayout()
        self.limit_in = QLineEdit("")
        self.limit_in.setMinimumWidth(110)
        hl.addWidget(QLabel("Сколько компаний парсить? (пусто = все):"))
        hl.addWidget(self.limit_in)
        form.addRow(hl)
        v_main.addLayout(form)

        btns = QHBoxLayout()
        self.parse_btn = QPushButton("Начать парсинг")
        self.parse_btn.clicked.connect(self.do_parse)
        btns.addWidget(self.parse_btn)
        self.pause_btn = QPushButton("⏸ Пауза")
        self.pause_btn.clicked.connect(lambda: parser_pause_event.set())
        btns.addWidget(self.pause_btn)
        self.resume_btn = QPushButton("▶ Продолжить")
        self.resume_btn.clicked.connect(lambda: parser_pause_event.clear())
        btns.addWidget(self.resume_btn)
        self.stop_btn = QPushButton("⏹ Остановить")
        self.stop_btn.clicked.connect(lambda: parser_stop_event.set())
        btns.addWidget(self.stop_btn)
        db_btn = QPushButton("📊 Посмотреть Базу")
        db_btn.clicked.connect(self.open_db_view)
        btns.addWidget(db_btn)
        v_main.addLayout(btns)

        v_main.addWidget(QLabel("Сообщения отладки / ход работы:"))
        self.log_box = QTextEdit()
        self.log_box.setReadOnly(True)
        v_main.addWidget(self.log_box, stretch=2)

        self.db_viewer = None
        self.parser_thread = None

        self.log_timer = threading.Timer(0.15, self.process_log_queue)
        self.start_log_timer = True
        self.process_log_queue()

    def closeEvent(self, e):
        self.start_log_timer = False
        super().closeEvent(e)

    def process_log_queue(self):
        try:
            while True:
                msg = log_queue.get_nowait()
                self.log_box.setReadOnly(False)
                self.log_box.append(msg)
                self.log_box.setReadOnly(True)
        except Empty:
            pass
        if self.start_log_timer:
            threading.Timer(0.15, self.process_log_queue).start()

    def open_settings(self):
        dlg = SettingsDialog(self)
        dlg.exec()

    def open_menu(self):
        msgbox = QMessageBox(self)
        msgbox.setWindowTitle("Меню")
        msgbox.setText("Что сделать?")
        s_btn = msgbox.addButton("Настройки", QMessageBox.ActionRole)
        e_btn = msgbox.addButton("Выход", QMessageBox.RejectRole)
        msgbox.exec()
        if msgbox.clickedButton() == s_btn:
            self.open_settings()
        elif msgbox.clickedButton() == e_btn:
            QApplication.quit()

    def open_db_view(self):
        if not os.path.exists(EXCEL_FILENAME):
            QMessageBox.information(self, "Нет базы", "Файл базы ещё не создан. Сначала сделайте хотя бы один парсинг.")
            return
        if self.db_viewer is None:
            self.db_viewer = DatabaseViewer(self)
        self.db_viewer.load_db()
        self.db_viewer.show()

    def do_parse(self):
        query = self.query_in.text().strip()
        limit = self.limit_in.text().strip()
        if not query:
            QMessageBox.warning(self, "Ошибка", "Введите поисковый запрос!")
            return
        self.parse_btn.setEnabled(False)
        parser_stop_event.clear()
        parser_pause_event.clear()
        self.parser_thread = ParserThread(query, limit, self)
        self.parser_thread.log_signal.connect(lambda m: log_to_queue(m))
        self.parser_thread.finished_signal.connect(lambda n: self.parse_btn.setEnabled(True))
        self.parser_thread.show_scroll.connect(self.on_scroll_dialog)
        self.parser_thread.start()

    def on_scroll_dialog(self):
        QMessageBox.information(
            self, "Ручная прокрутка",
            "Прокрутите список организаций в Яндекс.Картах до НИЗУ ВРУЧНУЮ (мышкой, колесиком или PageDown), чтобы ВСЕ компании появились на странице.\n\nПосле этого нажмите OK.")
        # Сигналим в поток, что можно продолжать
        if self.parser_thread:
            self.parser_thread.on_scroll_dialog_done()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyleSheet(QSS)
    win = MainWin(app)
    win.show()
    sys.exit(app.exec())