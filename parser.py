import customtkinter as ctk
import threading
import datetime
import json
import tkinter as tk
from tkinter.scrolledtext import ScrolledText
from tkinter import ttk, messagebox
import pandas as pd
import os
import sys
import re
from threading import Event
from queue import Queue, Empty
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==================== Базовая директория (для PyInstaller) ====================
if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
else:
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))

os.chdir(BASE_DIR)

# ==================== Настройки JSON ====================
SETTINGS_FILE = os.path.join(BASE_DIR, "settings.json")

DEFAULT_SETTINGS = {
    "timings": {
        "after_search_enter": 5,
        "loading_card": 4,
        "yandex_search_wait": 8,
        "captcha_wait": 15,
        "between_site_pages": 2.5,
        "site_request_timeout": 8,
        "selenium_wait_timeout": 15,
    },
    "black_domains": [
        "vk.com", "avito.ru", "avito.com", "hh.ru", "ok.ru", "youtube.com",
        "facebook.com", "instagram.com", "twitter.com", "t.me", "2gis.ru",
        ".yandex.", "ya.ru", "mail.ru", "rb.ru", "google.com", "zoon.ru",
        "orgpage.ru", "google.ru", "yandex.ru/maps", "rusprofile.ru",
        ".clients.site", ".orgsinfo.ru", ".jsprav.ru", "yandex.ru/profile",
        "ruscatalog.org"
    ],
    "messenger_links": [
        "wa.me/", "t.me/", "viber.me/", "viber://", "telegram.me/", "telegram.org/"
    ],
    "forbidden_emails": [
        "support@maps.yandex.ru",
        "webmaps-revolution@yandex-team.ru",
        "m-maps@support.yandex.ru"
    ],
    "contact_pages": [
        "", "/contacts", "/contact", "/kontakty", "/kontakt",
        "/about", "/about-us", "/company", "/info", "/contact-us"
    ]
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
        messagebox.showerror("Ошибка", f"Не удалось сохранить настройки:\n{e}")


settings = load_settings()

EXCEL_FILENAME = os.path.join(BASE_DIR, "contacts_database.xlsx")
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

parser_stop_event = Event()
parser_pause_event = Event()
log_file = None
log_queue = Queue()

# ==================== Цвета тёмной темы ====================
C_BG = "#1a1a1a"
C_BG2 = "#212223"
C_BG3 = "#2b2b2b"
C_FG = "#d6d6d6"
C_ACCENT = "#1f6aa5"
C_BORDER = "#444444"


# ==================== Утилиты ====================

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


# ==================== Потокобезопасный лог ====================

def log_to_queue(msg):
    log_queue.put(msg)


def process_log_queue():
    global log_file
    try:
        while True:
            msg = log_queue.get_nowait()
            log_text.configure(state="normal")
            log_text.insert("end", msg + "\n")
            log_text.see("end")
            log_text.configure(state="disabled")
            try:
                if log_file:
                    log_file.write(msg + '\n')
                    log_file.flush()
            except Exception:
                pass
    except Empty:
        pass
    root.after(100, process_log_queue)


dialog_event = Event()


def ask_manual_scroll():
    messagebox.showinfo(
        "Ручная Прокрутка",
        "Прокрутите список организаций в Яндекс.Картах до НИЗУ ВРУЧНУЮ "
        "(мышкой, колесиком или PageDown), чтобы ВСЕ компании появились на странице.\n\n"
        "После этого нажмите OK для запуска парсинга."
    )
    dialog_event.set()


# ==================== Основной парсер ====================

def run_parser(search_query, log_func, company_limit=None):
    import time
    import requests
    import urllib.parse
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

    global parser_stop_event, parser_pause_event, log_file

    T = settings["timings"]
    MESSENGER = settings["messenger_links"]
    CONTACT_PAGES = settings["contact_pages"]

    if company_limit and str(company_limit).strip().isdigit():
        company_limit = int(company_limit)
    else:
        company_limit = None

    log_filename = os.path.join(
        LOG_DIR, f"log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    )
    log_file = open(log_filename, "w", encoding="utf-8")

    if os.path.exists(EXCEL_FILENAME):
        try:
            df_main = pd.read_excel(EXCEL_FILENAME)
        except Exception:
            df_main = pd.DataFrame()
    else:
        df_main = pd.DataFrame()

    tri_index = set()
    if (not df_main.empty
            and "Название" in df_main.columns
            and "Адрес" in df_main.columns
            and "Сайт ЯндексКарты" in df_main.columns):
        for _, row in df_main.iterrows():
            n = str(row.get("Название", "")).strip().lower()
            a = str(row.get("Адрес", "")).strip().lower()
            s = normalize_site(str(row.get("Сайт ЯндексКарты", "")))
            tri_index.add((n, a, s))

    now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def find_sites_from_yandex_via_selenium(driver, company_name, city):
        log_func("=== Поиск сайта через Яндекс (selenium) ===")
        search_variants = [
            f"{company_name} {city} сайт",
            f"{company_name.replace('-', ' ')} {city} сайт"
        ]
        for query in search_variants:
            log_func(f"\nYandex search query: {query}")
            url = "https://yandex.ru/search/?text=" + urllib.parse.quote_plus(query)
            log_func(f"Открываю браузер с Яндекс-поиском: {url}")
            try:
                driver.execute_script("window.open('');")
                driver.switch_to.window(driver.window_handles[-1])
                driver.get(url)
                time.sleep(T["yandex_search_wait"])
                html = driver.page_source
                if "smart-captcha" in html or "Капча" in html or "captcha" in html:
                    log_func(f"⚠ Обнаружена капча! Решите вручную, жду {T['captcha_wait']} сек.")
                    time.sleep(T["captcha_wait"])
                    html = driver.page_source
                soup = BeautifulSoup(html, "html.parser")
                all_links = []
                for a in soup.find_all('a', href=True):
                    href = a['href']
                    if any(x in href for x in MESSENGER):
                        continue
                    if (href.startswith("http")
                            and not black_domain(href)
                            and ".jpg" not in href
                            and ".png" not in href):
                        if href not in all_links and len(all_links) < 3:
                            all_links.append(href)
                driver.close()
                driver.switch_to.window(driver.window_handles[0])
                if all_links:
                    for i, site in enumerate(all_links, 1):
                        log_func(f"{i}) {site}")
                    return all_links
            except Exception as e:
                log_func(f"Ошибка поиска через Яндекс: {e}")
                try:
                    if len(driver.window_handles) > 1:
                        driver.close()
                    driver.switch_to.window(driver.window_handles[0])
                except Exception:
                    pass
        log_func("! Нет ни одной нормальной ссылки")
        return []

    def parse_contacts_from_site(site_url):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36"
        }
        if black_domain(site_url):
            pages_to_check = [""]
        else:
            pages_to_check = list(CONTACT_PAGES)
        found_emails, found_phones = [], []
        for pageurl in pages_to_check:
            if black_domain(site_url) and pageurl != "":
                continue
            try:
                url = site_url.rstrip("/") + pageurl
                log_func(f"Загружаем: {url}")
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
                if found_emails:
                    break
            except Exception as e:
                log_func(f"Ошибка запроса: {e}")
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
        log_func("Открыт Яндекс.Карты")

        search_input = wait.until(EC.presence_of_element_located((By.TAG_NAME, "input")))
        log_func("Нашли поле поиска")
        search_input.send_keys(search_query)
        search_input.send_keys(Keys.ENTER)
        time.sleep(T["after_search_enter"])

        log_func("Прокрутите список компаний Яндекс.Карт до конца. Затем нажмите OK в появившемся окне.")
        dialog_event.clear()
        root.after(0, ask_manual_scroll)
        dialog_event.wait()

        cards = driver.find_elements(By.CSS_SELECTOR, "a[href*='/org/']")
        log_func(f"После прокрутки найдено карточек: {len(cards)}")

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
            except Exception:
                pass

        log_func(f"Уникальных карточек: {len(unique_card_pairs)}")

        if df_main.shape[0] > 0 and "Название" in df_main.columns:
            names_in_db = set(str(n).strip().lower() for n in df_main["Название"].dropna().unique())
        else:
            names_in_db = set()

        new_pairs = []
        dupe_count = 0
        for name, link in unique_card_pairs:
            if name and name.strip().lower() in names_in_db:
                dupe_count += 1
            else:
                new_pairs.append((name, link))

        log_func("\n====== СТАТИСТИКА ПАРСИНГА ======")
        log_func(f"Уникальных компаний для парсинга: {len(new_pairs)}")
        log_func(f"Дубликатов (по имени в базе): {dupe_count}")
        if company_limit:
            log_func(f"Лимит парсинга: {company_limit} компаний")
        log_func("==========================\n")

        parser_stop_event.clear()
        companies_count = 0

        for idx, (name, link) in enumerate(new_pairs, 1):
            if parser_stop_event.is_set():
                log_func("Операция остановлена оператором!")
                break
            if company_limit and companies_count >= company_limit:
                log_func(f"Достигнут лимит: {company_limit} компаний.")
                break
            while parser_pause_event.is_set():
                log_func("ПАРСЕР на паузе...")
                time.sleep(1)
                if parser_stop_event.is_set():
                    log_func("Операция остановлена оператором на паузе!")
                    break
            if parser_stop_event.is_set():
                break

            log_func(f"\n=== Парсим карточку {idx}/{len(new_pairs)} ===\nСсылка: {link}")

            try:
                driver.get(link)
                time.sleep(T["loading_card"])
                soup = BeautifulSoup(driver.page_source, "html.parser")

                try:
                    actual_name = driver.find_element(By.TAG_NAME, "h1").text.strip()
                except Exception:
                    actual_name = name or ""

                actual_address = ""
                try:
                    address_elem = soup.find("a", class_="business-contacts-view__address-link")
                    if address_elem:
                        actual_address = address_elem.text.strip()
                except Exception:
                    pass

                yacards_site = ""
                try:
                    url_div = soup.find("div", class_="business-urls-view__url")
                    if url_div:
                        a_tag = url_div.find("a", class_="business-urls-view__link")
                        if a_tag and a_tag.has_attr("href"):
                            yacards_site = a_tag["href"].strip()
                except Exception:
                    pass

                norm_name = (actual_name or '').strip().lower()
                norm_addr = (actual_address or '').strip().lower()
                norm_yacards = normalize_site(yacards_site)
                key_tri = (norm_name, norm_addr, norm_yacards)
                if key_tri in tri_index:
                    log_func(f"Пропущено по триплет-дублю: '{norm_name}' / '{norm_addr}' / '{norm_yacards}'")
                    continue
                log_func(f"Проходит контроль дублей: '{norm_name}' / '{norm_addr}'")

                phone = ""
                try:
                    phone_elem = driver.find_element(By.CSS_SELECTOR, ".orgpage-phones-view__phone-number")
                    phone = phone_elem.text.strip()
                except Exception:
                    try:
                        phone = driver.find_element(By.XPATH, "//a[contains(@href,'tel')]").text.strip()
                    except Exception:
                        phone = ""
                log_func(f"Телефон (Яндекс): {phone or 'не найден'}")
                log_func(f"Адрес: {actual_address or 'не найден'}")

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
                except Exception:
                    pass
                log_func(f"Деятельность: {occupation or 'не найдена'}")

                email = ""
                website = ""
                site_phones = ""

                # 1. Email на Яндекс.Картах
                try:
                    page_source = driver.page_source
                    emails = re.findall(r'[\w\.-]+@[\w\.-]+', page_source)
                    found_good = [e for e in emails if is_valid_email(e)]
                    seen = set()
                    deduped = []
                    for e in found_good:
                        if e.lower() not in seen:
                            seen.add(e.lower())
                            deduped.append(e)
                    email = join_unique(deduped)
                    if email:
                        log_func(f"Email на Я.Картах: {email}")
                    else:
                        log_func("Email на Яндекс.Картах не найден.")
                except Exception as e:
                    log_func(f"Ошибка поиска email: {e}")

                # 2. Email на сайте с Я.Карт
                if not email:
                    try:
                        site_element = driver.find_element(
                            By.XPATH,
                            "//a[contains(@href,'http') and not(contains(@href,'yandex'))]"
                        )
                        website = site_element.get_attribute("href")
                        if website and (any(x in website for x in MESSENGER) or black_domain(website)):
                            log_func(f"Сайт исключён (мессенджер/чёрный список): {website}")
                            website = ""
                        elif website:
                            log_func(f"Парсим email с сайта: {website}")
                    except Exception:
                        website = ""
                    if website:
                        try:
                            email_site, phones_site = parse_contacts_from_site(website)
                            if email_site:
                                email = email_site
                                site_phones = phones_site
                                log_func(f"Email с сайта: {email_site}")
                            elif phones_site:
                                site_phones = phones_site
                        except Exception as e:
                            log_func(f"Ошибка обхода сайта: {e}")

                # 3. Поиск через Яндекс
                if not email:
                    log_func("Ищем по top-3 сайтов из Яндекс.Поиска...")
                    city = search_query.split()[-1] if search_query.strip() else ""
                    found_sites = find_sites_from_yandex_via_selenium(
                        driver, actual_name or name, city
                    )
                    for i, site in enumerate(found_sites, 1):
                        if any(x in site for x in MESSENGER) or black_domain(site):
                            log_func(f"Пропущена ссылка-исключение: {site}")
                            continue
                        log_func(f"Пробуем сайт #{i}: {site}")
                        try:
                            email_candidate, phone_candidate = parse_contacts_from_site(site)
                            if email_candidate:
                                email = email_candidate
                                website = site
                                log_func(f"Найден email на сайте #{i}: {email}")
                                break
                            if phone_candidate and not site_phones:
                                site_phones = phone_candidate
                        except Exception as e:
                            log_func(f"Ошибка парсинга сайта: {e}")
                    if not email:
                        log_func("Email не найден ни на одном сайте.")

                new_info = {
                    "Дата поиска": now_str, "Запрос": search_query,
                    "Название": actual_name, "Телефон (Яндекс)": phone,
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
                    log_func("✅ Сохранено в базу.")
                except Exception as e:
                    log_func(f"⚠ Ошибка сохранения в Excel: {e}")

                tri_index.add(key_tri)
                companies_count += 1

                log_func(
                    f"--- Итог по карточке ---\n"
                    f"Email: {email}\nТелефон (Яндекс): {phone}\n"
                    f"Телефон (сайт): {site_phones}\nСайт: {website}\n"
                    f"Адрес: {actual_address}\nОписание: {occupation}"
                )

            except Exception as e:
                log_func(f"ОШИБКА ГЛАВНОГО ЦИКЛА: {e}")

        log_func(f"\n{'=' * 40}")
        log_func(f"Парсинг завершён. Добавлено компаний: {companies_count}")
        if companies_count == 0:
            log_func("Ни одной новой компании не добавлено (всё дубли или пусто)")
        log_func(f"{'=' * 40}")

    except Exception as e:
        log_func(f"КРИТИЧЕСКАЯ ОШИБКА: {e}")
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
        if log_file:
            try:
                log_file.close()
            except Exception:
                pass


# ==================== Окно настроек ====================

settings_window = None


def open_settings_window():
    global settings, settings_window

    # Если окно уже открыто — поднять
    if settings_window is not None:
        try:
            if settings_window.winfo_exists():
                settings_window.focus_set()
                settings_window.lift()
                return
        except Exception:
            pass
        settings_window = None

    sw = ctk.CTkToplevel(root)
    settings_window = sw
    sw.title("⚙ Настройки")
    sw.geometry("750x720")
    sw.configure(fg_color=C_BG)

    # НЕ используем grab_set() и transient() — они ломают повторное открытие
    # Просто поднимаем окно наверх
    sw.lift()
    sw.focus_set()

    def on_close():
        global settings_window
        sw.destroy()
        settings_window = None

    sw.protocol("WM_DELETE_WINDOW", on_close)

    tabview = ctk.CTkTabview(sw, width=710, height=580,
                              fg_color=C_BG2,
                              segmented_button_fg_color=C_BG,
                              segmented_button_selected_color=C_ACCENT,
                              segmented_button_selected_hover_color="#1a5a90",
                              segmented_button_unselected_color=C_BG,
                              segmented_button_unselected_hover_color="#333333")
    tabview.pack(padx=15, pady=(15, 5), fill="both", expand=True)

    tab_timings = tabview.add("⏱ Тайминги")
    tab_black = tabview.add("🚫 Чёрный список")
    tab_messenger = tabview.add("💬 Мессенджеры")
    tab_emails = tabview.add("📧 Запр. email")
    tab_pages = tabview.add("📄 Страницы")

    timing_labels = {
        "after_search_enter":    "Ожидание после ввода запроса в Я.Карты",
        "loading_card":          "Загрузка карточки компании",
        "yandex_search_wait":    "Ожидание загрузки Яндекс.Поиска",
        "captcha_wait":          "Ожидание решения капчи вручную",
        "between_site_pages":    "Пауза между страницами сайта",
        "site_request_timeout":  "Таймаут HTTP-запроса к сайту",
        "selenium_wait_timeout": "Таймаут ожидания элемента (Selenium)",
    }
    timing_vars = {}

    for key, label in timing_labels.items():
        row = ctk.CTkFrame(tab_timings, fg_color="transparent")
        row.pack(anchor="w", padx=10, pady=5, fill="x")
        ctk.CTkLabel(row, text=label, anchor="w", width=400, text_color=C_FG).pack(side="left")
        var = ctk.StringVar(value=str(settings["timings"].get(key, DEFAULT_SETTINGS["timings"][key])))
        timing_vars[key] = var
        ctk.CTkEntry(row, textvariable=var, width=70, font=("Consolas", 14),
                     fg_color=C_BG3, text_color=C_FG, border_color=C_BORDER).pack(side="left", padx=(10, 0))
        ctk.CTkLabel(row, text="сек", anchor="w", width=30, text_color="#888888").pack(side="left", padx=(6, 0))

    def create_dark_list_editor(parent, items_list):
        wrapper = ctk.CTkFrame(parent, fg_color="transparent")
        wrapper.pack(padx=10, pady=5, fill="both", expand=True)
        list_frame = ctk.CTkFrame(wrapper, fg_color=C_BG3, corner_radius=6)
        list_frame.pack(fill="both", expand=True)
        scrollbar = tk.Scrollbar(list_frame, orient="vertical",
                                  bg=C_BG3, troughcolor=C_BG2,
                                  activebackground=C_ACCENT, highlightthickness=0)
        listbox = tk.Listbox(
            list_frame, yscrollcommand=scrollbar.set,
            bg=C_BG3, fg=C_FG, font=("Consolas", 12),
            selectbackground=C_ACCENT, selectforeground="white",
            activestyle="none", relief="flat", bd=0,
            highlightthickness=1, highlightbackground=C_BORDER,
            highlightcolor=C_ACCENT
        )
        scrollbar.config(command=listbox.yview)
        listbox.pack(side="left", fill="both", expand=True, padx=(4, 0), pady=4)
        scrollbar.pack(side="right", fill="y", pady=4, padx=(0, 4))
        for item in items_list:
            listbox.insert("end", item)
        btn_frame = ctk.CTkFrame(wrapper, fg_color="transparent")
        btn_frame.pack(pady=(8, 0), fill="x")
        entry_var = ctk.StringVar()
        ctk.CTkEntry(btn_frame, textvariable=entry_var, width=350, font=("Arial", 13),
                     placeholder_text="Введите значение...",
                     fg_color=C_BG3, text_color=C_FG, border_color=C_BORDER,
                     placeholder_text_color="#666666").pack(side="left", padx=(0, 8))

        def add_item():
            val = entry_var.get().strip()
            if val:
                listbox.insert("end", val)
                entry_var.set("")

        def delete_selected():
            sel = listbox.curselection()
            if sel:
                listbox.delete(sel[0])

        ctk.CTkButton(btn_frame, text="➕ Добавить", command=add_item, width=120,
                      fg_color=C_ACCENT).pack(side="left", padx=(0, 5))
        ctk.CTkButton(btn_frame, text="🗑 Удалить", command=delete_selected, width=120,
                      fg_color="#c0392b", hover_color="#e74c3c").pack(side="left")
        return listbox

    lb_black = create_dark_list_editor(tab_black, settings["black_domains"])
    lb_messenger = create_dark_list_editor(tab_messenger, settings["messenger_links"])
    lb_emails = create_dark_list_editor(tab_emails, settings["forbidden_emails"])
    lb_pages = create_dark_list_editor(tab_pages, settings["contact_pages"])

    bottom = ctk.CTkFrame(sw, fg_color="transparent")
    bottom.pack(padx=15, pady=(5, 15), fill="x")

    def do_save():
        global settings, settings_window
        new_timings = {}
        for key, var in timing_vars.items():
            try:
                val = float(var.get().replace(",", "."))
                if val < 0:
                    raise ValueError
                new_timings[key] = val
            except ValueError:
                messagebox.showwarning("Ошибка",
                                       f"Некорректное значение:\n«{timing_labels[key]}» = {var.get()}")
                return

        def lb_to_list(lb):
            return [lb.get(i) for i in range(lb.size())]

        settings = {
            "timings": new_timings,
            "black_domains": lb_to_list(lb_black),
            "messenger_links": lb_to_list(lb_messenger),
            "forbidden_emails": lb_to_list(lb_emails),
            "contact_pages": lb_to_list(lb_pages),
        }
        save_settings(settings)
        messagebox.showinfo("✅ Сохранено",
                            "Настройки сохранены!\nБудут применены при следующем запуске парсинга.")
        sw.destroy()
        settings_window = None

    def do_reset():
        global settings, settings_window
        if messagebox.askyesno("Сброс", "Вернуть все настройки к значениям по умолчанию?"):
            settings = DEFAULT_SETTINGS.copy()
            save_settings(settings)
            messagebox.showinfo("Сброс", "Настройки сброшены.")
            sw.destroy()
            settings_window = None

    ctk.CTkButton(bottom, text="💾 Сохранить", command=do_save,
                  width=200, height=40, font=("Arial", 14),
                  fg_color=C_ACCENT).pack(side="left", padx=(0, 10))
    ctk.CTkButton(bottom, text="🔄 Сбросить по умолчанию", command=do_reset,
                  width=230, height=40, font=("Arial", 14),
                  fg_color="#555555", hover_color="#777777").pack(side="left")


# ==================== GUI ====================
ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("dark-blue")

root = ctk.CTk()
root.title("Яндекс-Карты КонтактПарсер")
root.geometry("950x700")

frame = ctk.CTkFrame(root)
frame.pack(padx=15, pady=(0, 15), fill="both", expand=True)

# ==================== Кастомное тёмное меню (CTk, без tkinter.Menu) ====================
menu_bar = ctk.CTkFrame(root, height=32, fg_color=C_BG, corner_radius=0)
menu_bar.pack(fill="x", side="top", before=frame)
menu_bar.pack_propagate(False)

file_dropdown = None


def toggle_file_menu():
    global file_dropdown
    if file_dropdown is not None and file_dropdown.winfo_exists():
        file_dropdown.destroy()
        file_dropdown = None
        return

    x = btn_file.winfo_rootx() - root.winfo_rootx()
    y = btn_file.winfo_rooty() - root.winfo_rooty() + btn_file.winfo_height()

    file_dropdown = ctk.CTkFrame(root, fg_color=C_BG2, corner_radius=6,
                                  border_width=1, border_color=C_BORDER)
    file_dropdown.place(x=x, y=y)
    file_dropdown.lift()

    def on_settings():
        file_dropdown.destroy()
        open_settings_window()

    def on_exit():
        root.quit()

    ctk.CTkButton(file_dropdown, text="⚙  Настройки", command=on_settings,
                  width=180, height=30, anchor="w",
                  fg_color="transparent", hover_color="#333333",
                  text_color=C_FG, font=("Arial", 13)).pack(padx=4, pady=(4, 0))

    sep = ctk.CTkFrame(file_dropdown, height=1, fg_color=C_BORDER)
    sep.pack(fill="x", padx=8, pady=2)

    ctk.CTkButton(file_dropdown, text="🚪  Выход", command=on_exit,
                  width=180, height=30, anchor="w",
                  fg_color="transparent", hover_color="#333333",
                  text_color=C_FG, font=("Arial", 13)).pack(padx=4, pady=(0, 4))

    def close_on_click(event):
        global file_dropdown
        if file_dropdown and file_dropdown.winfo_exists():
            w = event.widget
            try:
                if not (str(w).startswith(str(file_dropdown)) or w == btn_file):
                    file_dropdown.destroy()
                    file_dropdown = None
            except Exception:
                pass

    root.bind("<Button-1>", close_on_click, add="+")


btn_file = ctk.CTkButton(menu_bar, text="Файл", command=toggle_file_menu,
                          width=60, height=26, font=("Arial", 13),
                          fg_color="transparent", hover_color="#333333",
                          text_color=C_FG, corner_radius=4)
btn_file.pack(side="left", padx=(6, 0), pady=3)

# ==================== Основной интерфейс ====================
query_var = ctk.StringVar(value="металлообработка Подольск")
limit_var = ctk.StringVar(value="")
database_window = None
db_tree = None
db_columns = None


def do_parse():
    btn_parse.configure(state="disabled")
    parser_stop_event.clear()
    parser_pause_event.clear()
    query = query_var.get().strip()
    limit = limit_var.get().strip()
    if not query:
        messagebox.showwarning("Ошибка", "Введите поисковый запрос!")
        btn_parse.configure(state="normal")
        return
    t = threading.Thread(target=run_parser, args=(query, log_to_queue, limit), daemon=True)
    t.start()

    def reenable():
        t.join()
        root.after(0, lambda: btn_parse.configure(state="normal"))
    threading.Thread(target=reenable, daemon=True).start()


ctk.CTkLabel(frame, text="Поисковый запрос:", anchor="w").pack(anchor="w", pady=(4, 4))
ctk.CTkEntry(frame, textvariable=query_var, width=500, font=("Arial", 15)).pack(anchor="w", pady=(0, 12))

ctk.CTkLabel(frame, text="Сколько компаний парсить? (пусто = все)", anchor="w").pack(anchor="w")
ctk.CTkEntry(frame, textvariable=limit_var, width=100, font=("Arial", 15)).pack(anchor="w", pady=(0, 18))

btn_parse = ctk.CTkButton(frame, text="Начать парсинг", command=do_parse, width=200, height=42)
btn_parse.pack(anchor="w", pady=(0, 14))

control_frame = ctk.CTkFrame(frame, fg_color="transparent")
control_frame.pack(anchor="w", pady=(0, 14))

ctk.CTkButton(control_frame, text="⏸ Пауза", command=lambda: parser_pause_event.set(),
              width=140, fg_color="orange").pack(side="left", padx=(0, 8))
ctk.CTkButton(control_frame, text="▶ Продолжить", command=lambda: parser_pause_event.clear(),
              width=140, fg_color="green").pack(side="left", padx=(0, 8))
ctk.CTkButton(control_frame, text="⏹ Остановить", command=lambda: parser_stop_event.set(),
              width=160, height=42, fg_color="red").pack(side="left", padx=(0, 8))

ctk.CTkButton(frame, text="📊 Посмотреть Базу", command=lambda: open_db_view(),
              width=180, height=38).pack(anchor="w", pady=(0, 14))

ctk.CTkLabel(frame, text="Сообщения отладки / ход работы:").pack(anchor="w", pady=(0, 5))
log_text = ScrolledText(frame, height=24, width=100, bg=C_BG2, fg=C_FG,
                         font=("Consolas", 12), wrap="word", state="disabled",
                         insertbackground="white", relief="flat", bd=0,
                         highlightthickness=1, highlightbackground=C_BORDER)
log_text.pack(fill="both", expand=True, pady=(0, 10))


def show_db_table(df):
    global db_tree, db_columns
    if db_tree is None:
        return
    db_tree.delete(*db_tree.get_children())
    for _, row in df.iterrows():
        db_tree.insert("", "end", values=[str(row[c]) if pd.notna(row[c]) else "" for c in db_columns])


def open_db_view():
    global database_window, db_tree, db_columns
    if (database_window is not None
            and hasattr(database_window, "winfo_exists")
            and database_window.winfo_exists()):
        try:
            df = pd.read_excel(EXCEL_FILENAME)
            show_db_table(df)
        except Exception:
            pass
        database_window.focus_set()
        return
    if not os.path.exists(EXCEL_FILENAME):
        messagebox.showinfo("Нет базы", "Файл базы ещё не создан. Сначала сделайте хотя бы один парсинг.")
        return
    df = pd.read_excel(EXCEL_FILENAME)
    database_window = ctk.CTkToplevel(root)
    database_window.title("Просмотр базы данных")
    database_window.geometry("1280x700")
    frm = ctk.CTkFrame(database_window)
    frm.pack(fill="both", expand=True, padx=10, pady=5)
    db_columns = list(df.columns)

    style = ttk.Style()
    style.theme_use("clam")
    style.configure("Dark.Treeview",
                    background=C_BG3, foreground=C_FG, fieldbackground=C_BG3,
                    borderwidth=0, relief="flat", rowheight=28, font=("Arial", 11))
    style.configure("Dark.Treeview.Heading",
                    background=C_BG, foreground=C_FG, relief="flat",
                    borderwidth=0, font=("Arial", 11, "bold"))
    style.map("Dark.Treeview",
              background=[("selected", C_ACCENT)],
              foreground=[("selected", "white")])
    style.map("Dark.Treeview.Heading",
              background=[("active", "#333333")])

    db_tree = ttk.Treeview(frm, show="headings", style="Dark.Treeview")
    db_tree["columns"] = db_columns
    for col in db_columns:
        db_tree.heading(col, text=col, anchor="w")
        db_tree.column(col, width=170, anchor="w")
    db_tree.pack(fill="both", expand=True, side="left")
    vsb = ttk.Scrollbar(frm, orient="vertical", command=db_tree.yview)
    db_tree.configure(yscrollcommand=vsb.set)
    vsb.pack(side="right", fill="y")
    show_db_table(df)


root.after(100, process_log_queue)
root.mainloop()