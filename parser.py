import customtkinter as ctk
import threading
import datetime
from tkinter.scrolledtext import ScrolledText
from tkinter import ttk, simpledialog
import pandas as pd
import os
import re
from threading import Event

BLACK_DOMAINS = [
    "vk.com", "avito.ru", "avito.com", "hh.ru", "ok.ru", "youtube.com", "facebook.com", "instagram.com",
    "twitter.com", "t.me", "2gis.ru", ".yandex.", "ya.ru", "mail.ru", "rb.ru", "google.com", "zoon.ru", "orgpage.ru", "google.ru"
]
MESSENGER_LINKS = ("wa.me/", "t.me/", "viber.me/", "viber://", "telegram.me/", "telegram.org/")

FORBIDDEN_EMAILS = [
    "support@maps.yandex.ru",
    "webmaps-revolution@yandex-team.ru",
    "m-maps@support.yandex.ru"
]

EXCEL_FILENAME = "contacts_database.xlsx"
LOG_DIR = "logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

parser_stop_event = Event()
parser_pause_event = Event()
log_file = None

def cut_to_main_yamaps_card(link):
    m = re.match(r"^(https://yandex\.[^/]+/maps/org/[^/]+/\d+)", link)
    return m.group(1) if m else link.split("?")[0].split("/reviews")[0].split("/photos")[0].split("/gallery")[0]

def is_valid_email(email):
    if not isinstance(email, str) or "@" not in email or email.count("@") != 1:
        return False
    if email.lower() in [x.lower() for x in FORBIDDEN_EMAILS]:
        return False
    username, domain = email.split("@", 1)
    allowed_domains = (".ru", ".com", ".bk", ".net", ".org", ".by", ".ua")
    if not any(domain.lower().endswith(ad) for ad in allowed_domains):
        return False
    tld = domain.rsplit('.', 1)[-1]
    if not tld.isalpha() or len(tld) < 2:
        return False
    return True

def log(msg):
    global log_file
    log_text.configure(state="normal")
    log_text.insert("end", msg + "\n")
    log_text.see("end")
    log_text.configure(state="disabled")
    root.update()
    try:
        if log_file:
            log_file.write(msg + '\n')
            log_file.flush()
    except Exception:
        pass

def run_parser(search_query, log_func, company_limit=None):
    import time, requests, urllib.parse
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.keys import Keys
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from bs4 import BeautifulSoup

    global parser_stop_event, parser_pause_event, log_file

    log_filename = os.path.join(
        LOG_DIR,
        f"log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    )
    log_file = open(log_filename, "w", encoding="utf-8")

    if os.path.exists(EXCEL_FILENAME):
        df_main = pd.read_excel(EXCEL_FILENAME)
    else:
        df_main = pd.DataFrame()

    now_str = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def black_domain(site):
        return any(domain in site for domain in BLACK_DOMAINS if site)

    def join_unique(items):
        uniq = []
        for x in items:
            x = x.strip()
            if x and x not in uniq:
                uniq.append(x)
            if len(uniq) >= 3:
                break
        return '; '.join(uniq)

    def find_sites_from_yandex_via_selenium(driver, company_name, city):
        printlog = log_func
        printlog("=== Поиск сайта через Яндекс (selenium) ===")
        search_variants = [
            f"{company_name} {city} сайт",
            f"{company_name.replace('-', ' ')} {city} сайт"
        ]
        result_sites = []
        for query in search_variants:
            printlog(f"\nYandex search query: {query}")
            url = "https://yandex.ru/search/?text=" + urllib.parse.quote_plus(query)
            printlog(f"Открываю браузер с Яндекс-поиском: {url}")
            driver.execute_script("window.open('');")
            driver.switch_to.window(driver.window_handles[-1])
            driver.get(url)
            time.sleep(8)
            html = driver.page_source
            if "smart-captcha" in html or "Капча" in html or "captcha" in html:
                try:
                    ctk.CTkMessagebox(title="Капча!", message="Реши капчу в окне selenium (ручное действие!)", icon="warning")
                except Exception:
                    pass
                time.sleep(10)
                html = driver.page_source
            soup = BeautifulSoup(html, "html.parser")
            all_links = []
            for a in soup.find_all('a', href=True):
                href = a['href']
                if any(x in href for x in MESSENGER_LINKS):
                    continue
                if href.startswith("http") and not any(bd in href for bd in BLACK_DOMAINS) and (".jpg" not in href and ".png" not in href):
                    if href not in all_links and len(all_links) < 3:
                        all_links.append(href)
            driver.close()
            driver.switch_to.window(driver.window_handles[0])
            if all_links:
                for i, site in enumerate(all_links, 1):
                    printlog(f"{i}) {site}")
                return all_links
        printlog("! Нет ни одной нормальной ссылки")
        return result_sites

    def parse_contacts_from_site(site_url):
        headers = {"User-Agent": "Mozilla/5.0"}
        if black_domain(site_url):
            pages_to_check = [""]
        else:
            pages_to_check = [
                "", "/contacts", "/contact", "/kontakty", "/kontakt", "/about", "/about-us", "/company", "/info"
            ]
        found_emails, found_phones = [], []
        for pageurl in pages_to_check:
            if black_domain(site_url) and pageurl != "":
                continue
            try:
                url = site_url.rstrip("/") + pageurl
                log_func(f"Загружаем: {url}")
                time.sleep(2.5)
                r = requests.get(url, timeout=10, headers=headers)
                text = r.text
                emails = re.findall(r'[\w\.-]+@[\w\.-]+', text)
                for e in emails:
                    e_low = e.lower()
                    if is_valid_email(e) and e_low not in [x.lower() for x in FORBIDDEN_EMAILS] and e not in found_emails:
                        found_emails.append(e)
                phones = re.findall(r'\+7[\d\-\(\) ]{10,15}', text)
                for p in phones:
                    p = p.strip()
                    if p not in found_phones:
                        found_phones.append(p)
                if found_emails:
                    break  # Уже нашли email — больше не лезем
            except Exception as e:
                log_func(f"Ошибка запроса: {e}")
                continue
        email = join_unique(found_emails)
        site_phones = join_unique(found_phones)
        return email, site_phones

    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    wait = WebDriverWait(driver, 10)
    driver.get("https://yandex.ru/maps")
    log_func("Отк��ыт Яндекс.Карты")
    search_input = wait.until(EC.presence_of_element_located((By.TAG_NAME, "input")))
    log_func("Нашли поле поиска")
    search_input.send_keys(search_query)
    search_input.send_keys(Keys.ENTER)
    time.sleep(5)

    log_func("Прокрутите список компаний Яндекс.Карт до конца. Затем нажмите OK.")
    import tkinter.messagebox
    tkinter.messagebox.showinfo(
        "Ручная Прокрутка",
        "Прокрутите список организаций в Яндекс.Картах до НИЗУ ВРУЧНУЮ (мышкой, колесиком или PageDown), "
        "чтобы ВСЕ компании появились на странице.\n\nПосле этого нажмите OK для запуска парсинга."
    )

    cards = driver.find_elements(By.CSS_SELECTOR, "a[href*='/org/']")
    log_func(f"После прокрутки найдено карт��чек: {len(cards)}")

    all_links, all_names = [], []
    for card in cards:
        try:
            link = card.get_attribute("href")
            name = card.text.strip() if card.text else ""
            if link and "/org/" in link and not any(x in link for x in MESSENGER_LINKS):
                all_links.append(link)
                all_names.append(name)
        except:
            pass
    links = list(set(all_links))
    names = all_names[:len(links)]
    log_func(f"Найдено карточек: {len(names)}")

    # Быстрая логика дублей по названию
    if df_main.shape[0] > 0 and "Название" in df_main.columns:
        names_in_db = set(str(n).strip().lower() for n in df_main["Название"].dropna().unique())
    else:
        names_in_db = set()
    unique_pairs = []
    dupe_pairs = []
    for name, link in zip(names, links):
        if name and name.strip().lower() not in names_in_db:
            unique_pairs.append((name, link))
        else:
            dupe_pairs.append((name, link))

    log_func("\n====== СТАТИСТИКА ПАРСИНГА ======")
    log_func(f"Уникальных компаний: {len(unique_pairs)}")
    log_func(f"Дубликатов (по имени): {len(dupe_pairs)}")
    log_func("==========================\n")
    time.sleep(1)

    global parser_stop_event
    parser_stop_event.clear()
    companies = []

    for idx, (name, link) in enumerate(unique_pairs, 1):
        if parser_stop_event.is_set():
            log_func("Операция остановлена оператором!")
            break

        while parser_pause_event.is_set():
            log_func("ПАРСЕР на паузе...")
            time.sleep(1)
            if parser_stop_event.is_set():
                log_func("Операция остановлена оператором на паузе!")
                return

        main_link = cut_to_main_yamaps_card(link)
        log_func(f"\n=== Парсим карточку {idx} (уникальная) ===\nСсылка: {main_link}")

        try:
            driver.get(main_link)
            time.sleep(4)
            phone, website, address, email, site_phones, occupation = "","","","","",""
            soup = BeautifulSoup(driver.page_source, "html.parser")
            try:
                name = driver.find_element(By.TAG_NAME, "h1").text
                log_func(f"Название: {name}")
            except Exception as e:
                log_func(f"Ошибка поиска названия: {e}")
            try:
                phone = driver.find_element(By.XPATH, "//a[contains(@href,'tel')]").text
                log_func(f"Телефон (Яндекс): {phone}")
            except:
                log_func("Телефон (Яндекс): не найден")
            # Чистый адрес с a.business-contacts-view__address-link
            try:
                address_elem = soup.find("a", class_="business-contacts-view__address-link")
                address = address_elem.text.strip() if address_elem else ""
                log_func(f"Актуальный адрес: {address}")
            except Exception as e:
                address = ""
                log_func("Не найден адрес!")
            # Описание деятельности по категориям
            try:
                occupation_items = []
                cats_div = soup.find("div", class_="orgpage-categories-info-view")
                if cats_div:
                    for span in cats_div.find_all("span", class_="button__text"):
                        txt = span.get_text(strip=True)
                        if txt and txt not in occupation_items:
                            occupation_items.append(txt)
                occupation = "; ".join(occupation_items)
                log_func(f"Актуальные виды деятельности: {occupation}")
            except Exception as e:
                occupation = ""
                log_func("Ошибка поиска деятельности!")

            # ======== Ищем email по приоритету ==========

            # 1. email на Яндекс.Картах
            try:
                page_source = driver.page_source
                emails = re.findall(r'[\w\.-]+@[\w\.-]+', page_source)
                found_good_email = []
                for e in emails:
                    e_low = e.lower()
                    if is_valid_email(e) and e_low not in [x.lower() for x in FORBIDDEN_EMAILS] and e not in found_good_email:
                        found_good_email.append(e)
                email = join_unique(found_good_email)
                if email:
                    log_func(f"Нашли email на Я.Картах: {email}")
                else:
                    log_func(f"Email на Яндекс.Картах не найден.")
            except Exception as e:
                log_func(f"Ошибка поиска email: {e}")

            # 2. Если не нашли, ищем на сайте с Я.Карт
            if not email:
                try:
                    site_element = driver.find_element(By.XPATH, "//a[contains(@href,'http') and not(contains(@href,'yandex'))]")
                    website = site_element.get_attribute("href")
                    if website and (any(x in website for x in MESSENGER_LINKS) or black_domain(website)):
                        log_func(f"Сайт исключён (или мессенджер/черный), пропускаем.")
                        website = ""
                    else:
                        log_func(f"Пробуем парсить email с сайта с Я.Карт: {website}")
                except:
                    website = ""
                if website:
                    try:
                        email_site, phones_site = parse_contacts_from_site(website)
                        if email_site:
                            email = email_site
                            site_phones = phones_site
                            log_func(f"Нашли email на сайте: {email_site}")
                        elif phones_site:
                            site_phones = phones_site
                    except Exception as e:
                        log_func(f"Ошибка обхода сайта: {e}")

            # 3. Если всё ещё нет email — ищем через Яндекс Поиск (top 3, по очереди)
            if not email:
                log_func("Ищем по top-3 сайтов из Яндекс.Поиска...")
                found_sites = find_sites_from_yandex_via_selenium(driver, name, search_query.split()[-1])
                if found_sites:
                    for i, site in enumerate(found_sites, 1):
                        if any(x in site for x in MESSENGER_LINKS) or black_domain(site):
                            log_func(f"Пропущена ссылка-исключение: {site}")
                            continue
                        log_func(f"Пробуем сайт #{i} из поиска: {site}")
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
                        log_func("Не удалось найти email на первых 3 сайтах Яндекса.")
                else:
                    log_func("Не найден ни один сайт через Яндекс. Пропускаем.")

            new_info = {
                "Дата поиска": now_str,
                "Запрос": search_query,
                "Название": name,
                "Телефон (Яндекс)": phone,
                "Телефон (сайт)": site_phones,
                "Email": email,
                "Сайт": website,
                "Адрес": address,
                "Описание деятельности": occupation
            }

            companies.append(new_info)

            if len(companies) > 0:
                df_add = pd.DataFrame([new_info])
                if os.path.exists(EXCEL_FILENAME):
                    df_main_cur = pd.read_excel(EXCEL_FILENAME)
                    df_final = pd.concat([df_main_cur, df_add], ignore_index=True)
                else:
                    df_final = df_add
                df_final.to_excel(EXCEL_FILENAME, index=False)
                log_func("Результаты по текущей компании сохранены в базу.")
                try:
                    if database_window is not None and database_window.winfo_exists():
                        try:
                            df = pd.read_excel(EXCEL_FILENAME)
                            show_db_table(df)
                        except Exception:
                            pass
                except Exception as ex:
                    log_func(f"(Не удалось обновить окно просмотра базы: {ex})")
            log_func(f"--- Итог по карточке ---\nEmail: {email}\nТелефон (Яндекс): {phone}\nТелефон (сайт): {site_phones}\nСайт: {website}\nАдрес: {address}\nОписание: {occupation}")
        except Exception as e:
            log_func(f"ОШИБКА ГЛАВНОГО ЦИКЛА: {e}")

    driver.quit()
    if len(companies) > 0:
        df_add = pd.DataFrame(companies)
        if df_main.empty:
            df_final = df_add
        else:
            df_final = pd.concat([df_main, df_add], ignore_index=True)
        df_final.to_excel(EXCEL_FILENAME, index=False)
        log_func(f"\nДанные добавлены в общий файл: {EXCEL_FILENAME}\nГотово! Добавлено: {len(companies)} новых компаний.")
    else:
        log_func("Ни одной новой компании не добавлено (всё дубли или пусто)")

    if log_file:
        log_file.close()

# --------- GUI ---------
ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("dark-blue")

root = ctk.CTk()
database_window = None
root.title("Яндекс-Карты КонтактПарсер")
root.geometry("950x700")
frame = ctk.CTkFrame(root)
frame.pack(padx=15, pady=15, fill="both", expand=True)

query_var = ctk.StringVar(value="металлообработка Подольск")
limit_var = ctk.StringVar(value="")

def do_parse():
    btn_parse.configure(state="disabled")
    query = query_var.get()
    limit = limit_var.get().strip()
    t = threading.Thread(target=run_parser, args=(query, log, limit))
    t.start()
    def reenable():
        t.join()
        btn_parse.configure(state="normal")
    threading.Thread(target=reenable).start()

lbl_query = ctk.CTkLabel(frame, text="Поисковый запрос:", anchor="w")
lbl_query.pack(anchor="w", pady=(4,4))
query_entry = ctk.CTkEntry(frame, textvariable=query_var, width=500, font=("Arial", 15))
query_entry.pack(anchor="w", pady=(0,12))

lbl_limit = ctk.CTkLabel(frame, text="Сколько компаний парсить? (пусто = все)", anchor="w")
lbl_limit.pack(anchor="w")
limit_entry = ctk.CTkEntry(frame, textvariable=limit_var, width=100, font=("Arial", 15))
limit_entry.pack(anchor="w", pady=(0,18))

btn_parse = ctk.CTkButton(frame, text="Начать парсинг", command=do_parse, width=200, height=42)
btn_parse.pack(anchor="w", pady=(0,24))

btn_pause = ctk.CTkButton(frame, text="Пауза", command=lambda: parser_pause_event.set(), width=140, fg_color="orange")
btn_pause.pack(anchor="w", pady=(0,5))

btn_resume = ctk.CTkButton(frame, text="Продолжить", command=lambda: parser_pause_event.clear(), width=140, fg_color="green")
btn_resume.pack(anchor="w", pady=(0,14))

btn_stop = ctk.CTkButton(frame, text="Остановить парсинг", command=lambda: parser_stop_event.set(), width=200, height=42, fg_color="red")
btn_stop.pack(anchor="w", pady=(0, 14))

btn_dbview = ctk.CTkButton(frame, text="Посмотреть Базу", command=lambda: open_db_view(), width=180, height=38)
btn_dbview.pack(anchor="w", pady=(0,14))

lbl_log = ctk.CTkLabel(frame, text="Сообщения отладки / ход работы:")
lbl_log.pack(anchor="w", pady=(0,5))
log_text = ScrolledText(frame, height=24, width=100, bg="#212223", fg="#D6D6D6", font=("Consolas", 12), wrap="word", state="disabled", insertbackground="white")
log_text.pack(fill="both", expand=True, padx=(0,0), pady=(0,10))

database_window = None
db_tree = None
db_columns = None

def show_db_table(df):
    global db_tree, db_columns
    if db_tree is None:
        return
    db_tree.delete(*db_tree.get_children())
    for i, row in df.iterrows():
        db_tree.insert("", "end", values=[str(row[c]) if pd.notna(row[c]) else "" for c in db_columns])

def open_db_view():
    global database_window, db_tree, db_columns
    if (database_window is not None
        and hasattr(database_window, "winfo_exists") and database_window.winfo_exists()):
        try:
            df = pd.read_excel(EXCEL_FILENAME)
            show_db_table(df)
        except Exception:
            pass
        database_window.focus_set()
        return

    if not os.path.exists(EXCEL_FILENAME):
        from tkinter import messagebox
        messagebox.showinfo("Нет базы", "Файл базы ещё не создан. Сначала сделайте хотя бы один парсинг.")
        return
    df = pd.read_excel(EXCEL_FILENAME)

    database_window = ctk.CTkToplevel(root)
    database_window.title("Просмотр базы данных (Excel-фильтр)")
    database_window.geometry("1280x700")

    frm = ctk.CTkFrame(database_window)
    frm.pack(fill="both", expand=True, padx=10, pady=5)

    db_columns = list(df.columns)
    db_tree = ttk.Treeview(frm, show="headings")
    db_tree["columns"] = db_columns
    style = ttk.Style(db_tree)
    style.theme_use("clam")
    style.configure("Treeview.Heading", background="#1a1a1a", foreground="#d6d6d6", relief="flat")
    for col in db_columns:
        db_tree.heading(col, text=col, anchor="w")
        db_tree.column(col, width=170, anchor="w")
    db_tree.pack(fill="both", expand=True, side="left")

    vsb = ttk.Scrollbar(frm, orient="vertical", command=db_tree.yview)
    db_tree.configure(yscrollcommand=vsb.set)
    vsb.pack(side="right", fill="y")

    show_db_table(df)

root.mainloop()