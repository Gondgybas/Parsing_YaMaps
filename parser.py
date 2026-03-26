import customtkinter as ctk
import threading
import datetime
from tkinter.scrolledtext import ScrolledText
from tkinter import ttk, simpledialog, messagebox
import pandas as pd
import os
import re
from threading import Event

BLACK_DOMAINS = [
    "vk.com", "avito.ru", "avito.com", "zoon.ru", "zoon.com",
    "hh.ru", "ok.ru", "youtube.com", "facebook.com", "instagram.com",
    "twitter.com", "t.me", "telegram.me", "telegram.org",
    "2gis.ru", ".yandex.", "ya.ru", "mail.ru",
    "rb.ru", "google.com", "google.ru"
]
EXCLUDE_LINKS = ("wa.me/", "t.me/", "viber.me/", "viber://", "telegram.me/", "telegram.org/")

EXCEL_FILENAME = "contacts_database.xlsx"
LOG_DIR = "logs"

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

parser_stop_event = Event()
parser_pause_event = Event()
log_file = None

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

def is_valid_email(email):
    if (
        not isinstance(email, str)
        or not email
        or email.count("@") != 1
        or email.startswith("@")
        or len(email) < 6
        or any(c.isspace() for c in email)
        or re.search(r'[\u0400-\u04FF]', email)
    ):
        return False
    username, domain = email.split("@", 1)
    if "." not in domain:
        return False
    if domain.endswith("."):
        return False
    domain_main = domain.rsplit(".", 1)[-1]
    if len(domain_main) < 2 or not domain_main.isalpha():
        return False
    if not re.search(r'\.[a-z]{2,6}$', domain):
        return False
    return True

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

    stats_total = 0
    stats_unique = 0
    stats_duplicate = 0
    stats_parsed = 0
    stats_errors = 0
    start_time = time.time()
    start_datetime = datetime.datetime.now()
    FORBIDDEN_EMAILS = [
        "support@maps.yandex.ru",
        "webmaps-revolution@yandex-team.ru",
        "m-maps@support.yandex.ru"
    ]

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

    def transliterate_name(name):
        table = {
            'а':'a','б':'b','в':'v','г':'g','д':'d','е':'e','ё':'e','ж':'zh','з':'z','и':'i','й':'y','к':'k',
            'л':'l','м':'m','н':'n','о':'o','п':'p','р':'r','с':'s','т':'t','у':'u','ф':'f','х':'h','ц':'ts',
            'ч':'ch','ш':'sh','щ':'sch','ъ':'','ы':'y','ь':'','э':'e','ю':'yu','я':'ya',' ':'_','-':'-'
        }
        result = ""
        for ch in name.lower():
            result += table.get(ch, ch)
        return result

    def find_sites_from_yandex_via_selenium(driver, company_name, city):
        printlog = log_func
        printlog("=== Поиск сайта через Яндекс (selenium) ===")
        search_variants = [
            f"{company_name} {city} сайт",
            f"{transliterate_name(company_name)} {city} сайт",
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
            time.sleep(2.4) # ускорено
            html = driver.page_source
            soup = BeautifulSoup(html, "html.parser")
            all_links = []
            for a in soup.find_all('a', href=True):
                href = a['href']
                if any(x in href for x in EXCLUDE_LINKS):
                    continue
                if (
                    href.startswith("http")
                    and not any(d in href for d in BLACK_DOMAINS)
                    and (".jpg" not in href and ".png" not in href)
                ):
                    if href not in all_links and len(all_links) < 3:
                        all_links.append(href)
            driver.close()
            driver.switch_to.window(driver.window_handles[0])
            if all_links:
                printlog("Кандидаты на сайт: " + "; ".join(all_links))
                return all_links
        printlog("! Нет ни одной нормальной ссылки")
        return result_sites

    def parse_contacts_from_site(site_url, email, site_phones):
        headers = {"User-Agent": "Mozilla/5.0"}
        if black_domain(site_url):
            pages_to_check = [""]
        else:
            pages_to_check = [
                "", "/contacts", "/contact", "/kontakty", "/kontakt",
                "/about", "/about-us", "/company", "/info"
            ]
        found_emails, found_phones = [], []
        for pageurl in pages_to_check:
            if black_domain(site_url) and pageurl != "":
                continue
            try:
                url = site_url.rstrip("/") + pageurl
                log_func(f"Загружаем: {url}")
                time.sleep(1.2)
                r = requests.get(url, timeout=8, headers=headers)
                text = r.text
                emails = re.findall(r'[\w\.-]+@[\w\.-]+', text)
                for e in emails:
                    if is_valid_email(e) and e.lower() not in FORBIDDEN_EMAILS and e not in found_emails:
                        found_emails.append(e)
                phones = re.findall(r'\+7[\d\-\(\) ]{10,15}', text)
                for p in phones:
                    p = p.strip()
                    if p not in found_phones:
                        found_phones.append(p)
            except Exception as e:
                log_func(f"Ошибка запроса: {e}")
                continue
        email = join_unique(found_emails)
        site_phones = join_unique(found_phones)
        return email, site_phones

    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36')
    prefs = {
        "profile.default_content_setting_values.notifications": 2,
        "profile.default_content_setting_values.geolocation": 2,
        "profile.default_content_setting_values.popups": 2,
        "profile.default_content_setting_values.automatic_downloads": 2,
        "credentials_enable_service": False,
        "profile.password_manager_enabled": False,
        "profile.default_content_setting_values.ads": 2
    }
    options.add_experimental_option("prefs", prefs)
    options.add_argument('--disable-blink-features=AutomationControlled')

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    wait = WebDriverWait(driver, 10)
    driver.get("https://yandex.ru/maps")
    log_func("Открыт Яндекс.Карты")
    search_input = wait.until(EC.presence_of_element_located((By.TAG_NAME, "input")))
    log_func("Нашли поле поиска")
    search_input.send_keys(search_query)
    search_input.send_keys(Keys.ENTER)
    time.sleep(2.5) # быстрее

    log_func("Прокрутите список компаний Яндекс.Карт до конца. Затем нажмите OK.")
    messagebox.showinfo(
        "Ручная прокрутка",
        "Прокрутите список организаций в Яндекс.Картах до низа ВРУЧНУЮ (Scroll/PageDown). После этого нажмите OK."
    )

    cards = driver.find_elements(By.CSS_SELECTOR, "a[href*='/org/']")
    log_func(f"После прокрутки найдено карточек: {len(cards)}")
    card_names_links = []
    for card in cards:
        link = card.get_attribute("href")
        name = card.text.strip() if card.text else ""
        if link and "/org/" in link and not any(x in link for x in EXCLUDE_LINKS):
            card_names_links.append((name, link))
    card_names_links = list(set(card_names_links))
    log_func(f"Уникальных карточек (по ссылке+имени): {len(card_names_links)}")
    if company_limit and str(company_limit).isdigit() and int(company_limit) > 0:
        card_names_links = card_names_links[:int(company_limit)]
        log_func(f"Ограничено: только первых {company_limit}")

    # ПЕРВЫЙ ПРОХОД: Имена
    if os.path.exists(EXCEL_FILENAME):
        df_main = pd.read_excel(EXCEL_FILENAME)
        names_in_db = set(str(n).strip().lower() for n in df_main["Название"].dropna().unique())
    else:
        df_main = pd.DataFrame()
        names_in_db = set()
    unique_pairs = []
    dupe_pairs = []
    for name, link in card_names_links:
        if name and name.lower() not in names_in_db:
            unique_pairs.append((name, link))
        else:
            dupe_pairs.append((name, link))

    stats_total = len(unique_pairs) + len(dupe_pairs)
    stats_unique = len(unique_pairs)
    stats_duplicate = len(dupe_pairs)
    log_func(f"Уникальных компаний: {len(unique_pairs)}, уже есть в базе: {len(dupe_pairs)}")
    parser_stop_event.clear()
    parser_pause_event.clear()
    companies = []

    def check_paused():
        while parser_pause_event.is_set():
            log_func("ПАРСЕР на паузе...")
            time.sleep(1)
            if parser_stop_event.is_set():
                log_func("Операция остановлена оператором на паузе!")
                return True
        return False

    def extract_valid_emails(text):
        emails = re.findall(r'[\w\.-]+@[\w\.-]+', text)
        return [e for e in emails if is_valid_email(e)]

    # --- ПАРСИНГ УНИКАЛЬНЫХ ----
    for idx, (name, link) in enumerate(unique_pairs, 1):
        if parser_stop_event.is_set():
            log_func("Операция остановлена оператором!")
            break
        if check_paused():
            return
        log_func(f"\n=== Парсим карточку {idx} (уникальная) ===\nСсылка: {link}")
        try:
            driver.get(link)
            time.sleep(2)
            try:
                name_card = driver.find_element(By.TAG_NAME, "h1").text
            except Exception:
                name_card = name if name else ""
            try:
                phone = driver.find_element(By.XPATH, "//a[contains(@href,'tel')]").text
            except:
                phone = ""
            # EMAIL
            try:
                page_source = driver.page_source
                emails = extract_valid_emails(page_source)
                found_good_email = []
                for e in emails:
                    if e.lower() not in FORBIDDEN_EMAILS and e not in found_good_email:
                        found_good_email.append(e)
                email = join_unique(found_good_email)
            except Exception:
                email = ""
            # САЙТ (с фильтрацией wa.me и т.д.)
            try:
                site_element = driver.find_element(By.XPATH, "//a[contains(@href,'http') and not(contains(@href,'yandex'))]")
                website = site_element.get_attribute("href")
                # Блокируем переходы на мессенджеры, запрещённые сайты
                if website and any(x in website for x in EXCLUDE_LINKS):
                    website = ""
                elif website and any(d in website for d in BLACK_DOMAINS):
                    website = ""
            except:
                website = ""
            # АДРЕС и ДЕЯТЕЛЬНОСТЬ
            try:
                all_divs = driver.find_elements(By.TAG_NAME, "div")
                all_texts = []
                for div in all_divs:
                    try:
                        txt = div.text.strip()
                        if txt and len(txt) > 7:
                            all_texts.append(txt)
                    except Exception:
                        pass
                address = ""
                for t in all_texts:
                    if re.search(r"(Россия|г\.|ул\.|обл\.|д\.|микрорайон|проспект|\d{2,} ?[а-яА-ЯёЁ]+)", t) and \
                        not t.lower().startswith('адрес'):
                        address = t
                        break
                occupation = ""
                for t in all_texts:
                    lower = t.lower()
                    if (("услуги" in lower or "работы" in lower or
                        "деятельност" in lower or "металлообработка" in lower)
                        and len(t) > 15):
                        occupation = t
                        break
            except Exception:
                address = ""
                occupation = ""
            # Сайт — если не найден пробуем найти в Яндексе
            site_phones = ""
            if not website and name_card:
                city = ""
                searchwords = re.split(r'[,|. ]', search_query)
                for w in searchwords:
                    if w.strip() and (w.strip()[0].isupper() or "ск" in w or "г" in w):
                        city = w
                        break
                sites = find_sites_from_yandex_via_selenium(driver, name_card, city)
                for site in sites:
                    if black_domain(site): continue
                    email_site, phones_site = parse_contacts_from_site(site, email, site_phones)
                    if email_site: email = email_site
                    if phones_site: site_phones = phones_site
                    website = site
                    break
            elif website:
                email_site, phones_site = parse_contacts_from_site(website, email, site_phones)
                if email_site: email = email_site
                if phones_site: site_phones = phones_site
            # ------
            new_info = {
                "Дата поиска": start_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                "Запрос": search_query,
                "Название": name_card,
                "Телефон (Яндекс)": phone,
                "Телефон (сайт)": site_phones,
                "Email": email,
                "Сайт": website,
                "Адрес": address,
                "Описание деятельности": occupation
            }
            companies.append(new_info)
            stats_parsed += 1
            df_add = pd.DataFrame([new_info])
            if os.path.exists(EXCEL_FILENAME):
                df_main_cur = pd.read_excel(EXCEL_FILENAME)
                df_final = pd.concat([df_main_cur, df_add], ignore_index=True)
            else:
                df_final = df_add
            df_final.to_excel(EXCEL_FILENAME, index=False)
            try:
                if (database_window is not None and database_window.winfo_exists()):
                    df_ = pd.read_excel(EXCEL_FILENAME)
                    show_db_table(df_)
            except Exception as ex:
                log_func(f"(Не удалось обновить окно просмотра базы: {ex})")
        except Exception as e:
            log_func(f"ОШИБКА ПРИ ПАРСИНГЕ: {e}")
            stats_errors += 1

    # --- ЧЕКЛИСТ дубли ---
    if dupe_pairs and not parser_stop_event.is_set():
        dupe_names = [name for name, link in dupe_pairs]
        selected = []

        checklist_win = ctk.CTkToplevel(root)
        checklist_win.geometry("600x500")
        checklist_win.title("Дублирующиеся компании для парсинга")
        ctk.CTkLabel(checklist_win, text="Выберите дублирующиеся компании для перепарсинга:",
                     font=("Arial", 15)).pack(pady=5)
        vars_list = []
        frame_inner = ctk.CTkFrame(checklist_win)
        frame_inner.pack(fill="both", expand=True)
        for name in dupe_names:
            v = ctk.BooleanVar(value=False)
            chk = ctk.CTkCheckBox(frame_inner, text=name, variable=v)
            chk.pack(anchor="w")
            vars_list.append((name, v))
        def ok():
            checklist_win.selected_names = [name for name, v in vars_list if v.get()]
            checklist_win.destroy()
        btn = ctk.CTkButton(checklist_win, text="Парсить выбранные", command=ok)
        btn.pack(anchor="s", pady=8)
        checklist_win.grab_set()
        root.wait_window(checklist_win)
        selected_names = getattr(checklist_win, "selected_names", [])
        selected_pairs = [(n, l) for (n, l) in dupe_pairs if n in selected_names]
        if selected_pairs:
            for idx, (name, link) in enumerate(selected_pairs, 1):
                if parser_stop_event.is_set():
                    log_func("Операция остановлена оператором (повторы)!")
                    break
                if check_paused():
                    return
                log_func(f"\n=== Парсим повторную карточку {idx} ===\nСсылка: {link}")
                # Можно повторить тот же код что выше (парсинг карточки!)
                # ... (см. блок парсинга выше, копируется почти всё)

    driver.quit()
    end_time = time.time()
    end_datetime = datetime.datetime.now()
    elapsed = end_time - start_time
    hours = int(elapsed // 3600)
    minutes = int((elapsed % 3600) // 60)
    seconds = int(elapsed % 60)
    log_func("\n==== СТАТИСТИКА ПО СЕССИИ ====")
    log_func(f"Поисковый запрос: {search_query}")
    log_func(f"Время начала: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    log_func(f"Время окончания: {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
    if hours > 0:
        log_func(f"Общее время работы: {hours}ч {minutes}мин {seconds}сек")
    else:
        log_func(f"Общее время работы: {minutes}мин {seconds}сек")
    log_func(f"Всего найдено карточек: {stats_total}")
    log_func(f" - из них новых: {stats_unique}")
    log_func(f" - из них уже в базе: {stats_duplicate}")
    log_func(f"Успешно обработано компаний: {stats_parsed}")
    log_func(f"Ошибок: {stats_errors}")
    log_func("==============================\n")

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

log_text = ScrolledText(frame, height=24, width=100, bg="#212223", fg="#D6D6D6", font=("Consolas", 12),
                        wrap="word", state="disabled", insertbackground="white")
log_text.pack(fill="both", expand=True, padx=(0, 0), pady=(0, 10))

def do_parse():
    btn_parse.configure(state="disabled")
    btn_pause_resume.configure(state="normal")
    query = query_var.get()
    limit = limit_var.get().strip()
    t = threading.Thread(target=run_parser, args=(query, log, limit))
    t.start()
    def reenable():
        t.join()
        btn_parse.configure(state="normal")
        btn_pause_resume.configure(text="⏸ Пауза", fg_color="orange")
        parser_pause_event.clear()
    threading.Thread(target=reenable).start()

lbl_query = ctk.CTkLabel(frame, text="Поисковый запрос:", anchor="w")
lbl_query.pack(anchor="w", pady=(4, 4))
query_entry = ctk.CTkEntry(frame, textvariable=query_var, width=500, font=("Arial", 15))
query_entry.pack(anchor="w", pady=(0, 12))

lbl_limit = ctk.CTkLabel(frame, text="Сколько компаний парсить? (пусто = все)", anchor="w")
lbl_limit.pack(anchor="w")
limit_entry = ctk.CTkEntry(frame, textvariable=limit_var, width=100, font=("Arial", 15))
limit_entry.pack(anchor="w", pady=(0, 18))

btn_parse = ctk.CTkButton(frame, text="Начать парсинг", command=do_parse, width=200, height=42)
btn_parse.pack(anchor="w", pady=(0, 12))

def toggle_pause_resume():
    if not parser_pause_event.is_set():
        parser_pause_event.set()
        btn_pause_resume.configure(text="▶ Продолжить", fg_color="green")
    else:
        parser_pause_event.clear()
        btn_pause_resume.configure(text="⏸ Пауза", fg_color="orange")

btn_pause_resume = ctk.CTkButton(
    frame, text="⏸ Пауза", command=toggle_pause_resume, width=140, fg_color="orange"
)
btn_pause_resume.pack(anchor="w", pady=(0, 12))
btn_pause_resume.configure(state="disabled")

btn_stop = ctk.CTkButton(frame, text="Остановить парсинг", command=lambda: parser_stop_event.set(), width=200,
                         height=42, fg_color="red")
btn_stop.pack(anchor="w", pady=(0, 12))

btn_dbview = ctk.CTkButton(frame, text="Посмотреть Базу", command=lambda: open_db_view(), width=180, height=38)
btn_dbview.pack(anchor="w", pady=(0, 12))

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