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
    "vk.com", "avito.ru", "avito.com", "zoon.ru", "zoon.com",
    "hh.ru", "ok.ru", "youtube.com", "facebook.com", "instagram.com",
    "twitter.com", "t.me", "2gis.ru", ".yandex.", "ya.ru", "mail.ru",
    "rb.ru", "google.com", "google.ru"
]

EXCEL_FILENAME = "contacts_database.xlsx"  # Файл с единой базой

parser_stop_event = Event()
parser_pause_event = Event()

# ==== ЛОГ-ФАЙЛ для каждой сессии ====
LOG_DIR = "logs"
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

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
    """
    Оставляет только нормальные почты вида user@domain.ru/com/bk/...
    Убирает exp@300, аа@11, test@, etc.
    """
    if (
        not isinstance(email, str)
        or not email
        or email.count("@") != 1
        or email.startswith("@")
        or len(email) < 6
        or any(c.isspace() for c in email)
        or re.search(r'[\u0400-\u04FF]', email)  # кириллица
    ):
        return False
    username, domain = email.split("@", 1)
    # Домен должен быть похоже на адрес: без цифр, с точкой, окончание буквы не меньше 2
    if "." not in domain:
        return False
    if domain.endswith("."):
        return False
    domain_main = domain.rsplit(".", 1)[-1]
    if len(domain_main) < 2 or not domain_main.isalpha():
        return False
    # Список разрешённых доменных окончаний (регулярка .ru|.com|.bk|.net|.org|.mail и др.)
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
    import tkinter.messagebox

    global parser_stop_event, parser_pause_event, log_file

    # Лог-файл для этой сессии
    log_filename = os.path.join(
        LOG_DIR,
        f"log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    )
    log_file = open(log_filename, "w", encoding="utf-8")

    # Статистика
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

    def parse_contacts_from_site(site_url, email, site_phones):
        headers = {"User-Agent": "Mozilla/5.0"}

        # Парсим только главную для чёрных доменов!
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
                time.sleep(2)
                r = requests.get(url, timeout=10, headers=headers)
                text = r.text
                # фильтруем каждую почту:
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

    # --- Selenium старт ---
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
    time.sleep(5)

    log_func(
        "Прокрутите список компаний Яндекс.Карт вручную до НИЗУ, чтобы все карточки подгрузились! Затем нажмите OK."
    )
    tkinter.messagebox.showinfo(
        "Ручная прокрутка",
        "Прокрутите список организаций в Яндекс.Картах до низу ВРУЧНУЮ (колесиком или PageDown), "
        "чтобы ВСЕ компании появились на странице!\n\nПотом нажмите OK."
    )

    cards = driver.find_elements(By.CSS_SELECTOR, "a[href*='/org/']")
    log_func(f"После ручной прокрутки найдено карточек: {len(cards)}")
    card_names_links = []
    for card in cards:
        link = card.get_attribute("href")
        name = card.text.strip() if card.text else ""
        if link and "/org/" in link:
            card_names_links.append((name, link))
    # убираем дубли
    card_names_links = list(set(card_names_links))
    log_func(f"Уникальных карточек (по ссылке+имени): {len(card_names_links)}")
    if company_limit and str(company_limit).isdigit() and int(company_limit) > 0:
        card_names_links = card_names_links[:int(company_limit)]
        log_func(f"Ограничено: парсим только первых {company_limit} компаний.")

    # Проверка уникальности
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

    global parser_stop_event, parser_pause_event
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

    # -- ПАРСИМ УНИКАЛЬНЫЕ --
    for idx, (name, link) in enumerate(unique_pairs, 1):
        if parser_stop_event.is_set():
            log_func("Операция остановлена оператором!")
            break
        if check_paused():
            return
        log_func(f"\n=== Парсим карточку {idx} (уникальная) ===\nСсылка: {link}")
        try:
            driver.get(link)
            time.sleep(3)
            try:
                name = driver.find_element(By.TAG_NAME, "h1").text
                log_func(f"Название: {name}")
            except Exception as e:
                log_func(f"Ошибка поиска названия: {e}")
                name = name if name else ""
            try:
                phone = driver.find_element(By.XPATH, "//a[contains(@href,'tel')]").text
                log_func(f"Телефон (Яндекс): {phone}")
            except:
                log_func("Телефон (Яндекс): не найден")
                phone = ""
            # EMAIL из html
            try:
                page_source = driver.page_source
                emails = extract_valid_emails(page_source)
                found_good_email = []
                for e in emails:
                    if e.lower() not in FORBIDDEN_EMAILS and e not in found_good_email:
                        found_good_email.append(e)
                email = join_unique(found_good_email)
                if not email:
                    log_func("Нет email на Яндекс.Картах")
                else:
                    log_func(f"Используем email: {email}")
            except Exception as e:
                log_func(f"Ошибка поиска email: {e}")
                email = ""
            # САЙТ
            try:
                site_element = driver.find_element(By.XPATH, "//a[contains(@href,'http') and not(contains(@href,'yandex'))]")
                website = site_element.get_attribute("href")
                if website and any(d in website for d in BLACK_DOMAINS):
                    log_func(f"Сайт ведёт на запрещённый домен: {website}")
                else:
                    log_func(f"Сайт компании: {website if website else 'Не найден'}")
            except:
                log_func("Сайт компании: не найден")
                website = ""
            # АДРЕС и ДЕЯТЕЛЬНОСТЬ (универсальный способ)
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
                    if re.search(r"(Россия|Подольск|Чехов|Домодедово|Серпухов|г\.|ул\.|обл\.|д\.|микрорайон|проспект|\d{2,}\s*[а-яА-ЯёЁ]+)", t) and \
                            "Яндекс" not in t and not t.lower().startswith('адрес'):
                        address = t
                        break
                log_func(f"Автоматически найденный адрес: {address}")

                occupation = ""
                for t in all_texts:
                    lower = t.lower()
                    if (("услуги" in lower or "работы" in lower or "деятельност" in lower or "металлообработка" in lower)
                        and len(t) > 15):
                        occupation = t
                        break
                log_func(f"Автоматически найденное описание деятельности: {occupation}")

            except Exception as e:
                log_func(f"Ошибка поиска адреса/деятельности: {e}")
                address = ""
                occupation = ""
            # Парсинг сайта (только главная, если домен чёрный)
            site_phones = ""
            if website:
                log_func(f"Парсим сайт компании: {website}")
                try:
                    email_site, phones_site = parse_contacts_from_site(website, email, site_phones)
                    if email_site:
                        email = email_site
                    if phones_site:
                        site_phones = phones_site
                except Exception as e:
                    log_func(f"Ошибка обхода сайта: {e}")
            # Итоговая запись
            new_info = {
                "Дата поиска": start_datetime.strftime('%Y-%m-%d %H:%M:%S'),
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
            stats_parsed += 1
            # Сохраняем после каждой компании
            df_add = pd.DataFrame([new_info])
            if os.path.exists(EXCEL_FILENAME):
                df_main_cur = pd.read_excel(EXCEL_FILENAME)
                df_final = pd.concat([df_main_cur, df_add], ignore_index=True)
            else:
                df_final = df_add
            df_final.to_excel(EXCEL_FILENAME, index=False)
            try:
                if (database_window is not None
                        and hasattr(database_window, "winfo_exists")
                        and database_window.winfo_exists()):
                    df_ = pd.read_excel(EXCEL_FILENAME)
                    show_db_table(df_)
            except Exception as ex:
                log_func(f"(Не удалось обновить окно просмотра базы: {ex})")
            log_func("Результаты по текущей компании сохранены в базу.")
        except Exception as e:
            log_func(f"ОШИБКА ПРИ ПАРСИНГЕ: {e}")
            stats_errors += 1

    # == ПАРСИНГ ДУБЛИКАТОВ ==
    if len(dupe_pairs) > 0 and not parser_stop_event.is_set():
        resp = tkinter.messagebox.askyesno(
            "Уже есть в базе",
            f"В базе уже есть {len(dupe_pairs)} компаний из этого поиска.\n"
            f"Хотите также парсить уже существующие компании? (Да — парсить их, Нет — завершить)"
        )
        if resp:
            for idx, (name, link) in enumerate(dupe_pairs, 1):
                if parser_stop_event.is_set():
                    log_func("Операция остановлена оператором!")
                    break
                if check_paused():
                    return
                log_func(f"\n=== Парсим дубль {idx} ===\nСсылка: {link}")
                try:
                    driver.get(link)
                    time.sleep(3)
                    # ... Повторяем логику, как выше (с фильтрацией емейла по is_valid_email)
                except Exception as e:
                    log_func(f"ОШИБКА ПРИ ПАРСИНГЕ ДУБЛЯ: {e}")
                    stats_errors += 1

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

log_text = ScrolledText(frame, height=24, width=100, bg="#212223", fg="#D6D6D6", font=("Consolas", 12), wrap="word", state="disabled", insertbackground="white")
log_text.pack(fill="both", expand=True, padx=(0,0), pady=(0,10))

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
lbl_query.pack(anchor="w", pady=(4,4))
query_entry = ctk.CTkEntry(frame, textvariable=query_var, width=500, font=("Arial", 15))
query_entry.pack(anchor="w", pady=(0,12))

lbl_limit = ctk.CTkLabel(frame, text="Сколько компаний парсить? (пусто = все)", anchor="w")
lbl_limit.pack(anchor="w")
limit_entry = ctk.CTkEntry(frame, textvariable=limit_var, width=100, font=("Arial", 15))
limit_entry.pack(anchor="w", pady=(0,18))

btn_parse = ctk.CTkButton(frame, text="Начать парсинг", command=do_parse, width=200, height=42)
btn_parse.pack(anchor="w", pady=(0,12))

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
btn_pause_resume.pack(anchor="w", pady=(0,12))
btn_pause_resume.configure(state="disabled")  # сначала неактивна

btn_stop = ctk.CTkButton(frame, text="Остановить парсинг", command=lambda: parser_stop_event.set(), width=200, height=42, fg_color="red")
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