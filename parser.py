import customtkinter as ctk
import threading
import datetime
from tkinter.scrolledtext import ScrolledText
from tkinter import ttk, simpledialog
import pandas as pd
import os
import re
from threading import Event

BLACK_DOMAINS = ["vk.com", "avito.ru", "avito.com", "hh.ru", "ok.ru", "youtube.com", "facebook.com", "instagram.com",
                 "twitter.com", "t.me", "2gis.ru", ".yandex.", "ya.ru", "mail.ru", "rb.ru", "google.com", "google.ru"]

EXCEL_FILENAME = "contacts_database.xlsx"  # Файл с единой базой

parser_stop_event = Event()
parser_pause_event = Event()

def run_parser(search_query, log_func, company_limit=None):
    import time, re, requests, urllib.parse
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.keys import Keys
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from bs4 import BeautifulSoup

    FORBIDDEN_EMAILS = [
        "support@maps.yandex.ru",
        "webmaps-revolution@yandex-team.ru",
        "m-maps@support.yandex.ru"
    ]

    # === База для проверки дубликатов ===
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

    def is_valid_email(email):
        if (
                "@stacks.vk-portal.net" in email.lower()
                or email.lower().endswith("@stacks.vk-portal.net")
        ):
            return False
        return (
                len(email) > 6 and
                "." in email.split("@")[-1] and
                not email.isdigit() and
                email.count("@") == 1
        )

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
            time.sleep(8)
            html = driver.page_source
            if "smart-captcha" in html or "Капча" in html or "captcha" in html:
                printlog("=== КАПЧА, решите её!!! ===")
                ctk.CTkMessagebox(title="Капча!", message="Реши капчу в окне selenium (ручное действие!)", icon="warning")
                time.sleep(10)
                html = driver.page_source
            soup = BeautifulSoup(html, "html.parser")
            all_links = []
            for a in soup.find_all('a', href=True):
                href = a['href']
                text = a.get_text(strip=True)
                printlog(f"Найден HREF: {href} text: {text}")
                if (
                    href.startswith("http")
                    and "yandex" not in href
                    and "2gis" not in href
                    and "maps." not in href
                    and "rambler" not in href
                    and (".jpg" not in href and ".png" not in href)
                ):
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

    def parse_contacts_from_site(site_url, email, site_phones):
        headers = {"User-Agent": "Mozilla/5.0"}
        # если в BLACK_DOMAINS – парсим только корневую страницу!
        pages_to_check = [""] if black_domain(site_url) else [
            "", "/contacts", "/contact", "/kontakty", "/kontakt", "/about", "/about-us", "/company", "/info"
        ]
        found_emails, found_phones = [], []
        for pageurl in pages_to_check:
            try:
                url = site_url.rstrip("/") + pageurl
                log_func(f"Загружаем: {url}")
                time.sleep(2.5)
                r = requests.get(url, timeout=10, headers=headers)
                text = r.text
                emails = re.findall(r'[\w\.-]+@[\w\.-]+', text)
                for e in emails:
                    if e.lower() not in FORBIDDEN_EMAILS and is_valid_email(e) and e not in found_emails:
                        found_emails.append(e)
                phones = re.findall(r'\+7[\d\-\(\) ]{10,15}', text)
                for p in phones:
                    p = p.strip()
                    if p not in found_phones:
                        found_phones.append(p)
            except Exception as e:
                log_func(f"Ошибка запроса: {e}")
                continue
        # Возвращаем уникальные по 3 шт — строкой через ;
        email = join_unique(found_emails)
        site_phones = join_unique(found_phones)
        return email, site_phones

    # --- Selenium старт ---
    options = webdriver.ChromeOptions()
    options.add_argument("--start-maximized")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    wait = WebDriverWait(driver, 10)
    driver.get("https://yandex.ru/maps")
    log_func("Открыт Яндекс.Карты")
    search_input = wait.until(EC.presence_of_element_located((By.TAG_NAME, "input")))
    log_func("Нашли поле поиска")
    search_input.send_keys(search_query)
    search_input.send_keys(Keys.ENTER)
    time.sleep(5)
    companies = []
    import tkinter.messagebox

    log_func(
        "Пожалуйста, самостоятельно прокрутите список компаний Яндекс.Карт вручную до самого конца, чтобы все карточки подгрузились!")
    tkinter.messagebox.showinfo(
        "Ручная прокрутка",
        "Прокрутите список организаций в Яндекс.Картах до НИЗУ ВРУЧНУЮ (мышкой, колесиком или PageDown), "
        "чтобы ВСЕ компании появились на странице.\n\nПосле этого нажмите OK для запуска парсинга."
    )
    cards = driver.find_elements(By.CSS_SELECTOR, "a[href*='/org/']")
    log_func(f"После прокрутки найдено карточек: {len(cards)}")
    # Далее обработка links идёт как было:
    links = []
    for card in cards:
        try:
            link = card.get_attribute("href")
            if link and "/org/" in link:
                links.append(link)
        except:
            pass
    links = list(set(links))
    log_func(f"Уникальных карточек: {len(links)}")

    links = []
    for card in cards:
        try:
            link = card.get_attribute("href")
            if link and "/org/" in link:
                links.append(link)
        except:
            pass
    links = list(set(links))
    log_func(f"Уникальных карточек: {len(links)}")
    if company_limit and str(company_limit).isdigit() and int(company_limit) > 0:
        links = links[:int(company_limit)]
        log_func(f"Парсинг только первых {company_limit} компаний.")
    else:
        log_func("Парсим все найденные компании.")

    global parser_stop_event
    parser_stop_event.clear()
    companies = []

    # далее как обычно...

    for idx, link in enumerate(links, 1):
        if parser_stop_event.is_set():
            log_func("Операция остановлена оператором.")
            break

        # Если пауза: держим поток, пока не снимут через кнопку
        while parser_pause_event.is_set():
            log_func("ПАРСЕР на паузе...")
            time.sleep(1)
            if parser_stop_event.is_set():
                log_func("Операция остановлена оператором на паузе!")
                return

        log_func(f"\n=== Парсим карточку {idx} ===\nСсылка: {link}")
        try:
            driver.get(link)
            log_func(f"Открываем страницу организации")
            time.sleep(4)
            name, phone, website, address, email, site_phones = "","","","","",""
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
            try:
                page_source = driver.page_source
                emails = re.findall(r'[\w\.-]+@[\w\.-]+', page_source)
                found_good_email = []
                for e in emails:
                    if e.lower() not in FORBIDDEN_EMAILS and is_valid_email(e) and e not in found_good_email:
                        found_good_email.append(e)
                email = join_unique(found_good_email)
                if not email:
                    log_func("Нет email на Яндекс.Картах")
                else:
                    log_func(f"Используем email: {email}")
            except Exception as e:
                log_func(f"Ошибка поиска email: {e}")
            try:
                site_element = driver.find_element(By.XPATH, "//a[contains(@href,'http') and not(contains(@href,'yandex'))]")
                website = site_element.get_attribute("href")
                if website and ('ya.ru' in website or 'yandex.ru' in website):
                    log_func(f"Сайт ведёт на Яндекс ({website}), игнорируем")
                    website = ""
                else:
                    log_func(f"Сайт компании: {website if website else 'Не найден'}")
            except:
                log_func("Сайт компании: не найден")
                website = ""
            try:
                import re
                # Собираем все div с текстом
                all_divs = driver.find_elements(By.TAG_NAME, "div")
                all_texts = []
                for div in all_divs:
                    try:
                        txt = div.text.strip()
                        if txt and len(txt) > 7:
                            all_texts.append(txt)
                    except Exception:
                        pass

                # АДРЕС — ищем первую строку, похожую на адрес
                address = ""
                for t in all_texts:
                    # ищем ключевые слова + не просто "Адрес"
                    if re.search(r"(Россия|Подольск|Чехов|Домодедово|Серпухов|г\.|ул\.|обл\.|д\.|микрорайон|проспект|\d{2,}\s*[а-яА-ЯёЁ]+)",
                                 t) and \
                            "Яндекс" not in t and not t.lower().startswith('адрес'):
                        address = t
                        break
                log_func(f"Автоматически найденный адрес: {address}")

                # ДЕЯТЕЛЬНОСТЬ — ищем длинную строку со словами "услуги", "работы" или "металлообработка"
                occupation = ""
                for t in all_texts:
                    lower = t.lower()
                    if ((
                            "услуги" in lower or "работы" in lower or "деятельност" in lower or "металлообработка" in lower)
                            and len(t) > 15):
                        occupation = t
                        break
                log_func(f"Автоматически найденное описание деятельности: {occupation}")

            except Exception as e:
                log_func(f"Ошибка поиска адреса/деятельности: {e}")
                address = ""
                occupation = ""
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
            else:
                log_func("Сайт не найден, ищем через Яндекс...")
                found_sites = find_sites_from_yandex_via_selenium(driver, name, search_query.split()[-1])
                if found_sites:
                    for site in found_sites:
                        log_func(f"Пробуем сайт из Яндекса: {site}")
                        email_candidate, phone_candidate = parse_contacts_from_site(site, "", "")
                        if email_candidate:
                            log_func(f"На {site} найден email: {email_candidate}")
                            email = email_candidate
                            website = site
                            break
                        if phone_candidate and not site_phones:
                            site_phones = phone_candidate
                    if not email:
                        log_func("Не удалось найти email на первых 3 сайтах Яндекса")
                else:
                    log_func("Не найден ни один сайт через Яндекс. Пропускаем.")

            # --- Проверка уникальности ---
            def check_duplicate(new_entry):
                if df_main.empty: return False
                for _, row in df_main.iterrows():
                    matches = 0
                    if pd.notna(row.get("Название")) and row["Название"].strip().lower() == new_entry["Название"].strip().lower():
                        matches += 1
                    for col in ["Сайт", "Email", "Телефон (сайт)", "Телефон (Яндекс)", "Адрес"]:
                        if pd.notna(row.get(col)) and row.get(col) and new_entry.get(col) and any(s in row[col] for s in new_entry[col].split(";")):
                            matches += 1
                    if matches >= 2:
                        return True
                return False

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
            dup = check_duplicate(new_info)
            if dup:
                log_func("!! Найден дубликат по 2+ полям, СКИПУЕМ карточку.")
                continue

            companies.append(new_info)

            # Сохраняем после каждой компании
            if len(companies) > 0:
                # Добавить к старой базе
                df_add = pd.DataFrame([new_info])
                if os.path.exists(EXCEL_FILENAME):
                    df_main_cur = pd.read_excel(EXCEL_FILENAME)
                    df_final = pd.concat([df_main_cur, df_add], ignore_index=True)
                else:
                    df_final = df_add
                df_final.to_excel(EXCEL_FILENAME, index=False)
                log_func("Результаты по текущей компании сохранены в базу.")

                # Автоматическое обновление окна просмотра базы
                try:
                    if database_window is not None and database_window.winfo_exists():
                        # перерисовать содержимое (функцию show_frame(filtered_df) надо перевести во внешний scope)
                        df = pd.read_excel(EXCEL_FILENAME)
                        show_frame(df)
                except Exception as ex:
                    log_func(f"(Не удалось обновить окно просмотра базы: {ex})")
                    df_final.to_excel(EXCEL_FILENAME, index=False)

                    try:
                        if (database_window is not None
                                and hasattr(database_window, "winfo_exists") and database_window.winfo_exists()):
                            df_ = pd.read_excel(EXCEL_FILENAME)
                            show_db_table(df_)
                    except Exception as ex:
                        log_func(f"(Не удалось обновить окно просмотра базы: {ex})")

                    log_func("Результаты по текущей компании сохранены в базу.")

            log_func(f"--- Итог по карточке ---\nEmail: {email}\nТелефон (Яндекс): {phone}\nТелефон (сайт): {site_phones}\nСайт: {website}\nАдрес: {address}\n")
        except Exception as e:
            log_func(f"ОШИБКА ГЛАВНОГО ЦИКЛА: {e}")

    driver.quit()
    # --- Добавляем к базе, обновляем файл ---
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

def log(msg):
    log_text.configure(state="normal")
    log_text.insert("end", msg + "\n")
    log_text.see("end")
    log_text.configure(state="disabled")
    root.update()

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
        # Если окно уже открыто — просто обновить таблицу и сфокусироваться
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

    # Scrollbar
    vsb = ttk.Scrollbar(frm, orient="vertical", command=db_tree.yview)
    db_tree.configure(yscrollcommand=vsb.set)
    vsb.pack(side="right", fill="y")

    show_db_table(df)

root.mainloop()