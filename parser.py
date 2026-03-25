import customtkinter as ctk
import threading
import datetime
from tkinter.scrolledtext import ScrolledText
from tkinter import ttk
import pandas as pd
import os

BLACK_DOMAINS = ["vk.com", "avito.ru", "avito.com", "hh.ru", "ok.ru", "youtube.com", "facebook.com", "instagram.com",
                 "twitter.com", "t.me", "2gis.ru", ".yandex.", "ya.ru", "mail.ru", "rb.ru", "google.com", "google.ru"]

EXCEL_FILENAME = "contacts_database.xlsx"  # Файл с единой базой

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
    log_func("Скроллим список...")
    for _ in range(10):
        driver.find_element(By.TAG_NAME, "body").send_keys(Keys.PAGE_DOWN)
        time.sleep(1)
    cards = driver.find_elements(By.CSS_SELECTOR, "a[href*='/org/']")
    log_func(f"Найдено карточек: {len(cards)}")
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

    for idx, link in enumerate(links, 1):
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
                address = driver.find_element(By.XPATH, "//div[contains(text(),'Адрес')]").text
                log_func(f"Адрес: {address}")
            except:
                log_func("Адрес: не найден")
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
                "Адрес": address
            }
            dup = check_duplicate(new_info)
            if dup:
                log_func("!! Найден дубликат по 2+ полям, СКИПУЕМ карточку.")
                continue

            companies.append(new_info)
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
root.title("Яндекс-Карты КонтактПарсер - Единая База")

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

btn_dbview = ctk.CTkButton(frame, text="Посмотреть Базу", command=lambda: open_db_view(), width=180, height=38)
btn_dbview.pack(anchor="w", pady=(0,14))

lbl_log = ctk.CTkLabel(frame, text="Сообщения отладки / ход работы:")
lbl_log.pack(anchor="w", pady=(0,5))
log_text = ScrolledText(frame, height=24, width=100, bg="#212223", fg="#D6D6D6", font=("Consolas", 12), wrap="word", state="disabled", insertbackground="white")
log_text.pack(fill="both", expand=True, padx=(0,0), pady=(0,10))

def open_db_view():
    global database_window
    if database_window is not None and database_window.winfo_exists():
        database_window.focus_set()
        return
    if not os.path.exists(EXCEL_FILENAME):
        from tkinter import messagebox
        messagebox.showinfo("Нет базы", "Файл базы еще не создан. Сначала сделайте хотя бы один парсинг.")
        return
    df = pd.read_excel(EXCEL_FILENAME)
    database_window = ctk.CTkToplevel(root)
    database_window.title("Просмотр базы данных")
    database_window.geometry("1200x650")

    frm = ctk.CTkFrame(database_window)
    frm.pack(fill="both", expand=True)
    tree = ttk.Treeview(frm, show="headings")
    # Настройка колонок
    columns = list(df.columns)
    tree["columns"] = columns
    for col in columns:
        tree.heading(col, text=col)
        tree.column(col, width=160, anchor="w")
    # Заполняем строки (limit для памяти, можно убрать если нужно больше)
    for idx, row in df.iterrows():
        tree.insert("", "end", values=[str(row[col]) if pd.notna(row[col]) else "" for col in columns])
        if idx > 999: break  # Показываем не более 1000 строк для скорости
    # Добавляем scroll-бар
    vsb = ttk.Scrollbar(frm, orient="vertical", command=tree.yview)
    tree.configure(yscrollcommand=vsb.set)
    vsb.pack(side="right", fill="y")
    tree.pack(fill="both", expand=True, side="left")

root.mainloop()