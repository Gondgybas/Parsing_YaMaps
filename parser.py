import customtkinter as ctk
import threading
import datetime
from tkinter.scrolledtext import ScrolledText
import pandas as pd

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

    def is_valid_email(email):
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
        printlog("=== ОТЛАДКА ВЫДАЧИ ЯНДЕКСА (SELENIUM) ===")
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
                printlog("=== КАПЧА === Пройди её в окне selenium, затем продолжится.\nЖми 'OK' в браузере после прохождения капчи.")
                ctk.CTkMessagebox(title="Капча!", message="Реши капчу в Яндексе браузера, потом закрой вкладку вручную и нажми ОК...", icon="warning")
            soup = BeautifulSoup(driver.page_source, "html.parser")
            all_links = []
            for a in soup.find_all('a', href=True):
                href = a['href']
                text = a.get_text(strip=True)
                printlog(f"!!! НАЙДЕН HREF: {href} text: {text}")
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
        printlog("! Ни одной нормальной ссылки в Яндексе по всем вариантам")
        return result_sites

    def parse_contacts_from_site(site_url, email, site_phones):
        headers = {"User-Agent": "Mozilla/5.0"}
        pages_to_check = ["","/contacts","/contact","/kontakty","/kontakt","/about","/about-us","/company","/info"]
        for pageurl in pages_to_check:
            try:
                url = site_url.rstrip("/") + pageurl
                log_func(f"Загружаем: {url}")
                time.sleep(3)
                r = requests.get(url, timeout=10, headers=headers)
                text = r.text
                emails = re.findall(r'[\w\.-]+@[\w\.-]+', text)
                for e in emails:
                    log_func(f"На сайте найден email: {e}")
                    if e.lower() not in FORBIDDEN_EMAILS and is_valid_email(e) and not email:
                        log_func(f"Используем email с сайта: {e}")
                        email = e
                        break
                phones = re.findall(r'\+7[\d\-\(\) ]{10,15}', text)
                for p in phones:
                    log_func(f"На сайте найден телефон: {p}")
                if phones and not site_phones:
                    site_phones = phones[0]
                    log_func(f"Используем телефон с сайта: {site_phones}")
            except Exception as e:
                log_func(f"Ошибка запроса: {e}")
                continue
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
        time.sleep(1.0)
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
                found_good_email = False
                for e in emails:
                    log_func(f"Найден email на Яндексе: {e}")
                    if e.lower() not in FORBIDDEN_EMAILS and is_valid_email(e):
                        email = e
                        found_good_email = True
                        log_func(f"Используем email: {email}")
                        break
                if not found_good_email:
                    log_func("Нет подходящего email на Яндекс.Картах")
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
                    email, site_phones = parse_contacts_from_site(website, email, site_phones)
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
            companies.append({
                "Название": name,
                "Телефон (Яндекс)": phone,
                "Телефон (сайт)": site_phones,
                "Email": email,
                "Сайт": website,
                "Адрес": address
            })
            log_func(f"--- Итог по карточке ---\nEmail: {email}\nТелефон (Яндекс): {phone}\nТелефон (сайт): {site_phones}\nСайт: {website}\nАдрес: {address}\n")
        except Exception as e:
            log_func(f"ОШИБКА ГЛАВНОГО ЦИКЛА: {e}")
    driver.quit()
    now = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    fname = f'yandex_leads_{now}_{search_query.replace(" ","_")[:30]}.xlsx'
    pd.DataFrame(companies).to_excel(fname, index=False)
    log_func(f"\n=== Сохраняем в файл: {fname}\nГотово!")

# --------- GUI ---------

try:
    import customtkinter as ctk
except ImportError:
    raise SystemExit("Перед запуском: pip install customtkinter")

ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("dark-blue")

root = ctk.CTk()
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

lbl_log = ctk.CTkLabel(frame, text="Сообщения отладки / ход работы:")
lbl_log.pack(anchor="w", pady=(0,5))
log_text = ScrolledText(frame, height=24, width=100, bg="#212223", fg="#D6D6D6", font=("Consolas", 12), wrap="word", state="disabled", insertbackground="white")
log_text.pack(fill="both", expand=True, padx=(0,0), pady=(0,10))

root.mainloop()