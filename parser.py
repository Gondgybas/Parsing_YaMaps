from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import time
import pandas as pd
import re
import requests
from bs4 import BeautifulSoup
import urllib.parse

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
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'e', 'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y',
        'к': 'k',
        'л': 'l', 'м': 'm', 'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u', 'ф': 'f', 'х': 'h',
        'ц': 'ts',
        'ч': 'ch', 'ш': 'sh', 'щ': 'sch', 'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya', ' ': '_', '-': '-'
    }
    result = ""
    for ch in name.lower():
        result += table.get(ch, ch)
    return result


def find_sites_from_yandex(company_name, city):
    print("=== ОТЛАДКА ВЫДАЧИ ЯНДЕКСА ===")
    search_variants = [
        f"{company_name} {city} сайт",
        f"{transliterate_name(company_name)} {city} сайт",
        f"{company_name.replace('-', ' ')} {city} сайт"
    ]
    headers = {"User-Agent": "Mozilla/5.0"}
    result_sites = []
    for query in search_variants:
        print(f"\nYandex search query: {query}")
        url = "https://yandex.ru/search/?text=" + urllib.parse.quote_plus(query)
        print(f"Yandex search url: {url}")
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code != 200:
                print("Яндекс не отдал страницу или капча.")
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            all_links = []
            # Яндекс часто использует класс "Link" для ссылок на сайты в выдаче
            for a in soup.find_all('a', href=True):
                href = a['href']
                text = a.get_text(strip=True)
                print(f"!!! НАЙДЕН HREF: {href} text: {text}")
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
            if all_links:
                print("Списо�� сайтов, которые попадут в парсинг:")
                for i, site in enumerate(all_links, 1):
                    print(f"{i}) {site}")
                return all_links
            else:
                print("На этом поисковом запросе ни одной нормальной ссылки не найдено")
        except Exception as e:
            print(f"Ошибка поиска сайта в Яндексе: {e}")
            continue
    print("! НЕ найдено ни одной ссылки в Яндексе ни по одному поисковому варианту")
    return result_sites


def parse_contacts_from_site(site_url, email, site_phones):
    headers = {"User-Agent": "Mozilla/5.0"}
    pages_to_check = [
        "",
        "/contacts",
        "/contact",
        "/kontakty",
        "/kontakt",
        "/about",
        "/about-us",
        "/company",
        "/info"
    ]
    for pageurl in pages_to_check:
        try:
            url = site_url.rstrip("/") + pageurl
            print(f"Загружаем: {url}")
            time.sleep(4)
            r = requests.get(url, timeout=10, headers=headers)
            text = r.text
            emails = re.findall(r'[\w\.-]+@[\w\.-]+', text)
            found_email_here = False
            for e in emails:
                print(f"На сайте найден email: {e}")
                if e.lower() not in FORBIDDEN_EMAILS and is_valid_email(e) and not email:
                    print(f"Используем email с сайта: {e}")
                    email = e
                    found_email_here = True
                    break
            if found_email_here and email:
                break
            phones = re.findall(r'\+7[\d\-\(\) ]{10,15}', text)
            for p in phones:
                print(f"На сайте найден телефон: {p}")
            if phones and not site_phones:
                site_phones = phones[0]
                print(f"Используем телефон с сайта: {site_phones}")
        except Exception as e:
            print(f"Ошибка получения/парсинга сайта: {e}")
            continue
    return email, site_phones

def find_sites_from_yandex_via_selenium(driver, company_name, city):
    import urllib.parse
    from bs4 import BeautifulSoup
    import time

    print("=== ОТЛАДКА В��ДАЧИ ЯНДЕКСА (SELENIUM) ===")
    search_variants = [
        f"{company_name} {city} сайт",
        f"{transliterate_name(company_name)} {city} сайт",
        f"{company_name.replace('-', ' ')} {city} сайт"
    ]
    result_sites = []
    for query in search_variants:
        print(f"\nYandex search query: {query}")
        url = "https://yandex.ru/search/?text=" + urllib.parse.quote_plus(query)
        print(f"Открываю браузер с Яндекс-поиском: {url}")
        driver.execute_script("window.open('');")
        driver.switch_to.window(driver.window_handles[-1])
        driver.get(url)
        time.sleep(10)   # ждём ручного прохождения капчи (если она появилась)
        # выводим предупреждение если есть текст капчи
        html = driver.page_source
        if "smart-captcha" in html or "Капча" in html or "captcha" in html:
            print("=== КАПЧА === Пройди её вручную в окне selenium и продолжи")
            input("Нажми ENTER когда капча решена и страница с выдачей загружена...")
        soup = BeautifulSoup(driver.page_source, "html.parser")
        all_links = []
        for a in soup.find_all('a', href=True):
            href = a['href']
            text = a.get_text(strip=True)
            print(f"!!! НАЙДЕН HREF: {href} text: {text}")
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
            print("Список сайтов, которые попадут в парсинг:")
            for i, site in enumerate(all_links, 1):
                print(f"{i}) {site}")
            return all_links
        else:
            print("На этом поисковом запросе ни одной нормальной ссылки не найдено")
    print("! НЕ найдено ни одной ссылки в Яндексе ни по одному поисковому варианту")
    return result_sites

# --- Основная программа ---

options = webdriver.ChromeOptions()
options.add_argument("--start-maximized")

driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
wait = WebDriverWait(driver, 10)

search_query = "металлообработка Подольск"

driver.get("https://yandex.ru/maps")
print("Открыт Яндекс.Карты")

search_input = wait.until(EC.presence_of_element_located((By.TAG_NAME, "input")))
print("Нашли поле поиска")
search_input.send_keys(search_query)
search_input.send_keys(Keys.ENTER)

time.sleep(5)

companies = []

print("Скроллим список...")
for _ in range(10):
    driver.find_element(By.TAG_NAME, "body").send_keys(Keys.PAGE_DOWN)
    time.sleep(2)

cards = driver.find_elements(By.CSS_SELECTOR, "a[href*='/org/']")
print(f"Найдено карточек: {len(cards)}")

links = []
for card in cards:
    try:
        link = card.get_attribute("href")
        if link and "/org/" in link:
            links.append(link)
    except:
        pass

links = list(set(links))
print(f"Уникальных карточек: {len(links)}")
links = links[:3]

for idx, link in enumerate(links, 1):
    print(f"\n=== Парсим карточку {idx} ===")
    print(f"Ссылка: {link}")
    try:
        driver.get(link)
        print(f"Открываем страницу организации")
        time.sleep(5)

        name = ""
        phone = ""
        website = ""
        address = ""
        email = ""
        site_phones = ""

        try:
            name = driver.find_element(By.TAG_NAME, "h1").text
            print(f"Название: {name}")
        except Exception as e:
            print(f"Ошибка поиска названия: {e}")

        try:
            phone = driver.find_element(By.XPATH, "//a[contains(@href,'tel')]").text
            print(f"Телефон (Яндекс): {phone}")
        except:
            print("Телефон (Яндекс): не найден")

        try:
            page_source = driver.page_source
            emails = re.findall(r'[\w\.-]+@[\w\.-]+', page_source)
            found_good_email = False
            for e in emails:
                print(f"Найден email на странице Яндекса: {e}")
                if e.lower() not in FORBIDDEN_EMAILS and is_valid_email(e):
                    email = e
                    found_good_email = True
                    print(f"Используем email: {email}")
                    break
            if not found_good_email:
                print("Нет пригодного email на странице Яндекса (только служебные или ничего не найдено)")
        except Exception as e:
            print(f"Ошибка поиска email: {e}")

        try:
            site_element = driver.find_element(By.XPATH,
                                               "//a[contains(@href,'http') and not(contains(@href,'yandex'))]")
            website = site_element.get_attribute("href")
            if website and ('ya.ru' in website or 'yandex.ru' in website):
                print(f"Сайт ведет на Яндекс ({website}), игнорируем")
                website = ""
            else:
                print(f"Сайт компании: {website if website else 'Не найден'}")
        except:
            print("Сайт компании: не найден")
            website = ""

        try:
            address = driver.find_element(By.XPATH, "//div[contains(text(),'Адрес')]").text
            print(f"Адрес: {address}")
        except:
            print("Адрес: не найден")

        if website:
            print(f"Парсим сайт компании: {website}")
            try:
                email, site_phones = parse_contacts_from_site(website, email, site_phones)
            except Exception as e:
                print(f"Ошибка обхода страниц сайта компании: {e}")
        else:
            print("Сайт НЕ найден на Яндекс.Картах, ищем через Яндекс-поиск...")
            found_sites = find_sites_from_yandex_via_selenium(driver, name, search_query.split()[-1])
            if found_sites:
                for site in found_sites:
                    print(f"Пробуем сайт из Яндекса: {site}")
                    email_candidate, phone_candidate = parse_contacts_from_site(site, "", "")
                    if email_candidate:
                        print(f"На {site} найден email: {email_candidate}")
                        email = email_candidate
                        website = site
                        break
                    if phone_candidate and not site_phones:
                        site_phones = phone_candidate
                if not email:
                    print("Не удалось найти email на первых 3 сайтах Яндекса")
            else:
                print("Не найден ни один сайт организации через Яндекс. Пропускаем парсинг.")

        companies.append({
            "Название": name,
            "Телефон (Яндекс)": phone,
            "Телефон (сайт)": site_phones,
            "Email": email,
            "Сайт": website,
            "Адрес": address
        })

        print("--- Итог по карточке ---")
        print(f"Email: {email}")
        print(f"Телефон (Яндекс): {phone}")
        print(f"Телефон (сайт): {site_phones}")
        print(f"Сайт: {website}")
        print(f"Адрес: {address}")

    except Exception as e:
        print("ОШИБКА ГЛАВНОГО ЦИКЛА:", e)

print("\n=== Сохраняем в Excel ===")
df = pd.DataFrame(companies)
df.to_excel("yandex_leads.xlsx", index=False)

driver.quit()
print("Готово! Проверьте yandex_leads.xlsx и сообщения выше для отладки.")