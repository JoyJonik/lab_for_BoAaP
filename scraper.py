#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для сбора данных о компаниях с сайта pappers.fr
Использует Playwright для обхода защиты и сбора данных
"""

import asyncio
import sys
import re
import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
from datetime import datetime
import logging

# ИСПРАВЛЕНИЕ ДЛЯ PYTHON 3.13 НА WINDOWS
if sys.platform == 'win32':
    # Используем ProactorEventLoop, который поддерживает подпроцессы (необходимо для Playwright)
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PappersPlaywrightScraper:
    """Скрепер на базе Playwright для pappers.fr"""
    
    def __init__(self):
        self.browser = None
        self.context = None
        self.page = None
        self.results = []
    
    async def init_browser(self, headless=False):
        """Инициализация браузера"""
        logger.info("Запуск браузера Playwright...")
        
        playwright = await async_playwright().start()
        
        # Запускаем браузер с настройками для обхода защиты
        self.browser = await playwright.chromium.launch(
            headless=headless,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-web-security',
                '--disable-features=IsolateOrigins,site-per-process',
            ]
        )
        
        # Создаем контекст с нужными настройками
        self.context = await self.browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            locale='fr-FR',
            timezone_id='Europe/Paris',
        )
        
        # Открываем страницу
        self.page = await self.context.new_page()
        
        # Скрываем признак автоматизации
        await self.page.evaluate("""() => {
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        }""")
        
        logger.info("Браузер запущен")
    
    async def close_cookies(self):
        """Закрытие окна с куки"""
        try:
            # Ищем кнопку принятия куки
            cookie_button = await self.page.wait_for_selector(
                "button:has-text('Accepter'), button:has-text('Tout accepter'), button:has-text('Accepter & Fermer'), button:has-text('Refuser')",
                timeout=5000
            )
            if cookie_button:
                await cookie_button.click()
                logger.info("Куки приняты/отклонены")
                await asyncio.sleep(1)
        except:
            logger.debug("Окно с куки не найдено")
    
    async def scrape_search_pages_manual(self, total_pages=20):
        """Ручной парсинг поисковых страниц - пользователь сам переключает страницы"""
        base_url = "https://www.pappers.fr/recherche?activite=20.42Z&capital_min=0&capital_max=10000000000&effectifs_min=1&effectifs_max=500000&date_publication_min=03-10-2025&date_publication_max=02-04-2026&page=1"
        
        all_companies = []
        
        logger.info(f"=== РУЧНОЙ РЕЖИМ: Откройте браузер и перейдите по {total_pages} страницам ===")
        logger.info(f"Начальная URL: {base_url}")
        logger.info("После загрузки каждой страницы скрипт автоматически соберет компании.")
        logger.info(f"Необходимо вручную перейти на {total_pages} страниц (нажимая стрелку вправо).")
        logger.info("После сбора компаний со всех страниц начнется автоматический сбор детальных данных.")
        
        # Загружаем первую страницу
        await self.page.goto(base_url, wait_until='networkidle')
        await asyncio.sleep(3)
        await self.close_cookies()
        
        for page_num in range(1, total_pages + 1):
            logger.info(f"\n{'='*60}")
            logger.info(f"Ожидание данных страницы {page_num}/{total_pages}...")
            logger.info(f"{'='*60}")
            
            try:
                # Ждем загрузки результатов
                await self.page.wait_for_selector('.container-resultat', timeout=30000)
                await asyncio.sleep(2)
                
                # Находим все блоки с компаниями
                company_blocks = await self.page.query_selector_all('.container-resultat')
                
                logger.info(f"Найдено {len(company_blocks)} компаний на странице {page_num}")
                
                for block in company_blocks:
                    try:
                        # Извлекаем ссылку на компанию
                        link_elem = await block.query_selector('a[href^="/entreprise/"]')
                        if link_elem:
                            href = await link_elem.get_attribute('href')
                            full_url = f"https://www.pappers.fr{href}"
                            
                            # Извлекаем название компании
                            name_elem = await block.query_selector('.nom-entreprise a.gros-nom')
                            company_name = ''
                            if name_elem:
                                company_name = await name_elem.text_content()
                                # Очищаем от HTML тегов ya-tr-span
                                company_name = re.sub(r'<[^>]+>', '', company_name).strip()
                            
                            # Извлекаем дополнительную информацию из блока
                            info = {
                                'company_name': company_name,
                                'url': full_url,
                                'page_source': page_num
                            }
                            
                            # Юридическая форма
                            forme_elem = await block.query_selector('p.key:has-text("Forme Juridique") + p.value')
                            if forme_elem:
                                info['legal_form'] = await forme_elem.text_content()
                            else:
                                info['legal_form'] = ''
                            
                            # Деятельность
                            activity_elem = await block.query_selector('p.key:has-text("Activité") + p.value')
                            if activity_elem:
                                info['activity'] = await activity_elem.text_content()
                            else:
                                info['activity'] = ''
                            
                            # Код NAF
                            naf_elem = await block.query_selector('p.key:has-text("Code NAF")')
                            if naf_elem:
                                naf_text = await naf_elem.text_content()
                                info['naf_code'] = naf_text.replace('Code NAF :', '').strip()
                            else:
                                info['naf_code'] = ''
                            
                            # Местоположение
                            location_elem = await block.query_selector('p.key:has-text("Lieu") + p.value')
                            if location_elem:
                                info['location'] = await location_elem.text_content()
                            else:
                                info['location'] = ''
                            
                            # Почтовый индекс
                            postal_elem = await block.query_selector('p.key:has-text("Code postal")')
                            if postal_elem:
                                postal_text = await postal_elem.text_content()
                                info['postal_code'] = postal_text.replace('Code postal :', '').strip()
                            else:
                                info['postal_code'] = ''
                            
                            # Количество сотрудников
                            effectif_elem = await block.query_selector('p.key:has-text("Effectif") + span.value')
                            if effectif_elem:
                                info['employees'] = await effectif_elem.text_content()
                            else:
                                info['employees'] = ''
                            
                            # Капитал
                            capital_elem = await block.query_selector('p.key:has-text("Capital") + span.value')
                            if capital_elem:
                                info['capital'] = await capital_elem.text_content()
                            else:
                                info['capital'] = ''
                            
                            all_companies.append(info)
                            logger.info(f"  -> Добавлена компания: {company_name}")
                    
                    except Exception as e:
                        logger.warning(f"Ошибка при парсинге компании: {e}")
                        continue
                
                # Если это не последняя страница, просим пользователя перейти дальше
                if page_num < total_pages:
                    logger.info(f"\n{'='*60}")
                    logger.info(f"Страница {page_num} обработана. Собрано {len([c for c in all_companies if c['page_source'] == page_num])} компаний.")
                    logger.info(f"Всего собрано компаний: {len(all_companies)}")
                    logger.info(f"ПОЖАЛУЙСТА, НАЖМИТЕ СТРЕЛКУ ВПРАВО для перехода на страницу {page_num + 1}")
                    logger.info(f"Скрипт продолжит автоматически после обнаружения новых данных...")
                    logger.info(f"{'='*60}\n")
                    
                    # Ждем появления новой страницы (ожидание изменения контента)
                    await self.wait_for_next_page(page_num)
                
            except PlaywrightTimeout:
                logger.warning(f"Таймаут при ожидании страницы {page_num}")
                if page_num < total_pages:
                    logger.info(f"Попробуйте вручную перейти на страницу {page_num + 1}")
                    await self.wait_for_next_page(page_num)
                continue
            except Exception as e:
                logger.error(f"Ошибка при парсинге страницы {page_num}: {e}")
                if page_num < total_pages:
                    await self.wait_for_next_page(page_num)
                continue
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Все {total_pages} страниц обработаны!")
        logger.info(f"Всего найдено {len(all_companies)} компаний")
        logger.info(f"{'='*60}")
        return all_companies
    
    async def wait_for_next_page(self, current_page):
        """Ожидание перехода на следующую страницу"""
        import time
        
        max_wait = 120  # Максимальное время ожидания 2 минуты на страницу
        wait_interval = 0.5
        waited = 0
        
        # Запоминаем текущие компании
        prev_count = len(await self.page.query_selector_all('.container-resultat'))
        
        while waited < max_wait:
            await asyncio.sleep(wait_interval)
            waited += wait_interval
            
            try:
                # Проверяем, появились ли новые элементы
                current_blocks = await self.page.query_selector_all('.container-resultat')
                current_count = len(current_blocks)
                
                # Если количество изменилось или прошло достаточно времени, проверяем контент
                if current_count != prev_count:
                    # Даем немного времени на полную загрузку
                    await asyncio.sleep(2)
                    break
                
                # Проверяем номер страницы в пагинации
                page_indicator = await self.page.query_selector('.texte-droite p')
                if page_indicator:
                    page_text = await page_indicator.text_content()
                    # Если номер страницы изменился, выходим
                    if page_text and str(current_page + 1) in page_text:
                        await asyncio.sleep(2)
                        break
                        
            except:
                pass
        
        if waited >= max_wait:
            logger.warning("Превышено время ожидания. Переходим к следующей странице или завершаем...")
    
    async def extract_company_data(self, url):
        """Извлечение данных со страницы компании на pappers.fr"""
        try:
            logger.info(f"Сбор данных: {url}")
            
            await self.page.goto(url, wait_until='networkidle')
            await asyncio.sleep(3)
            
            # Закрываем куки если есть
            await self.close_cookies()
            
            data = {}
            
            # Название компании - из заголовка h1 или title
            try:
                name_elem = await self.page.wait_for_selector('h1', timeout=5000)
                if name_elem:
                    full_name = await name_elem.text_content()
                    # Очищаем от лишнего текста
                    data['company_name'] = full_name.replace('Informations juridiques de', '').strip()
                else:
                    data['company_name'] = ''
            except:
                data['company_name'] = ''
            
            # Секция informations - основные данные
            try:
                info_section = await self.page.query_selector('#informations')
                if info_section:
                    # SIREN
                    siren_elem = await info_section.query_selector('th:has-text("SIREN") + td')
                    if siren_elem:
                        siren_text = await siren_elem.text_content()
                        data['siren'] = re.sub(r'\s+', '', siren_text.split()[0]) if siren_text else ''
                    else:
                        data['siren'] = ''
                    
                    # SIRET
                    siret_elem = await info_section.query_selector('th:has-text("SIRET") + td')
                    if siret_elem:
                        siret_text = await siret_elem.text_content()
                        data['siret'] = re.sub(r'\s+', '', siret_text.split()[0]) if siret_text else ''
                    else:
                        data['siret'] = ''
                    
                    # Форма юридическая
                    forme_elem = await info_section.query_selector('th:has-text("Forme juridique") + td')
                    if forme_elem:
                        data['legal_form'] = await forme_elem.text_content()
                    else:
                        data['legal_form'] = ''
                    
                    # Номер НДС
                    tva_elem = await info_section.query_selector('th:has-text("Numéro de TVA") + td')
                    if tva_elem:
                        tva_text = await tva_elem.text_content()
                        # Извлекаем только номер НДС
                        tva_match = re.search(r'FR\d+', tva_text)
                        data['vat_number'] = tva_match.group(0) if tva_match else ''
                    else:
                        data['vat_number'] = ''
                    
                    # Регистрация RCS
                    rcs_elem = await info_section.query_selector('th:has-text("Inscription au RCS") + td')
                    if rcs_elem:
                        data['rcs_registration'] = await rcs_elem.text_content()
                    else:
                        data['rcs_registration'] = ''
                    
                    # Капитал
                    capital_elem = await info_section.query_selector('th:has-text("Capital social") + td')
                    if capital_elem:
                        data['capital'] = await capital_elem.text_content()
                    else:
                        data['capital'] = ''
            except Exception as e:
                logger.warning(f"Ошибка при сборе секции informations: {e}")
                data.update({'siren': '', 'siret': '', 'legal_form': '', 'vat_number': '', 'rcs_registration': '', 'capital': ''})
            
            # Секция activite - информация о деятельности
            try:
                activity_section = await self.page.query_selector('#activite')
                if activity_section:
                    # Основная деятельность
                    main_activity_elem = await activity_section.query_selector('th:has-text("Activité principale") + td')
                    if main_activity_elem:
                        activity_text = await main_activity_elem.text_content()
                        # Удаляем "Voir plus"
                        data['main_activity'] = re.sub(r'Voir\s*plus', '', activity_text).strip()
                    else:
                        data['main_activity'] = ''
                    
                    # Код NAF
                    naf_elem = await activity_section.query_selector('th:has-text("Code NAF") + td')
                    if naf_elem:
                        naf_text = await naf_elem.text_content()
                        # Извлекаем код NAF
                        naf_match = re.search(r'[\d.]+[A-Z]', naf_text)
                        data['naf_code'] = naf_match.group(0) if naf_match else ''
                        # Описание деятельности
                        naf_desc_match = re.search(r'\(([^)]+)\)', naf_text)
                        data['naf_description'] = naf_desc_match.group(1) if naf_desc_match else ''
                    else:
                        data['naf_code'] = ''
                        data['naf_description'] = ''
                    
                    # Область деятельности
                    domain_elem = await activity_section.query_selector('th:has-text("Domaine") + td')
                    if domain_elem:
                        data['activity_domain'] = await domain_elem.text_content()
                    else:
                        data['activity_domain'] = ''
                    
                    # Дата закрытия финансового года
                    closing_date_elem = await activity_section.query_selector('th:has-text("clôture") + td')
                    if closing_date_elem:
                        data['accounting_closing_date'] = await closing_date_elem.text_content()
                    else:
                        data['accounting_closing_date'] = ''
            except Exception as e:
                logger.warning(f"Ошибка при сборе секции activite: {e}")
                data.update({'main_activity': '', 'naf_code': '', 'naf_description': '', 'activity_domain': '', 'accounting_closing_date': ''})
            
            # Секция etablissements - адреса и заведения
            try:
                etab_section = await self.page.query_selector('#etablissements')
                if etab_section:
                    establishments = []
                    etab_items = await etab_section.query_selector_all('li.etablissement')
                    
                    for i, etab in enumerate(etab_items[:5]):  # Берем первые 5 заведений
                        etab_data = {}
                        
                        # Тип заведения (сиж или нет)
                        type_elem = await etab.query_selector('.top span.flex-column span:first-child')
                        if type_elem:
                            etab_data['type'] = await type_elem.text_content()
                        else:
                            etab_data['type'] = ''
                        
                        # Статус
                        status_elem = await etab.query_selector('.top p.color-main')
                        if status_elem:
                            etab_data['status'] = await status_elem.text_content()
                        else:
                            etab_data['status'] = ''
                        
                        # SIRET заведения
                        siret_elem = await etab.query_selector('.siret')
                        if siret_elem:
                            siret_text = await siret_elem.text_content()
                            etab_data['siret'] = re.sub(r'\s+', '', siret_text.split()[0]) if siret_text else ''
                        else:
                            etab_data['siret'] = ''
                        
                        # Адрес
                        address_elem = await etab.query_selector('.bottom div:first-child')
                        if address_elem:
                            address_text = await address_elem.text_content()
                            etab_data['address'] = address_text.replace('Adresse :', '').strip()
                        else:
                            etab_data['address'] = ''
                        
                        # Дата создания
                        creation_elem = await etab.query_selector('div:has-text("Date de création")')
                        if creation_elem:
                            creation_text = await creation_elem.text_content()
                            etab_data['creation_date'] = creation_text.replace('Date de création :', '').strip()
                        else:
                            etab_data['creation_date'] = ''
                        
                        # Коммерческое имя
                        name_elem = await etab.query_selector('div:has-text("Nom commercial")')
                        if name_elem:
                            name_text = await name_elem.text_content()
                            etab_data['trade_name'] = name_text.replace('Nom commercial :', '').strip()
                        else:
                            etab_data['trade_name'] = ''
                        
                        establishments.append(etab_data)
                    
                    data['establishments'] = establishments
                    # Также сохраняем адрес головного офиса отдельно
                    if establishments:
                        data['headquarters_address'] = establishments[0].get('address', '')
                else:
                    data['establishments'] = []
                    data['headquarters_address'] = ''
            except Exception as e:
                logger.warning(f"Ошибка при сборе секции etablissements: {e}")
                data.update({'establishments': [], 'headquarters_address': ''})
            
            # Секция finances - финансовые данные
            try:
                finances_section = await self.page.query_selector('#finances')
                if finances_section:
                    financial_data = {}
                    
                    # Ищем таблицу с финансовыми показателями
                    tables = await finances_section.query_selector_all('table')
                    for table in tables:
                        rows = await table.query_selector_all('tr')
                        for row in rows:
                            cells = await row.query_selector_all('th, td')
                            if len(cells) >= 2:
                                label_cell = cells[0]
                                label = await label_cell.text_content()
                                
                                # Получаем значения по годам (обычно 2 колонки с годами)
                                values = []
                                for j in range(1, min(len(cells), 4)):
                                    val = await cells[j].text_content()
                                    values.append(val.strip() if val else '')
                                
                                # Сохраняем данные
                                label_clean = label.strip().lower()
                                if "chiffre d'affaires" in label_clean or 'ca' in label_clean:
                                    financial_data['revenue'] = values
                                elif 'résultat net' in label_clean or 'resultat' in label_clean:
                                    financial_data['net_result'] = values
                                elif 'ebitda' in label_clean:
                                    financial_data['ebitda'] = values
                                elif 'marge brute' in label_clean:
                                    financial_data['gross_margin'] = values
                                elif 'fonds propres' in label_clean:
                                    financial_data['equity'] = values
                                elif 'dettes financières' in label_clean:
                                    financial_data['financial_debt'] = values
                                elif 'trésorerie' in label_clean:
                                    financial_data['cash'] = values
                                elif 'effectif' in label_clean or 'salariés' in label_clean:
                                    financial_data['employees'] = values
                    
                    data['financials'] = financial_data
                else:
                    data['financials'] = {}
            except Exception as e:
                logger.warning(f"Ошибка при сборе секции finances: {e}")
                data['financials'] = {}
            
            # Секция contact - контактная информация
            try:
                contact_section = await self.page.query_selector('#contact')
                if contact_section:
                    # Телефон
                    phone_elem = await contact_section.query_selector('th:has-text("Téléphone") + td')
                    if phone_elem:
                        phone_text = await phone_elem.text_content()
                        # Извлекаем номер телефона
                        phone_match = re.search(r'[\d\s.]+', phone_text)
                        data['phone'] = phone_match.group(0).strip() if phone_match else ''
                    else:
                        data['phone'] = ''
                    
                    # Email
                    email_elem = await contact_section.query_selector('th:has-text("Email") + td')
                    if email_elem:
                        email_text = await email_elem.text_content()
                        data['email'] = email_text.strip() if email_text and 'Non disponible' not in email_text else ''
                    else:
                        data['email'] = ''
                    
                    # Сайт
                    website_elem = await contact_section.query_selector('th:has-text("Site internet") + td')
                    if website_elem:
                        website_text = await website_elem.text_content()
                        data['website'] = website_text.strip() if website_text and 'Non disponible' not in website_text else ''
                    else:
                        data['website'] = ''
                    
                    # Полный адрес
                    full_address_elem = await contact_section.query_selector('th:has-text("Adresse complète") + td')
                    if full_address_elem:
                        address_text = await full_address_elem.text_content()
                        data['full_address'] = address_text.strip()
                    else:
                        data['full_address'] = ''
                else:
                    data.update({'phone': '', 'email': '', 'website': '', 'full_address': ''})
            except Exception as e:
                logger.warning(f"Ошибка при сборе секции contact: {e}")
                data.update({'phone': '', 'email': '', 'website': '', 'full_address': ''})
            
            logger.info(f"Собрано данных для: {data.get('company_name', 'Unknown')}")
            return data
            
        except Exception as e:
            logger.error(f"Ошибка сбора данных: {e}")
            return None
    
    async def scrape_all_companies_manual(self, total_pages=20, output_file='pappers_companies.xlsx'):
        """Полный цикл с ручным переключением страниц: сбор ссылок и детальных данных"""
        
        await self.init_browser(headless=False)
        
        try:
            # Шаг 1: Собираем ссылки на компании с поисковых страниц (ручное переключение)
            logger.info(f"Начинаем ручной сбор ссылок с {total_pages} страниц")
            companies_list = await self.scrape_search_pages_manual(total_pages=total_pages)
            
            if not companies_list:
                logger.warning("Не найдено компаний на поисковых страницах")
                return []
            
            # Сохраняем список компаний
            df_list = pd.DataFrame(companies_list)
            temp_output = output_file.replace('.xlsx', '_list.xlsx')
            df_list.to_excel(temp_output, index=False)
            logger.info(f"Сохранен список компаний: {temp_output}")
            
            # Шаг 2: Собираем детальные данные по каждой компании
            all_data = []
            total = len(companies_list)
            
            for i, company in enumerate(companies_list):
                logger.info(f"[{i+1}/{total}] Обработка компании: {company.get('company_name', 'Unknown')}")
                
                try:
                    # Извлекаем детальные данные
                    detailed_data = await self.extract_company_data(company['url'])
                    
                    if detailed_data:
                        # Объединяем данные из поиска и детальные данные
                        merged_data = {**company, **detailed_data}
                        all_data.append(merged_data)
                    
                    # Сохраняем каждые 5 компаний
                    if (i + 1) % 5 == 0:
                        df_temp = pd.DataFrame(all_data)
                        df_temp.to_excel(output_file, index=False)
                        logger.info(f"Промежуточное сохранение: {output_file} ({len(all_data)} компаний)")
                    
                    await asyncio.sleep(3)  # Пауза между запросами
                    
                except Exception as e:
                    logger.error(f"Ошибка при обработке компании {company.get('company_name')}: {e}")
                    continue
            
            # Финальное сохранение
            if all_data:
                # Нормализуем данные для DataFrame
                flattened_data = []
                for item in all_data:
                    flat_item = item.copy()
                    
                    # Обрабатываем establishments (список словарей)
                    if 'establishments' in flat_item and isinstance(flat_item['establishments'], list):
                        if flat_item['establishments']:
                            # Берем первое заведение для основных полей
                            first_etab = flat_item['establishments'][0]
                            flat_item['etab_siret'] = first_etab.get('siret', '')
                            flat_item['etab_address'] = first_etab.get('address', '')
                            flat_item['etab_creation_date'] = first_etab.get('creation_date', '')
                        del flat_item['establishments']
                    
                    # Обрабатываем financials (словарь со списками)
                    if 'financials' in flat_item and isinstance(flat_item['financials'], dict):
                        for key, value in flat_item['financials'].items():
                            if isinstance(value, list) and len(value) > 0:
                                flat_item[f'fin_{key}_latest'] = value[0] if value else ''
                                if len(value) > 1:
                                    flat_item[f'fin_{key}_previous'] = value[1]
                        del flat_item['financials']
                    
                    flattened_data.append(flat_item)
                
                df_final = pd.DataFrame(flattened_data)
                df_final.to_excel(output_file, index=False)
                logger.info(f"Финальное сохранение: {output_file} ({len(all_data)} компаний)")
            
            return all_data
            
        except KeyboardInterrupt:
            logger.info("Прерывание пользователем...")
        finally:
            await self.browser.close()
            logger.info("Браузер закрыт")


async def main():
    scraper = PappersPlaywrightScraper()
    
    await scraper.scrape_all_companies_manual(
        total_pages=20,
        output_file='pappers_parfumerie_companies.xlsx'
    )


if __name__ == "__main__":
    asyncio.run(main())
