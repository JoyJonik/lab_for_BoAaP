#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для сбора данных о компаниях с сайта infogreffe.fr
Использует Playwright (более стабильный чем Selenium)
"""

import asyncio
import sys
import re
import pandas as pd
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
from datetime import datetime
import logging

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class InfogreffePlaywrightScraper:
    """Скрепер на базе Playwright"""
    
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
                "button:has-text('Accepter'), button:has-text('Tout accepter'), button:has-text('Accepter & Fermer')",
                timeout=5000
            )
            if cookie_button:
                await cookie_button.click()
                logger.info("Куки приняты")
                await asyncio.sleep(1)
        except:
            logger.debug("Окно с куки не найдено")
    
    async def search_company(self, company_name):
        """Поиск компании"""
        try:
            logger.info(f"Поиск: {company_name}")
            
            # Формируем URL поиска
            search_query = company_name.replace(' ', '%20')
            url = f"https://www.infogreffe.fr/recherche-entreprise-dirigeant/resultats-de-recherche?recherche=Entreprises&denomination={search_query}&dirigeantPage=0&dirigeantPageSize=10"
            
            await self.page.goto(url, wait_until='networkidle')
            await asyncio.sleep(2)
            
            # Закрываем куки
            await self.close_cookies()
            
            # Проверяем наличие результатов
            try:
                # Ищем ссылку на компанию
                company_link = await self.page.wait_for_selector(
                    f"a:has-text('{company_name}')",
                    timeout=10000
                )
                
                if company_link:
                    href = await company_link.get_attribute('href')
                    logger.info(f"Найдена: {href}")
                    return href
                    
            except PlaywrightTimeout:
                logger.warning(f"Не найдено: {company_name}")
                return None
                
        except Exception as e:
            logger.error(f"Ошибка поиска {company_name}: {e}")
            return None
    
    async def extract_company_data(self, url):
        """Извлечение данных компании"""
        try:
            logger.info(f"Сбор данных: {url}")
            
            await self.page.goto(url, wait_until='networkidle')
            await asyncio.sleep(2)
            
            data = {}
            
            # Название
            try:
                name_elem = await self.page.wait_for_selector('h1', timeout=5000)
                data['Название компании'] = await name_elem.text_content()
            except:
                data['Название компании'] = ''
            
            # SIREN
            try:
                siren_elem = await self.page.query_selector("text=SIREN : >> xpath=following-sibling::div")
                if siren_elem:
                    siren = await siren_elem.text_content()
                    data['SIREN'] = siren.replace(' ', '').strip() if siren else ''
                else:
                    data['SIREN'] = ''
            except:
                data['SIREN'] = ''
            
            # Форма юридическая
            try:
                forme_elem = await self.page.query_selector("text=Forme juridique : >> xpath=following-sibling::div")
                if forme_elem:
                    data['Форма юридическая'] = await forme_elem.text_content()
                else:
                    data['Форма юридическая'] = ''
            except:
                data['Форма юридическая'] = ''
            
            # Год основания
            try:
                page_text = await self.page.content()
                year_match = re.search(r'Première immatriculation le\s*.*?/(\d{4})', page_text)
                if year_match:
                    data['Год основания'] = year_match.group(1)
                else:
                    data['Год основания'] = ''
            except:
                data['Год основания'] = ''
            
            # Адрес
            try:
                adresse_elem = await self.page.query_selector("text=Siège social : >> xpath=following-sibling::div")
                if adresse_elem:
                    data['Адрес'] = await adresse_elem.text_content()
                else:
                    data['Адрес'] = ''
            except:
                data['Адрес'] = ''
            
            # Владелец/Президент
            try:
                # Ищем в секции директоров
                president_elem = await self.page.query_selector("text=Président >> xpath=following-sibling::div")
                if president_elem:
                    data['Владелец/Президент'] = await president_elem.text_content()
                else:
                    data['Владелец/Президент'] = ''
            except:
                data['Владелец/Президент'] = ''
            
            # Директора
            try:
                directeurs = []
                # Ищем всех директоров
                dg_elems = await self.page.query_selector_all("text=Directeur général >> xpath=following-sibling::div")
                for elem in dg_elems[:3]:  # Берем первых 3
                    text = await elem.text_content()
                    if text:
                        directeurs.append(text.strip())
                data['Директора'] = ', '.join(directeurs) if directeurs else ''
            except:
                data['Директора'] = ''
            
            # Код NAF
            try:
                naf_elem = await self.page.query_selector("text=Activité (code NAF) : >> xpath=following-sibling::div")
                if naf_elem:
                    naf_text = await naf_elem.text_content()
                    data['Код NAF'] = naf_text.split(':')[0].strip() if ':' in naf_text else naf_text
                else:
                    data['Код NAF'] = ''
            except:
                data['Код NAF'] = ''
            
            # Количество заведений
            try:
                etab_link = await self.page.query_selector("a[href*='etablissements']")
                if etab_link:
                    etab_text = await etab_link.text_content()
                    etab_match = re.search(r'(\d+)', etab_text)
                    data['Количество заведений'] = etab_match.group(1) if etab_match else ''
                else:
                    data['Количество заведений'] = ''
            except:
                data['Количество заведений'] = ''
            
            # Статус
            try:
                page_text = await self.page.content()
                if 'Radiée' in page_text or 'Fermé' in page_text:
                    close_match = re.search(r'Radiée le\s*:\s*(\d{2})\s*/\s*(\d{2})\s*/\s*(\d{4})', page_text)
                    if close_match:
                        data['Статус'] = f"ЗАКРЫТА ({close_match.group(1)}.{close_match.group(2)}.{close_match.group(3)})"
                    else:
                        data['Статус'] = 'ЗАКРЫТА'
                else:
                    data['Статус'] = 'Активна'
            except:
                data['Статус'] = 'Активна'
            
            # Сайт
            try:
                site_link = await self.page.query_selector("a[href^='http']:not([href*='infogreffe'])")
                if site_link:
                    data['Сайт'] = await site_link.text_content()
                else:
                    data['Сайт'] = ''
            except:
                data['Сайт'] = ''
            
            # Финансовые данные из таблицы
            try:
                # Ищем таблицу с финансовыми данными
                rows = await self.page.query_selector_all('table tr')
                for row in rows:
                    cells = await row.query_selector_all('td')
                    if len(cells) >= 4:
                        year = await cells[0].text_content()
                        ca = await cells[1].text_content()
                        result = await cells[2].text_content()
                        effectif = await cells[3].text_content()
                        
                        if '2024' in year:
                            data['Оборот 2024'] = ca.strip()
                            data['Результат 2024'] = result.strip()
                            data['Количество сотрудников'] = effectif.strip()
                        elif '2023' in year:
                            data['Оборот 2023'] = ca.strip()
                            data['Результат 2023'] = result.strip()
                        elif '2022' in year:
                            data['Оборот 2022'] = ca.strip()
                            data['Результат 2022'] = result.strip()
            except:
                pass
            
            logger.info(f"Собрано: {data.get('Название компании', 'Unknown')}")
            return data
            
        except Exception as e:
            logger.error(f"Ошибка сбора данных: {e}")
            return None
    
    async def process_excel(self, input_file, output_file=None, start=0, end=None):
        """Обработка Excel файла"""
        
        df = pd.read_excel(input_file)
        logger.info(f"Загружено {len(df)} компаний")
        
        if end is None:
            end = len(df)
        
        await self.init_browser(headless=False)
        
        try:
            for i in range(start, end):
                row = df.iloc[i]
                company_name = row['Название компании']
                
                logger.info(f"[{i+1}/{end}] {company_name}")
                
                # Поиск
                url = await self.search_company(company_name)
                
                if url:
                    # Сбор данных
                    data = await self.extract_company_data(url)
                    
                    if data:
                        # Обновляем DataFrame
                        for key, value in data.items():
                            if key in df.columns and value:
                                # Преобразуем значение в строку для избежания проблем с типами данных
                                df.at[i, key] = str(value)
                        
                        self.results.append(data)
                
                # Сохраняем каждые 5 компаний
                if (i + 1) % 5 == 0:
                    out_path = output_file or input_file.replace('.xlsx', '_playwright.xlsx')
                    df.to_excel(out_path, index=False)
                    logger.info(f"Сохранено: {out_path}")
                
                await asyncio.sleep(2)
        
        except KeyboardInterrupt:
            logger.info("Прерывание...")
        
        finally:
            # Финальное сохранение
            out_path = output_file or input_file.replace('.xlsx', '_playwright.xlsx')
            df.to_excel(out_path, index=False)
            
            await self.browser.close()
            logger.info(f"Готово! Обработано {len(self.results)} компаний")
    
    async def close(self):
        """Закрытие браузера"""
        if self.browser:
            await self.browser.close()


async def main():
    scraper = InfogreffePlaywrightScraper()
    
    await scraper.process_excel(
        input_file=r"C:\Users\wmath\Downloads\Kimi_Agent_Скрапинг\parfumerie_300plus_companies_updated.xlsx",
        output_file=r"C:\Users\wmath\Downloads\Kimi_Agent_Скрапинг\parfumerie_300plus_companies_updated.xlsx",
        start=0,
        end=None  # Все компании
    )


if __name__ == "__main__":
    asyncio.run(main())
