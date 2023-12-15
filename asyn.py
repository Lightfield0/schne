import aiohttp
import asyncio
from bs4 import BeautifulSoup
import pandas as pd
import json, time
from rich.progress import Progress

def extract_json_data(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    json_element = soup.find('pes-description-and-specifications')
    if json_element:
        json_data = json_element['plain-characteristic-tables']
        return json.loads(json_data)
    return None

def parse_technical_details(ürün_ismi, ürün_linki, stok_kodu, json_data):
    technical_details = []
    for table in json_data:
        table_name = table['tableName']
        for row in table['rows']:
            characteristic_name = row['characteristicName']
            for value in row['characteristicValues']:
                feature = value['labelText']
                technical_details.append({'Ürün İsmi': ürün_ismi, 'Ürün Linki': ürün_linki, 'Stok Kodu': stok_kodu, 'Filtre': characteristic_name, 'Özellik': feature})
    return technical_details


def get_details(soup):
    product_main = soup.find('pes-product-main')
    if not product_main:
        # print("Ürün ana elementi bulunamadı.")
        return None, None, None

    product_id = product_main.get('plain-product-id')
    product_media_json = product_main.get('plain-product-media')
    product_cta_area_json = product_main.get('plain-cta-area')

    product_media = json.loads(product_media_json.replace('&quot;', '"'))
    product_cta_area = json.loads(product_cta_area_json.replace('&quot;', '"'))

    product_images = product_media.get('zoomPictureDesktop', {}).get('url')
    product_price = product_cta_area.get('pdsPrice')

    return product_id, product_images, product_price

def extract_text(item):
    if item:
        return item.get_text(strip=True)
    return ""


def fetch_breadcrumbs(soup):
    breadcrumbs_element = soup.find('pes-breadcrumbs')
    if breadcrumbs_element:
        breadcrumbs_json = breadcrumbs_element.get('plain-breadcrumbs', '[]')
        breadcrumbs_data = json.loads(breadcrumbs_json.replace('&quot;', '"'))
        return [extract_text(BeautifulSoup(breadcrumb['name'], 'html.parser')) for breadcrumb in breadcrumbs_data]
    return []

async def fetch(session, url, retries=3, delay=1):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
    for i in range(retries):
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    return await response.text()
                elif response.status == 403:
                    # print(f"Erişim reddedildi: {url}")
                    return None
                else:
                    # print(f"Başarısız istek: {url} - Durum Kodu: {response.status}")
                    return None
        except aiohttp.ClientError as e:
            if i < retries - 1:
                await asyncio.sleep(delay)
                continue
            else:
                # print(f"İstek başarısız: {url} - Hata: {e}")
                return None

    

async def fetch_product_details(session, ürün_kodu, progress_task, progress):
    url = f'https://www.se.com/tr/tr/product/{ürün_kodu}'
    print(url)
    try:
        response_text = await fetch(session, url)
        soup = BeautifulSoup(response_text, 'html.parser')

        product_id, product_images, product_price = get_details(soup)    

        ürün_ismi = soup.title.string if soup.title else ''
        if ürün_ismi == 'Access Denied':
            print(ürün_ismi)
        ürün_fiyatı = product_price if product_price else ''
        stok_kodu = product_id if product_id else ''

        breadcrumbs = fetch_breadcrumbs(soup)
        # print("Breadcrumb:", " > ".join(breadcrumbs))
        kategoriler = " > ".join(breadcrumbs)

        # Ürün açıklamasını ve diğer detayları al
        product_details = soup.find('pes-description-and-specifications')
        ürün_açıklaması = product_details.get('plain-long-desc-sentences', '') if product_details else ''

        teknik_detaylar = []
        json_data = extract_json_data(soup.prettify())
        if json_data:
            teknik_detaylar = parse_technical_details(ürün_kodu, url, stok_kodu, json_data)
            # teknik_özellikler.extend(teknik_detaylar)

        ürün_fotoları = [product_images] if product_images else []


        ürün = {
            "ÜRÜN İSMİ": ürün_ismi,
            "ÜRÜN FİYATI": ürün_fiyatı,
            "STOK KODU": stok_kodu,
            "Kategoriler": kategoriler,
            "URUN ACİKLAMASİ": ürün_açıklaması,
            "FOTOĞRAF LİNKLERİ": ', '.join(ürün_fotoları)
        }

        # print('ÜRÜN: ', ürün)
        # time.sleep(0.1)
        progress.update(progress_task, advance=1)  # İlerlemeyi güncelle
        return ürün, teknik_detaylar
    except Exception as e:
        # print(f"İstek başarısız oldu: {e}")
        return None  # veya uygun bir hata mesajı döndürün


timeout = aiohttp.ClientTimeout(total=40)
async def main(ürün_kodlari):
    async with aiohttp.ClientSession() as session:
        with Progress() as progress:
            progress_task = progress.add_task("[green]Ürünler İşleniyor...", total=len(ürün_kodlari))
            tasks = []
            for ürün_kodu in ürün_kodlari:
                await asyncio.sleep(0.1)  # Her istek arasında 0.5 saniye bekle
                task = asyncio.create_task(fetch_product_details(session, ürün_kodu, progress_task, progress))
                tasks.append(task)
            ürünler = await asyncio.gather(*tasks)
            return ürünler

# DataFrame oluşturma ve kaydetme
async def process_data():
    df = pd.read_excel('STANDART TEKLİF FORMATI-V7 FINAL.xlsx')
    ürün_kodlari = df['Referans'].tolist()
    ürünler = await main(ürün_kodlari)

    # Sadece 'None' olmayan ürünler için DataFrame oluştur
    df_ürünler = pd.DataFrame([ürün[0] for ürün in ürünler if ürün])
    df_ürünler.to_excel('SCHNEIDER_Ürünler.xlsx', index=False)

    # Teknik detaylar için de benzer bir kontrol yapılmalı
    df_teknik = pd.DataFrame([teknik for ürün in ürünler if ürün for teknik in ürün[1]])
    df_teknik.to_excel('SCHNEIDER_Teknik_Özellikler.xlsx', index=False)

asyncio.run(process_data())
