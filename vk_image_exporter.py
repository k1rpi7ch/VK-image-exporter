import os
import re
import sys
import argparse
import logging
from bs4 import BeautifulSoup
from pathlib import Path
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from PIL import Image
from io import BytesIO
import piexif

logging.basicConfig(
    filename='errors.log',
    filemode='a',
    level=logging.ERROR,
    format='%(asctime)s - %(message)s'
)

HTML_FILE_PATTERN = re.compile(r'^messages\d*\.html$')
IMAGE_LINK_PATTERN = re.compile(r'https://sun9-\d+\.userapi\.com/[^"]+')

def parse_args():
    parser = argparse.ArgumentParser(description="Загрузка изображений из HTML-файлов VK")
    parser.add_argument('-s', '--source', help='Директория с HTML-файлами', required=True)
    parser.add_argument('-d', '--destination', help='Директория для сохранения изображений', required=True)
    return parser.parse_args()

def read_file_with_fallback(filepath):
    for enc in ['cp1251', 'utf-8']:
        try:
            with open(filepath, 'r', encoding=enc) as f:
                return f.read()
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError(f"Не удалось прочитать файл {filepath} в известных кодировках.")

def extract_links_and_dates(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    results = []

    for item in soup.find_all('div', class_='item'):
        header = item.select_one('.message__header')
        if not header:
            continue
        date_text = header.get_text().strip().split(',')[-1].strip()

        for a_tag in item.select('a.attachment__link'):
            href = a_tag.get('href')
            if href and IMAGE_LINK_PATTERN.match(href):
                results.append((href, date_text))
    return results

def convert_date_format(date_str):
    months = {
        'янв': '01', 'фев': '02', 'мар': '03', 'апр': '04',
        'май': '05', 'мая': '05', 'июн': '06', 'июл': '07',
        'авг': '08', 'сен': '09', 'окт': '10', 'ноя': '11', 'дек': '12'
    }

    try:
        parts = date_str.strip().split(' в ')
        if len(parts) != 2:
            return "2000:01:01 00:00:00"
        date_part, time_part = parts
        day, month_rus, year = date_part.split()
        month = months.get(month_rus.lower())
        if not month:
            return "2000:01:01 00:00:00"
        formatted = f"{year}:{month}:{day.zfill(2)} {time_part}"
        return formatted
    except Exception as e:
        logging.error(f"Ошибка при разборе даты '{date_str}': {e}")
        return "2000:01:01 00:00:00"

def sanitize_date(date_str):
    return date_str.replace(":", "-").replace(" ", "_").replace("в", "").strip()

def set_exif_date(image_data, date_str):
    try:
        image = Image.open(BytesIO(image_data))
        if image.format != 'JPEG':
            return None

        dt = convert_date_format(date_str)
        exif_dict = {"0th": {}, "Exif": {}, "GPS": {}, "1st": {}, "thumbnail": None}
        exif_dict["Exif"][piexif.ExifIFD.DateTimeOriginal] = dt.encode('utf-8')
        exif_dict["0th"][piexif.ImageIFD.DateTime] = dt.encode('utf-8')
        exif_bytes = piexif.dump(exif_dict)

        output = BytesIO()
        image.save(output, format='JPEG', exif=exif_bytes)
        return output.getvalue()
    except Exception as e:
        logging.error(f"piexif ошибка EXIF: {e}")
        return None

def download_image(index, url, date_str, dest_dir):
    try:
        filename = f"{index:04d}"
        extension = os.path.splitext(urlparse(url).path)[-1].lower()

        if extension not in ['.jpg', '.jpeg', '.png']:
            extension = '.jpg'

        filepath = os.path.join(dest_dir, filename + extension)
        if os.path.exists(filepath):
            logging.error(f"Файл уже существует: {filepath}")
            return

        response = requests.get(url, timeout=10)
        response.raise_for_status()

        image_data = response.content

        if extension in ['.jpg', '.jpeg']:
            updated_data = set_exif_date(image_data, date_str)
            if updated_data:
                image_data = updated_data
        else:
            filepath = os.path.join(dest_dir, f"{filename}_{sanitize_date(date_str)}{extension}")

        with open(filepath, 'wb') as f:
            f.write(image_data)

    except Exception as e:
        logging.error(f"{url} - ошибка: {e}")

def main():
    args = parse_args()

    if not args.source or not args.destination:
        print("Использование: python vk_exporter.py -s <директория чата> -d <директория для сохранения фото>")
        sys.exit(1)

    source_dir = Path(args.source)
    dest_dir = Path(args.destination)

    if not source_dir.exists():
        print(f"Ошибка: директория источника '{source_dir}' не существует.")
        sys.exit(1)

    html_files = [f for f in source_dir.iterdir() if HTML_FILE_PATTERN.match(f.name)]
    if not html_files:
        print(f"Нет файлов формата messages*.html в '{source_dir}'.")
        sys.exit(1)

    dest_dir.mkdir(parents=True, exist_ok=True)

    all_links = []
    for html_file in html_files:
        try:
            content = read_file_with_fallback(html_file)
            all_links.extend(extract_links_and_dates(content))
        except Exception as e:
            logging.error(f"Ошибка при чтении {html_file}: {e}")

    print(f"Найдено {len(all_links)} ссылок на изображения. Начинаем загрузку...")

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(download_image, i + 1, url, date, str(dest_dir))
            for i, (url, date) in enumerate(all_links)
        ]
        for future in as_completed(futures):
            future.result()

    print("Загрузка завершена.")

if __name__ == '__main__':
    main()
