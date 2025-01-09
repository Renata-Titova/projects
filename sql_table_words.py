import os
import sqlite3
import pandas as pd
# Читаем seg
from itertools import product

letters = "GBRY"
nums = "1234"
levels = [ch + num for num, ch in product(nums, letters)]
level_codes = [2**i for i in range(len(levels))]
code_to_level = {i: j for i, j in zip(level_codes, levels)}
level_to_code = {j: i for i, j in zip(level_codes, levels)}


def read_seg(
    filename: str, encoding: str = "windows-1251"
) -> tuple[dict, list[dict]]:
    with open(filename, encoding=encoding) as f:
        lines = [line.strip() for line in f.readlines()]

    # найдём границы секций в списке строк:
    header_start = lines.index("[PARAMETERS]") + 1
    data_start = lines.index("[LABELS]") + 1

    # прочитаем параметры
    params = {}
    for line in lines[header_start : data_start - 1]:
        key, value = line.split("=")
        params[key] = int(value)

    # прочитаем метки
    labels = []
    for line in lines[data_start:]:
        # если в строке нет запятых, значит, это не метка и метки закончились
        if line.count(",") < 2:
            break
        pos, level, name = line.split(",", maxsplit=2)
        label = {
            "position": int(pos) // params["BYTE_PER_SAMPLE"] // params["N_CHANNEL"],
            "level": code_to_level[int(level)],
            "name": name,
        }
        labels.append(label)
    return params, labels


# Проверим на 1 файле read-seg
# path = r"c:\Users\hp\Downloads\ata\ata0001-0010\ata0003.seg_B1"
# params, labels = read_seg(path)
# print(params, labels)


folder_path = r"c:\Users\hp\Downloads\ata" 
words = []
# Обходим все директории и поддиректории
files_seg_Y1 = []
for root, dirs, files in os.walk(folder_path):
    for file in files:
        if file.endswith("seg_Y1"):
            files_seg_Y1.append(os.path.join(root, file))
            #  Удаление расширения .sbl

# Сортируем списки файлов
files_seg_Y1.sort()
# print(files_seg_Y1)

for filename in files_seg_Y1:
    base_name = os.path.basename(filename)  # Получаем имя файла
    file_name_without_extension = os.path.splitext(base_name)[0]  # Убираем расширение
    # Извлекаем название из имени файла
    desired_name = file_name_without_extension.split('.')[0].split('_')[0]
    desired_name = int(desired_name[3:]) * 111111
    # desired_name = file_name_without_extension.split('_')[0][3:] # "0001"
    # filename_id = int(desired_name) * 111111 # 111111

    # Читаем .seg файл
    params, labels = read_seg(filename)
    # print(labels)

    for left, right in zip(labels, labels[1:]):

        words_name = left["name"]
        start_time = left["position"] / params["SAMPLING_FREQ"]
        end_time = right["position"] / params["SAMPLING_FREQ"]
        # words_duration = end_time - start_time #получили время
        word = {
            "filename": desired_name,
            "word": words_name,
            "from": start_time,
            "to": end_time,
        }
        words.append(word)
# print(words[0:5])


# 1.  Создание соединения с базой данных
conn = sqlite3.connect("my_data.db")
cursor = conn.cursor()

# 2.  Создание таблицы Words
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS Words (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        filename INTEGER,
        unit TEXT,
        from_time REAL,
        to_time REAL,
        CONSTRAINT FK_Words_Sintagmas FOREIGN KEY (filename) REFERENCES Sintagmas(filename)
        CONSTRAINT FK_Words_filenames FOREIGN KEY (filename) REFERENCES filenames(filename)
    )
    """
)

# 3.  Вставка данных в таблицу Words
for i, word in enumerate(words):  # Используем enumerate для получения индекса
    filename = word["filename"]
    unit = word["word"]
    from_time = word["from"]
    to_time = word["to"]
    # Форматируем ID в виде "01", "02", "03"
    formatted_id = i + 101  # со 100
    cursor.execute(
        "INSERT INTO Words (id, filename, unit, from_time, to_time) VALUES (?, ?, ?, ?, ?)",
        (formatted_id, filename, unit, from_time, to_time),
    )

conn.commit()

# 4.  Вывод таблицы Words
cursor.execute("SELECT * FROM Words")
rows = cursor.fetchall()
print("Таблица Words:")
for row in rows:
    print(row)

# 5.  Сохранение таблицы Words в CSV-файл
df = pd.DataFrame(
    rows, columns=["id", "filename", "unit", "from_time", "to_time"]
)
df.to_csv(os.path.join(folder_path, "words.csv"), index=False, sep=",")

# 6.  Закрытие соединения с базой данных
conn.close()
