import os
import sqlite3
import pandas as pd
import numpy as np

# Читаем seg
from itertools import product

letters = "GBRY"
nums = "1234"
levels = [ch + num for num, ch in product(nums, letters)]
level_codes = [2**i for i in range(len(levels))]
code_to_level = {i: j for i, j in zip(level_codes, levels)}
level_to_code = {j: i for i, j in zip(level_codes, levels)}


def read_seg(filename: str, encoding: str = "windows-1251") -> tuple[dict, list[dict]]:
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

folder_path = r"c:\Users\hp\Downloads\ata"
result = []
# Обходим все директории и поддиректории
files_seg_G1 = []
for root, dirs, files in os.walk(folder_path):
    for file in files:
        if file.endswith("seg_G1"):
            files_seg_G1.append(os.path.join(root, file))
            #  Удаление расширения .sbl

# Сортируем списки файлов
files_seg_G1.sort()
# print(files_seg_G1)

min_f0 = 50.0

for filename in files_seg_G1:
    base_name = os.path.basename(filename)  # Получаем имя файла
    file_name_without_extension = os.path.splitext(base_name)[0]  # Убираем расширение
    # Извлекаем название из имени файла
    desired_name = file_name_without_extension.split(".")[0].split("_")[0]
    desired_name = int(desired_name[3:]) * 111111

    # Читаем .seg файл
    params, labels = read_seg(filename)
    # print(labels)
    # убрать из него метки начала и конца файла
    labels = labels[1:-1]

    for left, right in zip(labels, labels[1:]):
        start_time = left["position"] / params["SAMPLING_FREQ"]
        end_time = right["position"] / params["SAMPLING_FREQ"]
        duration_T = end_time - start_time  # получили время
        f0 = 1 / duration_T

        values_f0 = {
            "filename": desired_name,
            "all_values_f0": f0 if f0 >= min_f0 and left["name"] != "0" else np.nan,
            "from": start_time,
            "to": end_time,
        }
        result.append(values_f0)
# print(result[0:5])

# 1.  Создание соединения с базой данных
conn = sqlite3.connect("my_data.db")
cursor = conn.cursor()

# 2.  Создание таблицы ideal_transcription
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS F0 (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        filename INTEGER,
        unit TEXT,
        from_time REAL,
        to_time REAL,
        CONSTRAINT FK_F0_Real_transcription FOREIGN KEY (filename) REFERENCES Real_transcription(filename)
        CONSTRAINT FK_F0_Ideal_transcription FOREIGN KEY (filename) REFERENCES Ideal_transcription(filename)
        CONSTRAINT FK_F0_Words FOREIGN KEY (filename) REFERENCES Words(filename)
        CONSTRAINT FK_F0_Sintagmas FOREIGN KEY (filename) REFERENCES Sintagmas(filename)
        CONSTRAINT FK_F0_filenames FOREIGN KEY (filename) REFERENCES filenames(filename)

    )
    """
)
# 3.  Вставка данных в таблицу
for i, values_f0 in enumerate(result):  # Используем enumerate для получения индекса
    filename = values_f0["filename"]
    unit = values_f0["all_values_f0"]
    from_time = values_f0["from"]
    to_time = values_f0["to"]
    formatted_id = i + 5001  # со 5001
    cursor.execute(
        "INSERT INTO F0 (id, filename, unit, from_time, to_time) VALUES (?, ?, ?, ?, ?)",
        (formatted_id, filename, unit, from_time, to_time),
    )

conn.commit()

# 4.  Вывод таблицы Words
cursor.execute("SELECT * FROM F0")
rows = cursor.fetchall()
print("Таблица f0:")
for row in rows:
    print(row)

# 5.  Сохранение таблицы Words в CSV-файл
df_f0 = pd.DataFrame(result)
df_f0.to_csv(os.path.join(folder_path, "f0.csv"), index=False, sep=",")

# 6.  Закрытие соединения с базой данных
conn.close()
