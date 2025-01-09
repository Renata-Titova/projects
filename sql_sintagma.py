# функция для чтения сег файлов
import os
from itertools import product
import sqlite3
import pandas as pd


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
files_seg_Y1 = []
files_seg_R2 = []
for root, dirs, files in os.walk(folder_path):
    for file in files:
        if file.endswith("seg_Y1"):
            files_seg_Y1.append(os.path.join(root, file))
        if file.endswith("seg_R2"):
            files_seg_R2.append(os.path.join(root, file))
            #  Удаление расширения .sbl

# Сортируем списки файлов
files_seg_Y1.sort()
files_seg_R2.sort()
# print(files_seg_Y1)
# print(files_seg_R2)

for filename_r2, filename_y1 in zip(
    files_seg_R2, files_seg_Y1
):  # Проходимся по парам файлов
    base_name_r2 = os.path.basename(filename_r2)  # Получаем имя файла
    file_name_without_extension_r2 = os.path.splitext(base_name_r2)[
        0
    ]  # Убираем расширение

    # Извлекаем название из имени файла
    desired_name_r2 = file_name_without_extension_r2.split(".")[0].split("_")[0]
    desired_name_r2 = int(desired_name_r2[3:]) * 111111
    # Читаем .seg файл
    params, sintagmas = read_seg(filename_r2)
    params, words = read_seg(filename_y1)
    # print(words)

    # Создаем словари для каждого слова
    for left, right in zip(sintagmas, sintagmas[1:]):
        sintamas_name = left["name"]
        start_time = left["position"] / params["SAMPLING_FREQ"]
        end_time = right["position"] / params["SAMPLING_FREQ"]
        # Извлекая слова, которые соответствуют синтагме
        corresponding_words = [
            word["name"]
            for word in words
            if start_time <= word["position"] / params["SAMPLING_FREQ"] < end_time
        ]
        result_string = " ".join(corresponding_words)
        # print(result_string)

        # Формируем результатирующий словарь
        words_dict = {
            "filename": desired_name_r2,
            "from": start_time,
            "to": end_time,
            "pitch_pattern": sintamas_name,
            "words_in_sintagma": result_string,
        }
        result.append(words_dict)

# Вывод результата
# print(result[0:5])

# 1.  Создание соединения с базой данных
conn = sqlite3.connect("my_data.db")
cursor = conn.cursor()

# 2.  Создание таблицы Words
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS Sintagmas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        filename INTEGER,
        unit TEXT,
        words_in_sintagmas TEXT,
        from_time REAL,
        to_time REAL,
        CONSTRAINT FK_Sintagmas_filenames FOREIGN KEY (filename) REFERENCES filenames(filename) 
    )
    """
)

# 3.  Вставка данных в таблицу Words
for i, words_dict in enumerate(result):  # Используем enumerate для получения индекса после i название словаря, (название списка большого)
    filename = words_dict["filename"]
    unit = words_dict["pitch_pattern"]
    words_in_sintagmas = words_dict["words_in_sintagma"]
    from_time = words_dict["from"]
    to_time = words_dict["to"]
    # Форматируем ID
    formatted_id = i + 4001  # с 4000
    cursor.execute(
        "INSERT INTO Sintagmas (id, filename, unit, words_in_sintagmas, from_time, to_time) VALUES (?, ?, ?, ?, ?, ?)",
        (formatted_id, filename, unit, words_in_sintagmas, from_time, to_time),
    )

conn.commit()

# 4.  Вывод таблицы Words
cursor.execute("SELECT * FROM Sintagmas")
rows = cursor.fetchall()
print("Таблица Sintagmas:")
for row in rows:
    print(row)

# 5.  Сохранение таблицы Words в CSV-файл
df = pd.DataFrame(rows, columns=["id", "filename", "unit", "words_in_sintagmas", "from_time", "to_time"])
df.to_csv(os.path.join(folder_path, "sintagmas.csv"), index=False, sep=",")

# 6.  Закрытие соединения с базой данных
conn.close()
