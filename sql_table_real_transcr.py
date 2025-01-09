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
files_seg_B1 = []
for root, dirs, files in os.walk(folder_path):
    for file in files:
        if file.endswith("seg_Y1"):
            files_seg_Y1.append(os.path.join(root, file))
        if file.endswith("seg_B1"):
            files_seg_B1.append(os.path.join(root, file))
            #  Удаление расширения .sbl

# Сортируем списки файлов
files_seg_Y1.sort()
files_seg_B1.sort()
# print(files_seg_Y1)
# print(files_seg_B1)

for filename_y1, filename_b1 in zip(
    files_seg_Y1, files_seg_B1
):  # Проходимся по парам файлов
    base_name_y1 = os.path.basename(filename_y1)  # Получаем имя файла
    file_name_without_extension_y1 = os.path.splitext(base_name_y1)[
        0
    ]  # Убираем расширение
    # Извлекаем название из имени файла
    desired_name_y1 = file_name_without_extension_y1.split(".")[0].split("_")[0]
    desired_name_y1 = int(desired_name_y1[3:]) * 111111

    # Читаем .seg файл
    params, words = read_seg(filename_y1)
    params, sounds = read_seg(filename_b1)
    # print(sounds)

    # Создаем словари для каждого слова
    for left, right in zip(words, words[1:]):
        start_time = left["position"] / params["SAMPLING_FREQ"]
        end_time = right["position"] / params["SAMPLING_FREQ"]
        # Извлекая звуки, которые соответствуют слову
        corresponding_sounds = [sound['name'] for sound in sounds if start_time <= sound['position'] / params["SAMPLING_FREQ"] < end_time]
        result_string = ''.join(corresponding_sounds)
        # print(result_string)
        # Формируем результатирующий словарь
        sound_dict = {
            "filename": desired_name_y1,
            "from": start_time,
            "to": end_time,
            "real_transcription": result_string,
        }
        result.append(sound_dict)

# Вывод результата
# print(result[0:5])


# 1.  Создание соединения с базой данных
conn = sqlite3.connect("my_data.db")
cursor = conn.cursor()

# 2.  Создание таблицы Words
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS Real_transcription (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        filename INTEGER,
        unit TEXT,
        from_time REAL,
        to_time REAL,
        CONSTRAINT FK_Real_transcription_Ideal_transcription FOREIGN KEY (filename) REFERENCES Ideal_transcription(filename)
        CONSTRAINT FK_Real_transcription_Words FOREIGN KEY (filename) REFERENCES Words(filename)
        CONSTRAINT FK_Real_transcription_Sintagmas FOREIGN KEY (filename) REFERENCES Sintagmas(filename)
        CONSTRAINT FK_Real_transcription_filenames FOREIGN KEY (filename) REFERENCES filenames(filename)
        
    )
    """
)
# 3.  Вставка данных в таблицу
for i, sound_dict in enumerate(result):  # Используем enumerate для получения индекса
    filename = sound_dict["filename"]
    unit = sound_dict["real_transcription"]
    from_time = sound_dict["from"]
    to_time = sound_dict["to"]
    formatted_id = i + 2001  # со 2001
    cursor.execute(
        "INSERT INTO Real_transcription (id, filename, unit, from_time, to_time) VALUES (?, ?, ?, ?, ?)",
        (formatted_id, filename, unit, from_time, to_time),
    )

conn.commit()

# 4.  Вывод таблицы Words
cursor.execute("SELECT * FROM Real_transcription")
rows = cursor.fetchall()
print("Таблица Words:")
for row in rows:
    print(row)

# 5.  Сохранение таблицы Words в CSV-файл
df = pd.DataFrame(
    rows, columns=["id", "filename", "unit", "from_time", "to_time"]
)
df.to_csv(os.path.join(folder_path, "real_transcription.csv"), index=False, sep=",")

# 6.  Закрытие соединения с базой данных
conn.close()
