import pandas as pd
import os
import sqlite3

# 1. Создание соединения с SQL-базой данных
conn = sqlite3.connect("my_data.db")
cursor = conn.cursor()

# 2. Создание таблицы filenames
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS filenames (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        filename INTEGER
    )
"""
)

conn.commit()

# 3.  Путь к папке с файлами
data_dir = r"c:\Users\hp\Downloads\ata"

# 4. Обходим все директории и поддиректории
file_id = 1  # Счетчик для ID файлов
for root, dirs, files in os.walk(data_dir):
    for filename in files:
        if filename.endswith(".sbl"):
            #  Удаление расширения .sbl
            filename_without_ext = filename[:-4]
            filename_without_ext = int(filename_without_ext[3:]) * 111111

            #  Вставка данных в таблицу (с ID)
            cursor.execute(
                "REPLACE INTO filenames (id, filename) VALUES (?, ?)",
                (
                    file_id,
                    filename_without_ext,
                ),
            )
            file_id += 1  # Увеличение счетчика ID

conn.commit()

# 5.  Вывод таблицы
cursor.execute("SELECT * FROM filenames")
rows = cursor.fetchall()
print("Таблица filenames:")
for row in rows:
    print(row)

# 6.  Сохранение таблицы в CSV-файл
df = pd.DataFrame(rows, columns=["id", "filename"])
df.to_csv(os.path.join(data_dir, "filenames.csv"), index=False, sep=",")


# 7.  Закрытие соединения с базой данных
conn.close()
