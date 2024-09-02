import csv
import glob
import os

# Create a new directory for aligned CSV files
aligned_csv_dir = "results/aligned-csv"
os.makedirs(aligned_csv_dir, exist_ok=True)

# 读取所有CSV文件
csv_files = glob.glob("results/AIDungeon_*.csv")

# 收集所有字段
all_fieldnames = set()
for file in csv_files:
    with open(file, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        all_fieldnames.update(reader.fieldnames)

# 确保字段顺序一致
all_fieldnames = sorted(all_fieldnames)

# 重新写入CSV文件
for file in csv_files:
    rows = []
    with open(file, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            rows.append(row)
    
    aligned_file = os.path.join(aligned_csv_dir, os.path.basename(file))
    with open(aligned_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=all_fieldnames)
        writer.writeheader()
        for row in rows:
            # 确保每一行都有所有字段
            complete_row = {field: row.get(field, '') for field in all_fieldnames}
            writer.writerow(complete_row)

print("CSV files have been realigned and saved to 'results/aligned-csv'.")
