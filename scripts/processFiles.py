import sys
import os
from typing import Iterable
import csv
import json
import datetime
from tqdm import tqdm

from fileStreams import getFileJsonStream
from utils import FileProgressLog

# Check Python version
if sys.version_info < (3, 10):
    raise RuntimeError("This script requires Python 3.10 or higher")

fileOrFolderPath = 'E:/reddit/submissions/'
recursive = False

def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            items.append((new_key, json.dumps(v)))
        else:
            items.append((new_key, v))
    return dict(items)

def processFile(path: str):
    print(f"Processing file {path}")
    
    # Extract the month and year from the input file name
    file_name = os.path.basename(path)
    month_year = file_name.split('_')[1].split('.')[0]  # Extracts "2024-04" from "RS_2024-04.zst"
    output_file = os.path.join("results", f"AIDungeon_data_{month_year}.csv")
    
    # Ensure the results directory exists
    os.makedirs("results", exist_ok=True)
    
    with open(path, "rb") as f, open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        jsonStream = getFileJsonStream(path, f)
        if jsonStream is None:
            print(f"Skipping unknown file {path}")
            return
        
        # Get total file size for progress bar
        f.seek(0, 2)
        file_size = f.tell()
        f.seek(0)
        
        progressLog = FileProgressLog(path, f)
        
        fieldnames = []
        csv_writer = None
        processed_rows = 0

        with tqdm(total=file_size, unit='B', unit_scale=True, desc="Processing") as pbar:
            pbar.update(f.tell())
            for row in jsonStream:
                progressLog.onRow()
                pbar.update(f.tell() - pbar.n)
                
                if row.get('subreddit') == 'AIDungeon':
                    flat_row = flatten_dict(row)
                    
                    # Add created_date field
                    created_timestamp = int(flat_row.get('created_utc', flat_row.get('created', 0)))
                    created_date = datetime.datetime.fromtimestamp(created_timestamp).strftime('%Y-%m-%d-%H%M%S')
                    flat_row['created_date'] = created_date
                    
                    new_fields = set(flat_row.keys()) - set(fieldnames)
                    if new_fields:
                        fieldnames.extend(sorted(new_fields))
                        
                        csv_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                        if processed_rows == 0:
                            csv_writer.writeheader()
                    
                    if csv_writer:
                        csv_writer.writerow(flat_row)
                    
                    processed_rows += 1
                    
                    if processed_rows % 1000 == 0:
                        print(f"Processed {processed_rows} AIDungeon records")
        
        progressLog.logProgress("\n")
    
    print(f"CSV file created: {output_file}")
    print(f"Processed {processed_rows} records from AIDungeon subreddit")

def processFolder(path: str):
    fileIterator: Iterable[str]
    if recursive:
        def recursiveFileIterator():
            for root, dirs, files in os.walk(path):
                for file in files:
                    yield os.path.join(root, file)
        fileIterator = recursiveFileIterator()
    else:
        fileIterator = os.listdir(path)
        fileIterator = (os.path.join(path, file) for file in fileIterator)
    
    for i, file in enumerate(fileIterator):
        print(f"Processing file {i+1: 3} {file}")
        processFile(file)

def main():
    if os.path.isdir(fileOrFolderPath):
        processFolder(fileOrFolderPath)
    else:
        processFile(fileOrFolderPath)
    
    print("Done :>")

if __name__ == "__main__":
    main()