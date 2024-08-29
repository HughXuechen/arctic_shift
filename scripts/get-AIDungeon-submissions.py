import sys
version = sys.version_info
if version.major < 3 or (version.major == 3 and version.minor < 10):
	raise RuntimeError("This script requires Python 3.10 or higher")
import os
from typing import Iterable
from tqdm import tqdm
import csv
import json
import datetime

from fileStreams import getFileJsonStream
from utils import FileProgressLog

# check submissions or comments
fileOrFolderPath = '/Volumes/T7 Shield/reddit/submissions/'
recursive = False

def processFile(path: str):
    # Extract the month and year from the input file name
    file_name = os.path.basename(path)
    month_year = file_name.split('_')[1].split('.')[0]  # Extracts "2024-04" from "RS_2024-04.zst"
    output_file = os.path.join("results", f"AIDungeon_submission_{month_year}.csv")
    
    # Ensure the results directory exists
    os.makedirs("results", exist_ok=True)
    
    print(f"Processing file {path}")
    
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
        
        # Define the order of the first columns
        first_columns = ['created_date', 'name', 'title', 'selftext', 'ups', 'upvote_ratio','author_fullname''permalink']
        
        # Get the first row from AIDungeon subreddit to determine columns
        first_row = None
        for row in jsonStream:
            if row.get('subreddit') == 'AIDungeon':
                first_row = row
                break
        
        if first_row is None:
            print("No data found for AIDungeon subreddit in the file.")
            return
        
        # Flatten the first row and add created_date
        flat_first_row = flatten_dict(first_row)
        created_timestamp = int(flat_first_row.get('created_utc', flat_first_row.get('created', 0)))
        created_date = datetime.datetime.fromtimestamp(created_timestamp).strftime('%Y-%m-%d-%H%M%S')
        flat_first_row['created_date'] = created_date
        
        # Prepare the column order
        columns = first_columns + [col for col in flat_first_row.keys() if col not in first_columns]
        
        # Initialize CSV writer with the columns
        csv_writer = csv.DictWriter(csvfile, fieldnames=columns, extrasaction='ignore')
        csv_writer.writeheader()
        
        # Write the first row
        csv_writer.writerow(flat_first_row)
        
        # Initialize counter for rows
        processed_rows = 1
        
        # Initialize tqdm progress bar
        with tqdm(total=file_size, unit='B', unit_scale=True, desc="Processing") as pbar:
            pbar.update(f.tell())
            for row in jsonStream:
                try:
                    progressLog.onRow()
                    pbar.update(f.tell() - pbar.n)
                    
                    # Only process rows from the AIDungeon subreddit
                    if row.get('subreddit') == 'AIDungeon':
                        # Flatten the row and add created_date
                        flat_row = flatten_dict(row)
                        created_timestamp = int(flat_row.get('created_utc', flat_row.get('created', 0)))
                        created_date = datetime.datetime.fromtimestamp(created_timestamp).strftime('%Y-%m-%d-%H%M%S')
                        flat_row['created_date'] = created_date
                        
                        # Write to CSV
                        csv_writer.writerow(flat_row)
                        processed_rows += 1
                except Exception as e:
                    print(f"Error processing row: {e}")
                    continue
        
        progressLog.logProgress("\n")
    
    print(f"AIDungeon submissions ({processed_rows} rows) saved to {output_file}")

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