import sys
import os
from typing import Iterable
from tqdm import tqdm
import json
import csv
from datetime import datetime
import glob
import base64

from fileStreams import getFileJsonStream
from utils import FileProgressLog

# Check Python version
if sys.version_info < (3, 10):
    raise RuntimeError("This script requires Python 3.10 or higher")

# Set the paths for comments and submissions
COMMENTS_PATH = 'E:/reddit/comments/'
SUBMISSIONS_PATH = 'E:/reddit/submissions/'

def flatten_dict(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            items.append((new_key, json.dumps(v, ensure_ascii=False)))
        elif isinstance(v, str) and len(v) > 32000:  # Encode long strings
            items.append((new_key, base64.b64encode(v.encode('utf-8')).decode('ascii')))
        else:
            items.append((new_key, str(v)))
    return dict(items)

def process_file(path: str, data_type: str):
    print(f"Processing {data_type} file: {path}")
    
    # Extract date from filename
    filename = os.path.basename(path)
    date_str = filename.split('_')[1].split('.')[0]
    
    # Create output CSV file
    output_file = f"results/AIDungeon_{data_type}_{date_str}.csv"
    os.makedirs("results", exist_ok=True)
    
    try:
        with open(path, "rb") as f, open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            jsonStream = getFileJsonStream(path, f)
            if jsonStream is None:
                print(f"Skipping unknown file {path}")
                return
            
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
                        created_timestamp = flat_row.get('created_utc')
                        if created_timestamp:
                            created_date = datetime.fromtimestamp(float(created_timestamp)).strftime('%Y-%m-%d-%H%M%S')
                            flat_row['created_date'] = created_date
                        
                        new_fields = set(flat_row.keys()) - set(fieldnames)
                        if new_fields:
                            fieldnames.extend(sorted(new_fields))
                            
                            csv_writer = csv.DictWriter(csvfile, fieldnames=fieldnames, escapechar='\\', quoting=csv.QUOTE_ALL)
                            if processed_rows == 0:
                                csv_writer.writeheader()
                        
                        if csv_writer:
                            try:
                                csv_writer.writerow(flat_row)
                            except Exception as e:
                                print(f"Error writing row: {e}")
                                print(f"Problematic row: {flat_row}")
                        
                        processed_rows += 1
                        
                        if processed_rows % 1000 == 0:
                            print(f"Processed {processed_rows} AIDungeon {data_type}")
        
            progressLog.logProgress("\n")
    
    except Exception as e:
        print(f"Error processing {path}: {str(e)}")
        return

    print(f"CSV file created: {output_file}")
    print(f"Processed {processed_rows} {data_type} from AIDungeon subreddit")

def process_folder(path: str, data_type: str):
    for file_path in glob.glob(os.path.join(path, '*.zst')):
        process_file(file_path, data_type)

def main():
    print("Processing comments...")
    if os.path.isdir(COMMENTS_PATH):
        process_folder(COMMENTS_PATH, 'comments')
    elif os.path.isfile(COMMENTS_PATH):
        process_file(COMMENTS_PATH, 'comments')
    else:
        print(f"Error: {COMMENTS_PATH} is neither a file nor a directory")

    print("\nProcessing submissions...")
    if os.path.isdir(SUBMISSIONS_PATH):
        process_folder(SUBMISSIONS_PATH, 'submissions')
    elif os.path.isfile(SUBMISSIONS_PATH):
        process_file(SUBMISSIONS_PATH, 'submissions')
    else:
        print(f"Error: {SUBMISSIONS_PATH} is neither a file nor a directory")

    print("Done :>")

if __name__ == "__main__":
    main()