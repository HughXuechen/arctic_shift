import sys
version = sys.version_info
if version.major < 3 or (version.major == 3 and version.minor < 10):
    raise RuntimeError("This script requires Python 3.10 or higher")
import os
from typing import Iterable
from tqdm import tqdm
import json
import csv
from datetime import datetime

from fileStreams import getFileJsonStream
from utils import FileProgressLog

# Set the path to the comments file
fileOrFolderPath = os.path.join(os.path.dirname(__file__), '..', 'reddit', 'comments')

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
    
    # Extract date from filename
    filename = os.path.basename(path)
    date_str = filename.split('_')[1].split('.')[0]
    
    # Create output CSV file
    output_file = f"results/AIDungeon_comments_{date_str}.csv"
    
    # Define the order of important columns
    important_columns = ["created-date", "author", "author_fullname", "body", "id", "link_id", "name", "parent_id", "permalink", "score", "ups"]
    
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
        
        # Initialize CSV writer
        fieldnames = set()
        csv_writer = None

        # Initialize tqdm progress bar
        with tqdm(total=file_size, unit='B', unit_scale=True, desc="Processing") as pbar:
            pbar.update(f.tell())
            aidungeon_count = 0
            for row in jsonStream:
                progressLog.onRow()
                pbar.update(f.tell() - pbar.n)
                
                # Check if the comment is from the AIDungeon subreddit
                if row.get('subreddit') == 'AIDungeon':
                    # Flatten the row
                    flat_row = flatten_dict(row)
                    
                    # Convert 'created' to 'created-date'
                    if 'created' in flat_row:
                        created_timestamp = int(flat_row['created'])
                        created_date = datetime.utcfromtimestamp(created_timestamp).strftime('%Y%m%d-%H%M%S')
                        flat_row['created-date'] = created_date
                    
                    # Update fieldnames with any new fields
                    new_fields = set(flat_row.keys()) - fieldnames
                    if new_fields:
                        fieldnames.update(new_fields)
                        # Prepare the final fieldnames list with the specified order
                        final_fieldnames = [col for col in important_columns if col in fieldnames]
                        final_fieldnames.extend(sorted(fieldnames - set(important_columns)))
                        
                        # Create a new CSV writer with updated fieldnames
                        csv_writer = csv.DictWriter(csvfile, fieldnames=final_fieldnames)
                        if aidungeon_count == 0:
                            csv_writer.writeheader()
                    
                    # Write the row to CSV
                    if csv_writer:
                        csv_writer.writerow(flat_row)
                    
                    aidungeon_count += 1
                    
                    if aidungeon_count % 1000 == 0:
                        print(f"Processed {aidungeon_count} AIDungeon comments")
        
        progressLog.logProgress("\n")
    
    print(f"CSV file created: {output_file}")
    print(f"Processed {aidungeon_count} comments from AIDungeon subreddit")

def main():
    processFile(fileOrFolderPath)
    print("Done :>")

if __name__ == "__main__":
    main()