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
import glob

from fileStreams import getFileJsonStream
from utils import FileProgressLog

# Set the path to the comments and submissions files
commentsPath = '/Volumes/T7 Shield/reddit/comments/'
submissionsPath = '/Volumes/T7 Shield/reddit/submissions/'

def get_all_fieldnames(jsonStream):
    fieldnames = set()
    for row in jsonStream:
        flat_row = flatten_dict(row)
        fieldnames.update(flat_row.keys())
    return fieldnames

def processFile(path: str, is_comment: bool):
    print(f"Processing file {path}")
    
    # Extract date from filename
    filename = os.path.basename(path)
    date_str = filename.split('_')[1].split('.')[0] if '_' in filename else 'unknown_date'
    
    # Create output CSV file
    output_file = f"results/AIDungeon_{'comments' if is_comment else 'submissions'}_{date_str}.csv"
    
    # Define the order of important columns
    important_columns = ["created-date", "author", "author-fullname", "body", "id", "link-id", "name", "parent-id", "permalink", "score", "ups"]
    if not is_comment:
        important_columns = ['created-date', 'name', 'title', 'selftext', 'ups', 'upvote-ratio', 'author-fullname', 'permalink']
    
    with open(path, "rb") as f:
        jsonStream = getFileJsonStream(path, f)
        if jsonStream is None:
            print(f"Skipping unknown file {path}")
            return
        
        # Get all fieldnames
        fieldnames = get_all_fieldnames(jsonStream)
        final_fieldnames = [col for col in important_columns if col in fieldnames]
        final_fieldnames.extend(sorted(fieldnames - set(important_columns)))
        
        # Reset file pointer
        f.seek(0)
        jsonStream = getFileJsonStream(path, f)
        
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            csv_writer = csv.DictWriter(csvfile, fieldnames=final_fieldnames)
            csv_writer.writeheader()
            
            # Get total file size for progress bar
            f.seek(0, 2)
            file_size = f.tell()
            f.seek(0)
            
            progressLog = FileProgressLog(path, f)
            
            # Initialize tqdm progress bar
            with tqdm(total=file_size, unit='B', unit_scale=True, desc="Processing") as pbar:
                pbar.update(f.tell())
                aidungeon_count = 0
                for row in jsonStream:
                    progressLog.onRow()
                    pbar.update(f.tell() - pbar.n)
                    
                    # Check if the comment or submission is from the AIDungeon subreddit
                    if row.get('subreddit') == 'AIDungeon':
                        # Flatten the row
                        flat_row = flatten_dict(row)
                        
                        # Convert 'created' to 'created-date'
                        if 'created' in flat_row:
                            created_timestamp = int(flat_row['created'])
                            created_date = datetime.utcfromtimestamp(created_timestamp).strftime('%Y%m%d-%H%M%S')
                            flat_row['created-date'] = created_date
                        
                        # Write the row to CSV
                        csv_writer.writerow(flat_row)
                        
                        aidungeon_count += 1
                        
                        if aidungeon_count % 1000 == 0:
                            print(f"Processed {aidungeon_count} AIDungeon {'comments' if is_comment else 'submissions'}")
            
            progressLog.logProgress("\n")
    
    print(f"CSV file created: {output_file}")
    print(f"Processed {aidungeon_count} {'comments' if is_comment else 'submissions'} from AIDungeon subreddit")

def processFolder(path: str, is_comment: bool):
    fileIterator: Iterable[str]
    fileIterator = os.listdir(path)
    fileIterator = (os.path.join(path, file) for file in fileIterator)
    
    for i, file in enumerate(fileIterator):
        print(f"Processing file {i+1: 3} {file}")
        processFile(file, is_comment)

def main():
    if os.path.isdir(commentsPath):
        processFolder(commentsPath, is_comment=True)
    else:
        processFile(commentsPath, is_comment=True)
    
    if os.path.isdir(submissionsPath):
        processFolder(submissionsPath, is_comment=False)
    else:
        processFile(submissionsPath, is_comment=False)
    
    print("Done :>")

if __name__ == "__main__":
    main()