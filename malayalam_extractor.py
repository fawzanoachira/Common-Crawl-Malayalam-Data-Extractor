"""
Module: Malayalam Text Extractor for Common Crawl Data
Author: Muhammed Fawzan A
Created On: 03 November 2023
Updated On: 04 November 2023

This module extracts Malayalam text content from web archives (WARC) obtained from a list of URLs from warc.paths from Common Crawl.
"""


"""
[imports]
ArchiveIterator from warcio.archiveiterator: Part of the warcio library, it iterates through records in a WARC (Web ARChive) file.
subprocess: A module used for spawning new processes, connecting to their input/output/error pipes, and obtaining their return codes.
os: A module providing a portable way of using operating system-dependent functionality, like reading or writing to the file system.
time: A module offering various time-related functions. It can be used for time-related tasks, including sleeping for a specified amount of time.
BeautifulSoup from bs4: A Python library for pulling data out of HTML and XML files. It creates a parse tree from parsed HTML and XML documents.
re: A module providing support for regular expressions (RE). It helps in working with strings, searching, and manipulating text based on patterns.
"""
from typing import Optional
import subprocess
import time
import os
from warcio.archiveiterator import ArchiveIterator
from bs4 import BeautifulSoup
import re
import csv

"""
[constants]
URL_PATH: File path constant representing the location where the list of WARC file paths is stored.
MAX_RETRIES: Constant defining the maximum number of retries for a download attempt.
RETRY_DELAY: Constant defining the delay (in seconds) between download retry attempts.
OUTPUT_DIRECTORY: Constant representing the directory where downloaded content will be saved.
"""
MAX_RETRIES = 20
RETRY_DELAY = 10
URL_PATH = 'warc.paths'
OUTPUT_DIRECTORY = 'Extracted_files/'
EXTRACTED_TEXT_DIRECTORY="Extracted Texts/"

# Ensure the output directory exists
os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)
os.makedirs(EXTRACTED_TEXT_DIRECTORY, exist_ok=True)

def process_and_check_malayalam_text(html_content: str, min_malayalam_words: int=40) -> Optional[str]:
    """
    Processes and checks for Malayalam text in HTML content.

    Args:
    - html_content(str): HTML content to extract text from.
    - min_malayalam_words(int): Minimum count of Malayalam words to consider.

    Returns: (str | None)
    - Extracted Malayalam text if it meets the minimum word count, otherwise None.
    """

    # Parse HTML content
    try:
      soup = BeautifulSoup(html_content, 'html.parser')
    except Exception as e:
      print(e)
      return None
    # Remove style and script tags
    for tag in soup(['style', 'script']):
        tag.decompose()

    # Extract text
    text_content = " ".join(re.findall(r'[\u0D00-\u0D7F]+'," ".join(soup.stripped_strings)))

    # Use a regular expression to find Malayalam words
    # malayalam_words = re.findall(r'[\u0D00-\u0D7F]+', text_content)

    if len(text_content) >= min_malayalam_words:
        return text_content
    else:
        return None

# Function to convert WARC to text and save in separate files
def convert_warc_to_text(warc_file_path: str, output_dir: str, csv_file_path: str) -> None:
    """
    Converts WARC file to text and saves Malayalam content in a text file.

    Args:
    - warc_file_path(str): Path to the WARC file to process.

    Returns:
    - None
    """
    start_time = time.time()
    with open(warc_file_path, 'rb') as warc_file:
        for record in ArchiveIterator(warc_file):
            if record.rec_type == 'response':
                # Extract the content from the response record
                content = record.content_stream().read()

                # You may need to decode the content using the appropriate character encoding
                # For example, if it's HTML, you might use:
                # decoded_content = content.decode('utf-8')

                # Save the content to a text file with a unique name
                content = process_and_check_malayalam_text(content)
                if content is not None:
                    # Extract the last 5 numbers from the URL path
                    matched = re.search(r'(\d{14}-\d{5})\.warc\.gz$', warc_file_path)
                    if matched:
                        unique_string = matched.group(1)
                        output_file_name = f"extracted_text_{unique_string}.txt"
                        output_file_path = os.path.join(output_dir, output_file_name)
                        with open(output_file_path, 'a') as text_file:
                            text_file.write(content)

    end_time = time.time()
    time_taken = (end_time - start_time) / 60
    file_size_t = os.path.getsize(warc_file_path)
    file_size = round(file_size_t/ (1024 * 1024), 2)
    output_file_size_t = os.path.getsize(output_file_path)
    output_file_size = round(output_file_size_t/ 1024, 2)

    # Record information in the CSV file
    with open(csv_file_path, 'a', newline='') as csvfile:
      fieldnames = ['File Name', 'File Path', 'Input File Size (MB)', 'Output File Size (KB)', 'Time Taken (Minutes)']
      writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
      writer.writerow({'File Name': output_file_name, 'File Path': output_file_path,
                       'Input File Size (MB)': file_size, 'Output File Size (KB)': output_file_size,
                       'Time Taken (Minutes)': time_taken})


def main():
    """
    Main function to execute the process of downloading, calling WARC conversion function
    """
    # Create a CSV file to record information
    csv_file_path = os.path.join(EXTRACTED_TEXT_DIRECTORY, 'extraction_info.csv')
    with open(csv_file_path, 'w', newline='') as csvfile:
        fieldnames = ['File Name', 'File Path', 'Input File Size (MB)','Output File Size (KB)' , 'Time Taken (Minutes)']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

    errors_file_path = os.path.join(EXTRACTED_TEXT_DIRECTORY, 'error_log_info.csv')
    with open(errors_file_path, 'w', newline='') as csvfile:
        fieldnames = ['File Path']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
    
    # Open the URL file for reading
    with open(URL_PATH, 'r') as file:
        for line in file:
            url = line.strip()
            local_filename = os.path.join(OUTPUT_DIRECTORY, os.path.basename(url))

            # Use wget with retries to download the WARC file
            for retry in range(MAX_RETRIES):
                try:
                    subprocess.check_call(['wget', url, '-O', local_filename, '--tries', str(MAX_RETRIES), '--wait', str(RETRY_DELAY)])
                    print(f"Download successful. File saved as {local_filename}")
                    break  # Successful download, exit the loop
                except subprocess.CalledProcessError:
                    print(f"Retry {retry + 1}/{MAX_RETRIES}: Download failed. Retrying...")
                    time.sleep(RETRY_DELAY)
            else:
                print("Max retries reached. Download unsuccessful.")
                # Record information in the CSV file
                with open(errors_file_path, 'a', newline='') as csvfile:
                  fieldnames = ['File Path']
                  writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                  writer.writerow({'File Path': local_filename})
                continue  # Move to the next URL

            # Convert the downloaded WARC file to text
            convert_warc_to_text(local_filename, EXTRACTED_TEXT_DIRECTORY, csv_file_path)

            # Remove the downloaded WARC file
            os.remove(local_filename)


    print("All downloads and conversions complete.")

if __name__ == "__main__":
    main()