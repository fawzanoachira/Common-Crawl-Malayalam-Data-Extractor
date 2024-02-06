# Malayalam Data Extractor from Common Crawl

## Overview

This Python script is designed to extract Malayalam data from Common Crawl. It downloads data from a given set of URL, converts it from a .warc file to text, removes HTML tags, and extracts Malayalam data from the text files and save it as another text file which will be the same name of the file downloaded from given URL.

## Features

- Download data from Common Crawl
- Convert .warc files to plain text
- Remove HTML tags
- Extract Malayalam content
- Save it as seperate text files

## Usage

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/fawzanoachira/Common-Crawl-Malayalam-Data-Extractor.git
   cd Common-Crawl-Malayalam-Data-Extractor

2. **Install Dependencies**

   ```bash
   pip install -r requirements.txt

3. **Run the script**

   ```bash
   python3 main.py

4. **Output**

   The script will generate a file named which is same as the corresponding file downloaded from URL, containing the extracted Malayalam content.

## Contributing

Contributions are welcome! If you find any issues or have suggestions for improvements, please [open an issue]([https://github.com/fawzanoachira/Common-Crawl-Malayalam-Data-Extractor.git]) or submit a pull request.

