# Guttenberg-Text
Scraping text from Guttenberg for Vector Index benchmark.

The script scrapes, creates EvaDB table and inserts the text to create ~1M vector indices. The scraped text is stored in 'new_gut.txt'

Ref: https://github.com/aparrish/gutenberg-dammit


Download the guttenburg corpus from : http://static.decontextualize.com/gutenberg-dammit-files-v002.zip


## Setup
Ensure that the local Python version is >= 3.8. Install the required libraries:

```bat
pip install -r requirements.txt
```

## Usage
Run script: 
```bat
python retrieve_text.py
```
