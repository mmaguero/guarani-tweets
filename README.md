# guarani-tweets
Download guarani-dominant tweets

## Installation

`python3 -m venv venv`, then activate the virtual env with `source venv/bin/activate` (to get out of the virtualenv, run `deactivate`).

Install requirements `pip install -r requirements.txt`.

## Execution

Download tweets:

`python scrape_tweets.py [experiment_number] [use_api]`

    experiment_number: used for raw tweets folder name.
    use_api: by API or by SNScrape? True or False.

For filter from downloaded tweets:

`python get_filtered.py [output_dir] [sample1000] [language_lookup]`

    output_dir: path to output directory.
    sample1000: sample of 1000 tweets without lang identification. True or False.
    language_lookup: custom language identification by terms lookup. True or False.
    
### Language detection

We used this [GitHub Repository](https://github.com/mmaguero/lang_detection) for language identification.
