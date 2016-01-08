### Configuration

The script should be run in Python 3.

Install dependencies (requests, beautifulsoup4, and toml) with:

> python -m pip install -r requirements.txt


Add additional LifeMiles accounts to the configuration file (config.toml).


### Command Line Usage

To export a clean CSV file with sorted data:

> lifemiles -e airmiles.csv

The default sort is on the origin airport. To add reverse and unchecked routes, and sort by airmiles:

> lifemiles -e airmiles.csv -u -r -s 2

Rescan failed routes using an earlier search date (by default a weekday date about one month in the future is used):

> lifemiles -d 13-05-2015
