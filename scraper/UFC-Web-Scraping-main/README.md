# UFC Web Scraping

A Scrapy-based web scraping app for extracting UFC fight data from [ufcstats.com](http://ufcstats.com). This project collects detailed information about events, fights, fighters, and fight statistics with support for both aggregate and round-by-round data.

## Features

- **Event Data**: Scrape UFC event details including name, date, location, and fight listings
- **Fight Information**: Extract fight metadata such as weight class, rounds, finish methods, and officials
- **Fighter Profiles**: Collect fighter biographies including physical stats, records, and career opponents
- **Fight Statistics**: Fight metrics including round-by-round metrics (strikes, takedowns, control time, etc.)

All spiders are configured with respectful rate limiting:
- 1 second download delay
- Randomized delay to appear more natural
- Adjust in spider `custom_settings` if needed

## Getting Started

You can crawl everything with `make crawl_all`. You can also run specific spiders with `make crawl_%` - for example, if you just want to crawl fighter metrics, run `make crawl_fighters`.

When you export spider output with `OUTPUT=csv` or another feed format, files are written to the repository-root [`data/`] directory.

## Incremental Updates

If you already have CSVs in the repository-root `data/` directory, use `make update_%` to append only unseen rows to the existing file. For example, `make update_events` appends only event rows whose `event_id` is not already present in `data/events.csv`.

The incremental commands are:

- `make update_events`
- `make update_fights`
- `make update_fight_stats`
- `make update_fight_stats_by_round`
- `make update_fighters`
- `make update_all`

These commands still load the listing pages on UFC Stats, but they skip detail pages whose IDs are already present in your existing CSVs.

## Development

### Adding a New Data Field

1. Update the relevant dataclass in `entities.py`
2. Add extraction logic to the corresponding parser in `parsers.py`
3. Update constants in `constants.py` if needed
4. Add test coverage in `tests/parser_tests/`

## License

This project is open source. Please respect ufcstats.com's terms of service and use rate limiting when scraping.

## Acknowledgments

Data source: [UFCStats.com](http://ufcstats.com)

## Contact

For questions or feedback, please feel free to open an issue on GitHub or email me on remy.pereira@hotmail.co.uk
