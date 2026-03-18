"""HTML parsers for UFCStats pages.

This module contains Scrapy-based parsers for UFC events, fights,
fighters, and fight statistics (total and by-round).
"""

from datetime import datetime, timezone

from scrapy.http import Response

from ufc_scraper.parsers.base_parser import Parser
from entities import Fighter
from utils import clean_string, get_uuid_string


class FighterInfoParser(Parser):
    """Parses HTTP responses of ufcstats.com fighter pages.

    Parses key attributes of UFC fighters and yields Fighter dataclass.

    Args:
        response (Response): The HTTP response to be parsed.

    Attributes:
        _response (Response): The raw response object.
        _url (str): URL of the response.
        _id (str): Deterministic UUID derived from the response URL.
        _css_queries (Dict[str, str]): Mapping of semantic query names to
            CSS selectors used to extract fight metadata from the response.

    """

    def __init__(self, response: Response):
        super().__init__(response)
        try:
            self._fighter_stats = self._safe_css_get_all(
                self._css_queries.fighter_stats_query
            )
        except ValueError:
            # Some fighter pages (debutants, removed profiles) have no stat
            # list at all.  Downstream methods treat missing indices as None.
            self._fighter_stats = []

    def _get_fighter_name(self) -> None:
        try:
            name_raw = self._safe_css_get(self._css_queries.fighter_name_query)
            name_clean = clean_string(name_raw)
            names = name_clean.split(" ")
            self._full_name = " ".join(names)
            self._first_name = names[0]
            self._last_names = " ".join(names[1:])
        except (ValueError, IndexError):
            self._full_name = ""
            self._first_name = ""
            self._last_names = ""

        nickname_raw = self._response.css(
            self._css_queries.fighter_nickname_query
        ).get()
        self._nickname = clean_string(nickname_raw) if nickname_raw else ""

    def _get_fighter_height(self) -> None:
        self._height_ft = None
        self._height_in = None
        self._height_cm = None
        try:
            height = clean_string(self._fighter_stats[1])
            if height != "--":
                self._height_ft = int(height.split("'")[0])
                self._height_in = int(height.split("'")[1].replace('"', "").strip())
                self._height_cm = float(
                    ((self._height_ft * 12.0) * 2.54) + (self._height_in * 2.54)
                )
        except (IndexError, ValueError):
            pass

    def _get_fighter_weight(self) -> None:
        self._weight_lbs = None
        try:
            weight = clean_string(self._fighter_stats[3]).replace("lbs.", "")
            if weight != "--":
                self._weight_lbs = int(weight)
        except (IndexError, ValueError):
            pass

    def _get_fighter_reach(self) -> None:
        self._reach_in = None
        self._reach_cm = None
        try:
            reach = clean_string(self._fighter_stats[5]).replace('"', "")
            if reach != "--":
                self._reach_in = int(reach)
                self._reach_cm = int(float(reach) * 2.54)
        except (IndexError, ValueError):
            pass

    def _get_fighter_stance(self) -> None:
        try:
            self._stance = clean_string(self._fighter_stats[7])
        except (IndexError, ValueError):
            self._stance = ""

    def _get_fighter_dob(self) -> None:
        self._dob = None
        self._dob_formatted = None
        try:
            dob_string = clean_string(self._fighter_stats[9])
            if dob_string != "--":
                self._dob = dob_string
                dob_dt = datetime.strptime(dob_string, "%b %d, %Y")
                self._dob_formatted = datetime.strftime(dob_dt, "%Y-%m-%d")
        except (IndexError, ValueError):
            pass

    def _get_fighter_record(self) -> None:
        try:
            record_raw = self._safe_css_get(self._css_queries.fighter_record_query)
            record_clean = clean_string(record_raw)
            self._record = record_clean.split(": ")[1]
            self._wins = int(self._record.split("-")[0])
            self._losses = int(self._record.split("-")[1])
        except (ValueError, IndexError):
            self._record = ""
            self._wins = 0
            self._losses = 0
            self._draws = 0
            self._no_contests = 0
            return

        # If a fighter has > 0 no contests, the record looks like 'Record: 28-1-0 (1 NC)'
        try:
            self._draws = int(self._record.split("-")[2])
            self._no_contests = 0
        except ValueError:
            self._draws = int(self._record.split("-")[2].split(" ")[0])
            self._no_contests = int(
                self._record.split("-")[2].split(" ")[1].replace("(", "")
            )

    def _get_fight_ids(self) -> None:
        self._fight_ids = None
        fight_urls = self._response.css(self._css_queries.fighter_fights_query).getall()
        if fight_urls:
            fight_id_list = [get_uuid_string(url) for url in fight_urls]

            self._fight_ids = ", ".join(fight_id_list)

    def parse_response(self) -> Fighter:
        """Parse the HTML response to get key fighter attributes.

        Args:
            response (Response): The response object to query.

        Returns:
            Fighter: Dataclass containing all key fighter attributes.

        """
        self._get_fighter_name()
        self._get_fighter_height()
        self._get_fighter_weight()
        self._get_fighter_reach()
        self._get_fighter_stance()
        self._get_fighter_dob()
        self._get_fighter_record()
        self._get_fight_ids()

        return Fighter(
            scraped_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            fighter_id=self._id,
            url=self._url,
            full_name=self._full_name,
            first_name=self._first_name,
            last_names=self._last_names,
            nickname=self._nickname,
            height_ft=self._height_ft,
            height_in=self._height_in,
            height_cm=self._height_cm,
            weight_lbs=self._weight_lbs,
            reach_in=self._reach_in,
            reach_cm=self._reach_cm,
            stance=self._stance,
            dob=self._dob,
            dob_formatted=self._dob_formatted,
            record=self._record,
            wins=self._wins,
            losses=self._losses,
            draws=self._draws,
            no_contests=self._no_contests,
            fight_ids=self._fight_ids,
        )
