#!/usr/bin/env python3
import urllib.parse
import csv
import signal, sys, time, os.path, re
import argparse
import logging
from itertools import combinations, permutations
from datetime import datetime, timedelta
from queue import Queue
from threading import Thread, Lock
from random import randint

import requests
import toml
from bs4 import BeautifulSoup


# called on SIGINT (triggered by Ctrl+C)
_clean_up = None
def _signal_handler(signal, frame):
		if _clean_up: _clean_up()
		sys.exit(0)



class Airport(object):
	def __init__(self, code, name, city, country_code):
		self.code, self.name, self.city, self.country_code = code, name, city, country_code

	def __str__(self):
		return self.code


class Route(object):
	def __init__(self, origin_airport, dest_airport):
		self._origin, self._dest = origin_airport, dest_airport
		self.is_valid, self.error, self.scraped = None, None, False
		self.airmiles = -1
		self.date = ''

	@property
	def origin(self):
		return self._origin

	@property
	def dest(self):
		return self._dest

	@property
	def no_search_result(self):
	    return self.error == "ERROR:SoldOutOrInvalid"
	
	def as_list(self):
		r = [self._origin.code, self._dest.code]

		if self.is_valid == False:
			return r + ["NA", self.date, "ERROR:NonExistent"]
		if self.error:
			return r + ["NA", self.date, self.error]

		return r + [self.airmiles, self.date, ""]

	def description(self, with_error=False):
		airmiles_str = '{:,}'.format(self.airmiles) if self.airmiles > -1 else 'NA'
		desc = '{!s}: {}'.format(self, airmiles_str)
		if with_error and self.error:
			desc = '{}\t[{}]'.format(desc, self.error)
		return desc

	def __repr__(self):
		return "Route({!s}, {!s})".format(self._origin, self._dest)

	def __str__(self):
		return "{!s}-{!s}".format(self._origin, self._dest)

	def __eq__(self, other):
		if isinstance(other, self.__class__):
			return (self.origin.code, self.dest.code) == (other.origin.code, other.dest.code)
		return NotImplemented

	def __ne__(self, other):
		if isinstance(other, self.__class__):
			return not self.__eq__(other)
		return NotImplemented

	def __hash__(self):
		return hash((self.origin.code, self.dest.code))

	def set_scrape_data(self, airmiles, date, error):
		self.date = date
		self.scraped, self.error = True, error
		has_airmiles = airmiles[0] != 'N'
		self.airmiles = int(airmiles) if has_airmiles else -1
		self.is_valid = True if has_airmiles and error != 'ERROR:NonExistent' else False

	def reset(self):
		self.scraped = False
		self.is_valid = None
		self.error = None
		self.airmiles = -1


class RouteGenerator(object):
	"""Generates Routes from imported Airports.
	"""
	def __init__(self, airports_csv_fname='airports.csv', auto_import=True, sort_airports=True):
		"""Constructs an instance of RouteGenerator, optionally auto-importing airports.

		Args:
			airports_csv_fname: filename of airports CSV; rows contain: [code, name, city, country code]
			auto_import: imports airports from the CSV file (default True)
			sort_airports: when reverse routes considered identical, the alphabetically ordered origin-dest is used
		"""
		self.airports, self.airports_dict = [], {}
		self.airports_csv_fname = airports_csv_fname
		if auto_import:
			self._import_airports(sort_airports)

	def _import_airports(self, sort):
		with open(self.airports_csv_fname, 'r') as f:
			reader = csv.reader(f)
			for airport_data in reader:
				a = Airport(*airport_data)
				self.airports += [a]
				self.airports_dict[a.code] = a

		if sort:
			# sorting ensures consistency in origin and destinations when generating combinations
			# when not generating reverse routes, origin-dest will always be in alphabetical order:
			# e.g. ICN-LHR will always be generated, and not LHR-ICN
			self.airports = sorted(self.airports, key=lambda a: a.code)
		logging.info("Imported {} airports".format(len(self.airports)))

	def generate_all_routes(self, include_reverse=True):
		"""Generates all combinations or permutations of a list of airports. Imports airports if unimported.

		Args:
			include_reverse: includes reverse routes (e.g. ICN-ORD and ORD-ICN) (default True)
		Returns:
			generator of Routes for each combination/permutation of origin and destination airports
		"""
		if not self.airports:
			self._import_airports()

		logging.info("Generating routes ({}cluding reverse)".format('in' if include_reverse else 'ex'))
		gen = permutations if include_reverse else combinations
		for (origin, dest) in gen(self.airports, 2):
			yield Route(origin, dest)

	def route_from_codes(self, origin_code, dest_code):
		"""Constructs a route from a pair of IATA airport codes (case-sensitive)

		Args:
			origin_code: IATA code for origin airport
			dest_code: IATA code for destination airport
		Returns:
			a Route for the given origin and destination
		Raises:
			KeyError: if an airport with either code is not in the RouteGenerator's list of airports
		"""
		return Route(self.airports_dict[origin_code], self.airports_dict[dest_code])

	def get_airport_by_code(self, code):
		"""Retrieves the Airport with the given IATA code.
		
		Args:
			code: IATA code for an airport
		Returns:
			an Airport
		Raises:
			KeyError: if airport with given code is not in the RouteGenerator's list of airports
		"""
		return self.airports_dict[code]


class Scraper(object):
	"""Scraper for LifeMiles airmiles data.

		For each user account, it runs a ScraperSession (one per thread).
		Note: It is not possible to make parallel requests from a single account as search results
		      are not returned immediately but stored on the server and only presented after two
		      redirects.
		A target search date is generated that falls on a weekday about one month in the future;
		this is to avoid failed searches due to sold out flights for dates in the near future.
	"""

	DATE_FORMAT = "%m-%d-%Y"
	DATE_OUTPUT_FORMAT = "%d %B %Y (%A)"

	def __init__(self, credentials, routes, raw_results_fname, date_offset=28, want_weekday=True):
		self.date = self._generate_future_date(date_offset, want_weekday)
		self.sessions = [ScraperSession(cred, self.date) for cred in credentials]
		self.nsessions = len(self.sessions)
		self.logged_in, self.scraped_routes = False, []
		self.routes, self.pending_routes, self.scraped_routes = routes, None, []
		self.results = Results(raw_results_fname)
		self.lock = Lock()
		self.rq = self.sq = None

	def import_scraped_routes(self):
		"""Imports previously scraped results and removes them from the target routes to avoid repeat searches.

			Should be called before prioritisation or order will be reset.
		"""
		if not self.routes:
			raise Exception("Add all target routes before importing scraped routes")

		self.results.import_into(self.routes)

		self.scraped_routes = [r for r in self.routes if r.scraped]
		self.pending_routes = [r for r in self.routes if not r.scraped]
		logging.info("Imported {} scraped routes".format(len(self.scraped_routes)))

		duplicates = len(self.scraped_routes) - len(set(self.scraped_routes))
		if duplicates:
			logging.info("Found {} duplicate scraped routes".format(duplicates))

	def prioritise_routes_by_success(self):
		"""Reorders the list of routes according to the frequency of successful scrapes for each airport.

			Must be run manually, and is only of benefit if there are already many results from a previous scrape.
			The first ~20% of routes searched will yield the most data.
		"""
		if not (self.scraped_routes and self.pending_routes):
			return

		airport_counts = {}
		routes_with_airmiles = [r for r in self.scraped_routes if r.airmiles != -1]
		for r in routes_with_airmiles:
			airport_counts[r.origin.code] = airport_counts.get(r.origin.code, 0) + 1
			airport_counts[r.dest.code] = airport_counts.get(r.dest.code, 0) + 1

		def priority(route):
			origin_priority = airport_counts.get(route.origin.code, 0)
			dest_priority = airport_counts.get(route.dest.code, 0)

			bonus = 2**20 if (origin_priority > 0 and dest_priority > 0) else 0
			if origin_priority == 0 and dest_priority == 0:
				bonus = -2**20
			p = (origin_priority + 1) * (dest_priority + 1) + bonus
			return p

		self.pending_routes = sorted(self.pending_routes, key=priority, reverse=True)
		logging.info("Prioritised routes")

	def randomise_routes(self):
		if not self.pending_routes:
			self.pending_routes = list(routes)
		self.pending_routes = sorted(self.pending_routes, key=lambda _: randint(-10,10))
		logging.info("Randomised routes")

	def rescan(self, date):
		try:
			self.date = datetime.strptime(date, Scraper.DATE_FORMAT)
		except ValueError:
			logging.error("Invalid date specified; should be in the format MM-DD-YYYY")
			return

		print("\n")
		msg = datetime.strftime(self.date, "Rescanning failed routes (SoldOutOrInvalid) for date "+Scraper.DATE_OUTPUT_FORMAT)
		logging.info(msg)
		logging.info("If the scan fails again, the repeat result will still be output (with the new search date)")

		self.pending_routes = [r for r in self.routes if r.scraped and r.no_search_result]
		for r in self.pending_routes:
			r.reset()

		self.randomise_routes()

		logging.info("Set target routes to search to scraped routes with no search result ('ERROR:SoldOutOrInvalid')")
		logging.info("{} routes in total".format(len(self.pending_routes)))
		print("\n")

		self.start()


	def start(self):
		"""Initiate the scrape.

			If the route list is very long, it may take several days or even weeks to complete,
			but can be gracefully interrupted by calling stop() and resumed later.

			The (overall average) scrape rate is estimated at 1000 per hour per account.
			With 5 user accounts, that is about 100,000 per day (running uninterrupted).
		"""
		if not self._init_scrape():
			return

		self.sq = Queue(self.nsessions)
		self.rq = Queue(len(self.pending_routes))
		for route in self.pending_routes:
			self.rq.put(route)

		for i in range(self.nsessions):
			t = Thread(target=self._do_scrape)
			t.daemon = True
			t.start()

		self.results.begin_write()

		for session in self.sessions:
			self.sq.put(session)

		while(True):
			if(self.rq.empty()):
				break
			# necessary because rq.join() blocks and prevents interrupt; signal.pause() not an option on Windows
			time.sleep(1)

		self.results.end_write()

	def stop(self):
		"""Stop the scrape (awaiting completion of active searches."""
		logging.info("Stopping scrape")
		if(self.rq):
			logging.info("Awaiting completion of active searches...\n")
			self.rq.queue.clear()
			while(not self.sq.full()):
				time.sleep(1)
			self.rq = self.sq = None
		self.results.end_write()
		logging.info("Total scraped routes: {}".format(len(self.scraped_routes)))

	def _init_scrape(self):
		logging.info("Scrape started - {} total routes with {} accounts".format(len(self.routes), self.nsessions))
		logging.info(self.date.strftime(("Search date is "+Scraper.DATE_OUTPUT_FORMAT).format(self.date)))
		logging.info("{} of {} target routes scraped".format(len(self.scraped_routes), len(self.routes)))
		logging.info("Appending results to {}".format(self.results.filename))

		if not self.logged_in:
			logging.info("Logging in...\n")
			for session in self.sessions:
				session.login()
			if all(s.logged_in for s in self.sessions):
				self.logged_in = True
			else:
				logging.error("Scrape aborted")
				logging.error("Failed to log in to one or more user accounts. Scrape aborted.")
				return False
		print("\n")
		return True

	def _generate_future_date(self, days_to_add, want_weekday):
		today = datetime.today()
		is_weekend = today.weekday() in (5, 6)
		if is_weekend and want_weekday:
			days_to_add += 3
		future_date = today + timedelta(days=days_to_add)
		return future_date

	def _do_scrape(self, fast_filter_mode=False):
		while(not self.rq.empty()):
			session = self.sq.get()
			route = self.rq.get()

			if fast_filter_mode:
				exists = session._check_route_exists(route)
				if not exists:
					self._save_result(route)
				else:
					pass
			else:
				session.scrape_airmiles(route)
				with self.lock:
					self._save_result(route)

			self.sq.put(session)
			self.sq.task_done()
			self.rq.task_done()

	def _save_result(self, route, print_result=True):
		self.results.write(route)

		self.scraped_routes += [route]
		if print_result:
			nscraped = len(self.scraped_routes)
			if(nscraped % 250 == 0):
				logging.info("Scrape Count = {}".format(nscraped))
			print(route.description(with_error=True))



class ScraperSession(object):
	"""Encapsulates a LifeMiles session for a user account, and can retrieve airmiles data for a route.
	"""
	LOGIN_URL = 				"https://www.lifemiles.com/lib/ajax/ENG/getSession.aspx"
	ROUTE_VALIDATION_URL = 		"https://www.lifemiles.com/eng/use/red/dynredparsocae.aspx"
	ROUTE_VALIDATION_PARAMS = 	{'accion': 'exists', 'SocAe': 'SS', # SS means SmartSearch
								 'ori': None, 'dest': None}
	REDIRECTING_RESULT_URL = 	"https://www.lifemiles.com/eng/use/red/DYNREDCAL.ASPX?opc=GEN"
	ALT_REDIRECTING_RESULT_URL = 	"https://www.lifemiles.com/eng/use/red/DYNREDCAL.ASPX?qd=2&amp;opc=GEN"
	REDIRECTING_RESULT_HEADERS =	{'Referer': "https://www.lifemiles.com/eng/use/red/dynredcal.aspx",
								 	 'Content-Type': 'application/x-www-form-urlencoded'}
	ALT_REDIRECTING_RESULT_HEADERS ={'Referer': "https://www.lifemiles.com/eng/use/red/dynredcal.aspx&qd=2",
								 	 'Content-Type': 'application/x-www-form-urlencoded'}
	RESULT_URL = 				"https://www.lifemiles.com/eng/use/red/dynredflts.aspx"
	RESULT_HEADERS = 			{'Referer': "https://www.lifemiles.com/eng/use/red/dynredcal.aspx"}
	ALT_RESULT_HEADERS = 		{'Referer': "https://www.lifemiles.com/eng/use/red/dynredcal.aspx&qd=2"}
	SEARCH_URL = 				"https://www.lifemiles.com/eng/use/red/dynredcal.aspx"
	SEARCH_REQUEST_HEADERS = 	{'Referer': "https://www.lifemiles.com/eng/use/red/dynredpar.aspx",
								 'Content-Type': 'application/x-www-form-urlencoded'}
	SEARCH_PARAMS = {'cmbSocAe': '---', 'hidItineraryType': '1',
					'cmbOrigen': 'LHR', 'cmbDestino': 'ICN', 'fechaSalida': '05%2F01%2F2015',
					'horaSalida': '0000', 'fechaRegreso': '', 'horaRegreso': '0000', 'cmbOrigen1': '-1', 'cmbDestino1': '-1',
					'fechaSalida1': '', 'horaSalida1': '0000', 'cmbOrigen2': '-1', 'cmbDestino2': '-1', 'fechaSalida2': '',
					'horaSalida2': '0000', 'cmbOrigen3': '-1', 'cmbDestino3': '-1', 'fechaSalida3': '', 'horaSalida3': '0000',
					'cmbOrigen4': '-1', 'cmbDestino4': '-1', 'fechaSalida4': '', 'horaSalida4': '0000', 'cmbOrigen5': '-1',
					'cmbDestino5': '-1', 'fechaSalida5': '', 'horaSalida5': '0000', 'cmbOrigen6': '-1', 'cmbDestino6': '-1',
					'fechaSalida6': '', 'horaSalida6': '0000', 'cmbOrigen7': '-1', 'cmbDestino7': '-1', 'fechaSalida7': '',
					'horaSalida7': '0000', 'cmbOrigen8': '-1', 'cmbDestino8': '-1', 'fechaSalida8': '', 'horaSalida8': '0000',
					'CmbPaxNum': '1', 'cabin': 'Y', 'hidRedemptionType': '1', 'promoCode': '', 'hidRedir': 'N', 'hidCSocio': 'SS',
					'hidinput': 'textDestino', 'hidlength': '', 'txtCap': ''}
	PARAM_NAME_DATE, PARAM_NAME_ORIGIN_AIRPORT, PARAM_NAME_DEST_AIRPORT = 'fechaSalida', 'cmbOrigen', 'cmbDestino'

	def __init__(self, cred, date):
		self.date_str = date.strftime(Scraper.DATE_FORMAT)
		self.session = requests.session()
		self.username, self.password = cred['username'], cred['password']
		self.is_anonymous = not self.username

		self.cred_params = {'user': self.username, 'pass': self.password}
		self.logged_in = False
		self.pause = False

	def login(self):
		if self.is_anonymous:
			self.logged_in = True		# FIXME
			logging.info("Using anonymous account")		# FIXME
			return

		r = self.session.get(ScraperSession.LOGIN_URL, params=self.cred_params)
		if('Your sign-in information is incorrect' not in r.text):
			self.logged_in = True
			logging.info("[{}] Logged in".format(self.username))
		else:
			logging.error("[Error logging in; check credentials for {}]".format(self.username))

	def _check_route_exists(self, route, update_route=True):
		route_params = dict(ScraperSession.ROUTE_VALIDATION_PARAMS)
		route_params['ori'] = route.origin
		route_params['dest'] = route.dest
		
		r = self.session.get(ScraperSession.ROUTE_VALIDATION_URL, params=route_params)

		exists = r.text == "S"
		if update_route:
			route.is_valid = exists
		return exists

	def _wait_and_relogin(self, minutes=1):
		self.logged_in = False
		while not self.logged_in:
			logging.info("Re-logging in {!s}".format(self))
			time.sleep(minutes*60)
			self.login()
		self.pause = False

	def scrape_airmiles(self, route, update_route=True, max_retries=3, allow_redirect=False, delay=2):
		"""Retrieve airmiles for a route.

		Args:
			route: a target search Route
			update_route: update route with the results of the search (default: True)
		"""

		if self.pause:
			self._wait_and_relogin()

		route.scraped = True
		route.date = self.date_str

		try:
			self._check_route_exists(route, update_route=True)
		except requests.exceptions.RequestException as e:
			route.error = "ERROR:HttpOrTimeout"
			time.sleep(delay)
			return None
		if not route.is_valid:
			return None

		MAX_TRIES, retry, success = max_retries, 0, False
		while not success and retry < MAX_TRIES:
			if retry >= 1:
				logging.warning("HttpError or Timeout - retrying ({}) for route {!s}".format(retry, route))
				time.sleep(delay)

			try:
				search_params = urllib.parse.urlencode(self._generate_route_search_params(route))
				r = self.session.post(ScraperSession.SEARCH_URL, data=search_params, headers=ScraperSession.SEARCH_REQUEST_HEADERS)
				success = True
			except requests.exceptions.RequestException as e:
				if retry >= MAX_TRIES:
					route.error = "ERROR:HttpOrTimeout"
					return None
				else:
					retry += 1
					continue

			try:
				result_params = self._extract_result_params(r.text)
				url = ScraperSession.REDIRECTING_RESULT_URL
				headers = ScraperSession.REDIRECTING_RESULT_HEADERS
				r = self.session.post(url, data=result_params, headers=headers)
				success = True
			except requests.exceptions.RequestException as e:
				if retry >= MAX_TRIES:
					route.error = "ERROR:HttpOrTimeout"
					return None
				else:
					retry += 1
					continue
			except AttributeError as e:
				self.pause = True
				route.error = "ERROR:ServerProblem"
				logging.warning("Error extracting data from redirecting page for route {!s}".format(route))
				return None
		if not success:
			return None

		if allow_redirect:
			"""
			slows down search and always seems to return SoldOut response; better to try different date
			if all flights for the date are sold out we are redirected back to dynredcal.aspx with addition of
			query parameter (qd=2) with a new form submission url: DYNREDCAL.ASPX?qd=2&amp;opc=GEN
			(the viewstate and eventvalidation values are the same)
			we then load this new page (instead of dynredflts.aspx) setting the referer to dynredcal.aspx&qd=2
			if we are redirected to dynreddatessocae ("date search"?) this time, it implies the date is sold out
			- or that there are no flights at all for that route no matter which date we search
			"""
			if 'qd=2' in r.text:
				time.sleep(delay)
				try:
					result_params = self._extract_result_params(r.text)
					url = ScraperSession.ALT_REDIRECTING_RESULT_URL
					headers = ScraperSession.ALT_REDIRECTING_RESULT_HEADERS
					r = self.session.post(url, data=result_params, headers=headers)
				except requests.exceptions.RequestException as e:
					route.error = "ERROR:HttpOrTimeout"
					return None

				if 'dynreddatessocae' in r.text:
					route.error = "ERROR:SoldOutOrInvalid"
					return None
				else:
					try:
						r = self.session.get(ScraperSession.RESULT_URL, headers=ScraperSession.ALT_RESULT_HEADERS)
					except requests.exceptions.RequestException as e:
						route.error = "ERROR:HttpOrTimeout"
						return None
			else:
				try:
					r = self.session.get(ScraperSession.RESULT_URL, headers=ScraperSession.RESULT_HEADERS)
				except requests.exceptions.RequestException as e:
					route.error = "ERROR:HttpOrTimeout"
					return None

		if "Now searching" in r.text:	
			route.error = "ERROR:SoldOutOrInvalid"
			return None
		elif 'There was problem' in r.text:
			logging.error("Server error when searching for route {!s}".format(route))
			route.error = "ERROR:ServerProblem"
			return None

		airmiles = self._extract_airmiles_field(r.text)
		if airmiles:
			if update_route: route.airmiles = airmiles
		else:
			route.airmiles = -1
			route.error = "ERROR:AirmilesDataMissing"
		return airmiles

	def _extract_result_params(self, html):
		soup = BeautifulSoup(html)
		viewstate = soup.find(id='__VIEWSTATE').get('value')
		eventvalidation = soup.find(id='__EVENTVALIDATION').get('value')
		result_params = {'__VIEWSTATE': viewstate, '__EVENTVALIDATION': eventvalidation, 'hidSubmiting': 'true'}
		result_params = urllib.parse.urlencode(result_params)
		return result_params

	def _generate_route_search_params(self, route):
		params = dict(ScraperSession.SEARCH_PARAMS)
		params[ScraperSession.PARAM_NAME_DATE] = self.date_str
		params[ScraperSession.PARAM_NAME_ORIGIN_AIRPORT] = route.origin.code
		params[ScraperSession.PARAM_NAME_DEST_AIRPORT] = route.dest.code
		return params

	def __str__(self):
		return '{: >30}'.format(self.username if self.username else 'Anonymous')

	def _extract_airmiles_field(self, html, export_result_html=False):
		soup = BeautifulSoup(html)
		if export_result_html:
			with open('result.html', 'w') as f:
				f.write(soup.prettify())
		try:
			return int(re.findall(r'1 x ((?:\d{1,3},)?\d{3})', html)[0].replace(',',''))
		except IndexError:
			return None



class Results(object):
	"""Manages import, export and writing of results to CSV files.
	"""
	ORIGIN_AIRPORT_FIELD, DEST_AIRPORT_FIELD, AIRMILES_FIELD, DATE_FIELD, ERROR_FIELD = 0, 1, 2, 3, 4
	FIELD_NAMES = ("origin airport", "destination airport", "airmiles", "target date", "error")

	def __init__(self, filename, create_if_not_exists=True):
		self.filename = filename
		self.results_file = None
		if create_if_not_exists:
			if not os.path.isfile(self.filename):
				open(self.filename, 'a').close()

	def import_into(self, routes):
		"""Updates a list of routes from the raw results from a previous scrape.

			Sets airmiles, is_valid, and error fields of a route (if present in the list).
		"""
		routes_dict = {(r.origin.code,r.dest.code): r for r in routes}

		results = self._import_results()
		nmissing = 0
		for origin_code,dest_code,airmiles,date,error in results:
			key = (origin_code,dest_code)
			if key not in routes_dict:
				nmissing += 1
			else:
				routes_dict[key].set_scrape_data(airmiles, date, error)
		if nmissing > 0:
			logging.warning('Did not import {} scraped routes missing from the list of routes'.format(nmissing))

	def begin_write(self):
		self.results_file = open(self.filename, 'a', newline='')
		self.results_csv_writer = csv.writer(self.results_file, delimiter=',')

	def write(self, route, flush=True):
		route_details = route.as_list()
		self.results_csv_writer.writerow(route_details)
		if flush: self.results_file.flush()
		route_details_str = ', '.join(map(str, route_details))
		return route_details_str

	def end_write(self):
		if self.results_file:
			self.results_file.close()
			self.results_file = None

	def export_to(self, out_fname, field_to_sort_by=0, force_overwrite=False, add_reverse=True, source_routes=None):
		logging.info("Exporting from '{}' to '{}'".format(self.filename, out_fname))

		if not force_overwrite and os.path.isfile(out_fname):
			answer = input("'{}' already exists - overwrite? [y/N]:  ".format(out_fname))
			if answer.upper() != "Y":
				logging.info("Export aborted")
				return

		results = self._import_results()
		results = [r[:3] for r in results]

		if source_routes:
			results = self._append_unchecked_routes(results, source_routes)
		if add_reverse:
			results = self._append_reverse_routes(results)
		if field_to_sort_by != None:
			logging.info("Sorting by {}".format(Results.FIELD_NAMES[field_to_sort_by]))
			results = sorted(results, key=lambda r: (r[field_to_sort_by],)+tuple(r))

		with open(out_fname, 'w', newline='') as f:
			cw = csv.writer(f, delimiter=',')
			for result in results:
				cw.writerow(result)

		logging.info("Export complete")

	def _append_unchecked_routes(self, results, source_routes):
		logging.info("Appending unchecked routes")
		results_dict = {(r[0],r[1]): r for r in results}
		missing_results = []
		for r in source_routes:
			key, rev_key = (r.origin.code, r.dest.code), (r.dest.code, r.origin.code)
			if key not in results_dict.keys() and rev_key not in results_dict.keys():
				missing_results += [(r.origin.code, r.dest.code, '---')]
		logging.info("Appended {} unchecked routes".format(len(missing_results)))
		return results + missing_results


	def _append_reverse_routes(self, results):
		logging.info("Adding reverse routes (if they do not already exist)")
		results_dict = {(r[0],r[1]): r for r in results}
		nconflicts = 0
		conflicts = []
		for r in results:
			key, rev_key = (r[0],r[1]), (r[1],r[0])
			if key in results_dict:
				if rev_key in results_dict:
					airmiles = results_dict[key][Results.AIRMILES_FIELD]
					airmiles_reverse = results_dict[rev_key][Results.AIRMILES_FIELD]
					if airmiles != airmiles_reverse:
						nconflicts += 1
						conflicts += [(r[0], r[1], airmiles, airmiles_reverse)]
				else:
					results_dict[rev_key] = [r[1], r[0], r[2]]
		if nconflicts:
			logging.info("Found {} route mismatches:".format(nconflicts))
			for c in conflicts:
				logging.info("{}-{} = {} \t{}-{} = {}".format(c[0], c[1], c[2], c[1], c[0], c[3]))
		return results_dict.values()

	def _import_results(self, resolve_conflicts=True):
		results = []
		with open(self.filename, 'r') as f:
			reader = csv.reader(f)
			for row in reader:
				results += [row]

		if resolve_conflicts:
			results = self._resolve_conflicting_results(results)

		return results

	def _resolve_conflicting_results(self, results, verbose=True):
		results_dict = {}
		for r in results:
			key = (r[0], r[1])
			results_dict[key] = results_dict.get(key, []) + [r]
		final_results = []
		n_multi_results, nconflicts = 0, 0

		for route_key, results in results_dict.items():
			if len(results) > 1:
				n_multi_results += 1
				results_with_airmiles = [r for r in results if r[Results.AIRMILES_FIELD] != 'NA']
				n = len(results_with_airmiles)
				if n == 0:
					if len(set([r[Results.ERROR_FIELD] for r in results])) == 1:
						final_results += [results[0]]
					else:
						nconflicts += 1
						errors = [r[Results.ERROR_FIELD] for r in results]
						i = 0
						if "ERROR:NonExistent" in errors:
							i = errors.index("ERROR:NonExistent")
						elif "ERROR:SoldOutOrInvalid" in errors:
							i = errors.index("ERROR:SoldOutOrInvalid")
						final_results += [results[i]]
				elif n == 1:
					final_results += [results_with_airmiles[0]]
				else:
					if len(set([r[Results.AIRMILES_FIELD] for r in results_with_airmiles])) == 1:
						final_results += [results_with_airmiles[0]]
						continue
					else:
						nconflicts += 1
						if verbose:
							route = "{}-{}".format(*route_key)
							msg = "Found a route with multiple results and conflicting airmiles counts: {} -> {}".format(route, [r[Results.AIRMILES_FIELD] for r in results_with_airmiles])
							logging.warning(msg)
							logging.warning("Using result with largest airmile count")
						final_results += [max(results_with_airmiles, key=lambda r: int(r[Results.AIRMILES_FIELD]))]
			else:	# unique result
				final_results += [results[0]]

		if n_multi_results:
			logging.info("Multiple results for {} routes, including {} with different data.".format(n_multi_results, nconflicts))
			if nconflicts > 0:
				logging.info("Resolved conflicts")

		return final_results



def parse_args():
	parser = argparse.ArgumentParser(description='Scrape LifeMiles.com routes for airmiles')
	parser.add_argument('-d', '--retry-date', dest='date', action='store', default='',
						help='retries scan of failed routes for given date (MM-DD-YYYY)')
	parser.add_argument('-e', '--export', dest='filename', action='store',
						help='export results to file (CSV)')
	parser.add_argument('-s', '--sort', dest='sort_field_no', choices=range(3),
						action='store', default=0, type=int,
						help='field to sort by when exporting [0=origin (default), 1=dest, 2=airmiles]')
	parser.add_argument('-r', '--include-reverse', dest='add_reverse',
						action='store_const', const=True, default=False,
						help='include reverse routes in export (default: False)')
	parser.add_argument('-u', '--include-unchecked', dest='add_unchecked',
						action='store_const', const=True, default=False,
						help='include unchecked routes in export (default: False)')
	return parser.parse_args()

def set_up_loggers():
	root = logging.getLogger()
	root.setLevel(logging.INFO)

	logging.getLogger('requests').setLevel(logging.WARNING)

	ch = logging.StreamHandler(sys.stdout)
	ch.setLevel(logging.INFO)
	ch.setFormatter(logging.Formatter('-- %(message)s'))

	fh = logging.FileHandler('data/scraper.log')
	fh.setLevel(logging.INFO)
	fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

	root.addHandler(ch)
	root.addHandler(fh)


def generate_routes(airports_filename, sort_airports, include_reverse):
	rg = RouteGenerator(airports_filename, sort_airports=sort_airports)
	routes = rg.generate_all_routes(include_reverse=include_reverse)
	return list(routes)


def main(args):
	global _clean_up

	with open('config.toml') as f:
		config = toml.loads(f.read())
	airports_filename = config['files']['airports']
	raw_results_filename = config['files']['airmiles']
	sort_airports = config['search']['sort_airports']
	include_reverse = config['search']['reverse_routes']

	credentials = config['accounts'][:config['search']['max_accounts']]
	dummy_credential = {'username': '', 'password': ''}
	num_anon_sessions = config['search']['max_accounts'] - len(config['accounts'])
	num_avail_accounts = len(config['accounts'])
	credentials += [dummy_credential for _ in range(num_anon_sessions)]

	results_fname = config['files']['airmiles']
	date_offset = config['search']['date_offset']
	want_weekday = config['search']['want_weekday']
	order = config['search'].get('order', 'default')
	rescan_failed_routes = args.date != ''

	if args.filename:
		r = Results(raw_results_filename)
		routes = generate_routes(airports_filename, sort_airports, include_reverse) if args.add_unchecked else None
		r.export_to(args.filename, field_to_sort_by=args.sort_field_no, add_reverse=args.add_reverse, source_routes=routes)
		return

	print("Press Ctrl+C to terminate scrape\n")

	logging.info("Using {} of {} accounts".format(len(credentials), num_avail_accounts))
	if num_anon_sessions > 0:
		logging.info("Using an additional {} anonymous sessions.".format(num_anon_sessions))


	routes = generate_routes(airports_filename, sort_airports, include_reverse)
	
	s = Scraper(credentials, routes, results_fname,
			    date_offset=date_offset, want_weekday=want_weekday)
	_clean_up = s.stop

	s.import_scraped_routes()

	if not rescan_failed_routes:
		if order == 'prioritised':
			s.prioritise_routes_by_success()
		elif order == 'random':
			s.randomise_routes()

		s.start()
	else:
		s.rescan(args.date)




if __name__ == "__main__":
	args = parse_args()
	signal.signal(signal.SIGINT, _signal_handler)
	set_up_loggers()
	main(args)
