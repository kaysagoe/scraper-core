import logging
import backoff

from singer_sdk.streams import Stream
from typing import Any, Dict, Optional, Union, List, Iterable, Callable
from selenium.webdriver import Chrome
from selenium.webdriver.chrome.options import Options
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from bs4 import BeautifulSoup

DEFAULT_REQUEST_TIMEOUT = 300

class ScraperStream(Stream):
    url_base = ""
    _driver = None

    @property
    def driver(self):
        if not self._driver:
            options = Options()
            options.add_argument('--headless')
            options.add_argument('--ignore-ssl-errors=yes')
            options.add_argument('--ignore-certificate-errors')
            self._driver = Chrome(options=options)
        return self._driver

    @property
    def timeout(self) -> int:
        """Return the request timeout limit in seconds.

        The default timeout is 300 seconds, or as defined by DEFAULT_REQUEST_TIMEOUT.

        Returns:
            The request timeout limit as number of seconds.
        """
        return DEFAULT_REQUEST_TIMEOUT

    def get_url_params(
        self, context: Optional[dict]
    ):
        """Return a dictionary of values to be used in URL parameterization.

        If paging is supported, developers may override with specific paging logic.

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token, page number or any request argument to request the
                next page of data.

        Returns:
            Dictionary of URL query parameters to use in the request.
        """
        return {}

    def get_url(
        self, context: Optional[dict]
    ):
        params= self.get_url_params(context)
        params_string = '&'.join([f'{key}={value}' for key, value in params.items()])
        if params_string:
            params_string = f'?{params_string}'
        url = "".join([self.url_base, self.path or "", params_string])
        
        return url

    def validate_response(self, response: BeautifulSoup) -> None:
        """Validate HTTP response.

        By default, checks for error status codes (>400) and raises a
        :class:`singer_sdk.exceptions.FatalAPIError`.

        Tap developers are encouraged to override this method if their APIs use HTTP
        status codes in non-conventional ways, or if they communicate errors
        differently (e.g. in the response body).

        .. image:: ../images/200.png


        In case an error is deemed transient and can be safely retried, then this
        method should raise an :class:`singer_sdk.exceptions.RetriableAPIError`.

        Args:
            response: A `requests.Response`_ object.

        Raises:
            FatalAPIError: If the request is not retriable.
            RetriableAPIError: If the request is retriable.

        .. _requests.Response:
            https://docs.python-requests.org/en/latest/api/#requests.Response
        """
        pass

    def request_decorator(self, func: Callable) -> Callable:
        """Instantiate a decorator for handling request failures.

        Developers may override this method to provide custom backoff or retry
        handling.

        Args:
            func: Function to decorate.

        Returns:
            A decorated method.
        """
        decorator: Callable = backoff.on_exception(
            backoff.expo,
            (
                RetriableAPIError
            ),
            max_tries=5,
            factor=2,
        )(func)
        return decorator

    def _request(
        self, url: str, context: Optional[dict]
    ) -> BeautifulSoup:
        self.driver.set_page_load_timeout(self.timeout)
        self.driver.get(url)
        self._agree_cookies()
        response = BeautifulSoup(self.driver.page_source, 'html.parser')
        self.validate_response(response)
        logging.debug("Response received successfully.")
        return response
    
    def _agree_cookies(self) -> None:
        pass

    def parse_response(self, response: BeautifulSoup) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows.

        Args:
            response: A raw `requests.Response`_ object.

        Yields:
            One item for every item found in the response.

        .. _requests.Response:
            https://docs.python-requests.org/en/latest/api/#requests.Response
        """
        return {}


    def has_next_page(self) -> bool:
        return False

    def go_to_next_page(self) -> None:
        pass

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.
        """
        finished = False
        url = self.get_url(context,)
        resp = self._request(url, context)

        while not finished:
            for row in self.parse_response(resp):
                yield row
            finished = not self.has_next_page()
            if not finished:
                self.go_to_next_page()
                resp = BeautifulSoup(self.driver.page_source, 'html.parser')
                self.validate_response(resp) 