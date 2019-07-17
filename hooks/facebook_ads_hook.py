""" Facebook Ads Hook """

from urllib.parse import urlencode
from airflow.hooks.base_hook import BaseHook
import requests
import time
import logging


class FacebookAdsHook(BaseHook):
    def __init__(self, facebook_ads_conn_id, access_token):
        self.facebook_ads_conn_id = facebook_ads_conn_id
        self.connection = self.get_connection(facebook_ads_conn_id)

        self.base_uri = "https://graph.facebook.com"
        self.api_version = self.connection.extra_dejson["api_version"] or "3.2"
        self.access_token = access_token

        super().__init__(self)

    @staticmethod
    def parse_result(response):
        response.raise_for_status()
        response_body = response.json()

        logging.info("Response Headers: {}".format(response.headers))
        logging.info("Response Body: {}".format(response.json()))

        return_value = []
        return_value.extend(response_body["data"])

        while "next" in response_body.get("paging", {}):
            logging.info("Paginated result!")
            time.sleep(3)
            response = requests.get(response_body["paging"]["next"])
            response.raise_for_status()
            response_body = response.json()

            logging.info("Response Headers: {}".format(response.headers))

            return_value.extend(response_body["data"])

        return return_value

    def insights(
        self,
        account_id,
        insight_fields,
        breakdowns,
        time_range,
        time_increment="all_days",
        level="ad",
        limit=100,
    ):
        """
        Get insights from Facebook API.

        :param account_id:      Facebook account id.
        :type account_id:       string
        :param insight_fields:  List of fields.
        :type insight_fields:   list
        :param breakdowns:      List of breakdown fields.
        :type breakdowns:       list
        :param time_range:      A single time range object. UNIX timestamp not supported.
                                This param is ignored if time_ranges is provided.
        :type time_range:       {'since':YYYY-MM-DD,'until':YYYY-MM-DD}
        :param time_increment:  Default value: all_days
                                If it is an integer, it is the number of days from 1 to 90.
                                After you pick a reporting period by using time_range or date_preset, you may choose to
                                have the results for the whole period, or have results for smaller time slices.
                                If "all_days" is used, it means one result set for the whole period.
                                If "monthly" is used, you will get one result set for each calendar month in the given period.
                                Or you can have one result set for each N-day period specified by this param.
                                This param is ignored if time_ranges is specified.
        :type time_increment:   string
        :param level:           Represents the level of result (ad, adset, campaign, account).
        :type level:            string
        :param limit:           Items returned.
        :type limit:            int
        """
        payload = urlencode(
            {
                "access_token": self.access_token,
                "breakdowns": ",".join(breakdowns),
                "fields": ",".join(insight_fields),
                "time_range": time_range,
                "time_increment": time_increment,
                "level": level,
                "limit": limit,
            }
        )

        self.log.info(
            "API CALL: {base_uri}/v{api_version}/{account_id}/insights?{payload}".format(
                base_uri=self.base_uri,
                api_version=self.api_version,
                account_id=account_id,
                payload=payload,
            )
        )

        response = requests.get(
            "{base_uri}/v{api_version}/{account_id}/insights?{payload}".format(
                base_uri=self.base_uri,
                api_version=self.api_version,
                account_id=account_id,
                payload=payload,
            )
        )

        return self.parse_result(response)

    def campaigns(self, account_id, fields, limit=100):

        payload = urlencode(
            {
                "access_token": self.access_token,
                "fields": ",".join(fields),
                "limit": limit,
            }
        )

        self.log.info(
            "API CALL: {base_uri}/v{api_version}/{account_id}/campaigns?{payload}".format(
                base_uri=self.base_uri,
                api_version=self.api_version,
                account_id=account_id,
                payload=payload,
            )
        )

        response = requests.get(
            "{base_uri}/v{api_version}/{account_id}/campaigns?{payload}".format(
                base_uri=self.base_uri,
                api_version=self.api_version,
                account_id=account_id,
                payload=payload,
            )
        )

        return self.parse_result(response)

    def ads(self, account_id, fields, limit=100):

        payload = urlencode(
            {
                "access_token": self.access_token,
                "fields": ",".join(fields),
                "limit": limit,
            }
        )

        self.log.info(
            "API CALL: {base_uri}/v{api_version}/{account_id}/ads?{payload}".format(
                base_uri=self.base_uri,
                api_version=self.api_version,
                account_id=account_id,
                payload=payload,
            )
        )

        response = requests.get(
            "{base_uri}/v{api_version}/{account_id}/ads?{payload}".format(
                base_uri=self.base_uri,
                api_version=self.api_version,
                account_id=account_id,
                payload=payload,
            )
        )

        return self.parse_result(response)

    def adsets(self, account_id, fields, limit=100):

        payload = urlencode(
            {
                "access_token": self.access_token,
                "fields": ",".join(fields),
                "limit": limit,
            }
        )

        self.log.info(
            "API CALL: {base_uri}/v{api_version}/{account_id}/adsets?{payload}".format(
                base_uri=self.base_uri,
                api_version=self.api_version,
                account_id=account_id,
                payload=payload,
            )
        )

        response = requests.get(
            "{base_uri}/v{api_version}/{account_id}/adsets?{payload}".format(
                base_uri=self.base_uri,
                api_version=self.api_version,
                account_id=account_id,
                payload=payload,
            )
        )

        return self.parse_result(response)

    def adcreatives(self, account_id, fields, limit=100):

        payload = urlencode(
            {
                "access_token": self.access_token,
                "fields": ",".join(fields),
                "limit": limit,
            }
        )

        self.log.info(
            "API CALL: {base_uri}/v{api_version}/{account_id}/adcreatives?{payload}".format(
                base_uri=self.base_uri,
                api_version=self.api_version,
                account_id=account_id,
                payload=payload,
            )
        )

        response = requests.get(
            "{base_uri}/v{api_version}/{account_id}/adcreatives?{payload}".format(
                base_uri=self.base_uri,
                api_version=self.api_version,
                account_id=account_id,
                payload=payload,
            )
        )

        return self.parse_result(response)
