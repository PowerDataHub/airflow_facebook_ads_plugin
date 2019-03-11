from urllib.parse import urlencode
from airflow.hooks.base_hook import BaseHook
import requests
import time
import logging


class FacebookAdsHook(BaseHook):
    def __init__(
        self, access_token, source, facebook_ads_conn_id="facebook_ads_default"
    ):
        super().__init__(source)
        self.facebook_ads_conn_id = facebook_ads_conn_id
        self.connection = self.get_connection(facebook_ads_conn_id)

        self.base_uri = "https://graph.facebook.com"
        self.api_version = self.connection.extra_dejson["api_version"] or "3.2"
        self.access_token = access_token

    @staticmethod
    def parse_result(response):
        response.raise_for_status()
        response_body = response.json()
        return_value = []

        while "next" in response_body.get("paging", {}):
            time.sleep(2)
            return_value.extend(response_body["data"])
            response = requests.get(response_body["paging"]["next"])
            response.raise_for_status()
            response_body = response.json()

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

        logging.info(
            "API CALL: {base_uri}/v{api_version}/act_{account_id}/insights?{payload}".format(
                base_uri=self.base_uri,
                api_version=self.api_version,
                account_id=account_id,
                payload=payload,
            )
        )

        response = requests.get(
            "{base_uri}/v{api_version}/act_{account_id}/insights?{payload}".format(
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

        logging.info(
            "API CALL: {base_uri}/v{api_version}/act_{account_id}/campaigns?{payload}".format(
                base_uri=self.base_uri,
                api_version=self.api_version,
                account_id=account_id,
                payload=payload,
            )
        )

        response = requests.get(
            "{base_uri}/v{api_version}/act_{account_id}/campaigns?{payload}".format(
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

        logging.info(
            "API CALL: {base_uri}/v{api_version}/act_{account_id}/ads?{payload}".format(
                base_uri=self.base_uri,
                api_version=self.api_version,
                account_id=account_id,
                payload=payload,
            )
        )

        response = requests.get(
            "{base_uri}/v{api_version}/act_{account_id}/ads?{payload}".format(
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

        logging.info(
            "API CALL: {base_uri}/v{api_version}/act_{account_id}/adsets?{payload}".format(
                base_uri=self.base_uri,
                api_version=self.api_version,
                account_id=account_id,
                payload=payload,
            )
        )

        response = requests.get(
            "{base_uri}/v{api_version}/act_{account_id}/adsets?{payload}".format(
                base_uri=self.base_uri,
                api_version=self.api_version,
                account_id=account_id,
                payload=payload,
            )
        )

        return self.parse_result(response)
