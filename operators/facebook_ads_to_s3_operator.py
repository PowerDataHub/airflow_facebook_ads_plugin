import json
import os
import uuid
import logging


from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator

from facebook_ads_plugin.hooks.facebook_ads_hook import FacebookAdsHook


class FacebookAdsInsightsToS3Operator(BaseOperator):
    """
    Facebook Ads Insights To S3 Operator

    :param facebook_conn_id:        The source facebook connection id.
    :type facebook_conn_id:         string
    :param aws_conn_id:              The destination s3 connection id.
    :type aws_conn_id:               string
    :param s3_bucket:               The destination s3 bucket.
    :type s3_bucket:                string
    :param s3_key:                  The destination s3 key.
    :type s3_key:                   string
    :param account_ids:             An array of Facebook Ad Account Ids strings which
                                    own campaigns, ad_sets, and ads.
    :type account_ids:              list
    :param insight_fields:          An array of insight field strings to get back from
                                    the API.  Defaults to an empty array.
    :type insight_fields:           list
    :param breakdowns:              An array of breakdown strings for which to group insights.abs
                                    Defaults to an empty array.
    :type breakdowns:               list
    :param since:                   A datetime representing the start time to get Facebook data.
                                    Can use Airflow template for execution_date
    :type since:                    datetime
    :param until:                   A datetime representing the end time to get Facebook data.
                                    Can use Airflow template for next_execution_date
    :type until:                    datetime
    :param time_increment:          A string representing the time increment for which to get data,
                                    described by the Facebook Ads API. Defaults to 'all_days'.
    :type time_increment:           string
    :param level:                   A string representing the level for which to get Facebook Ads data,
                                    can be campaign, ad_set, or ad level.  Defaults to 'ad'.
    :type level:                    string
    :param limit:                   The number of records to fetch in each request. Defaults to 100.
    :type limit:                    integer
    """

    template_fields = ("s3_key", "s3_bucket", "since", "until")

    def __init__(
        self,
        access_token,
        facebook_conn_id,
        aws_conn_id,
        s3_bucket,
        s3_key,
        account_ids,
        insight_fields,
        breakdowns,
        since,
        until,
        time_increment="all_days",
        level="ad",
        limit=100,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.access_token = access_token
        self.facebook_conn_id = facebook_conn_id
        self.aws_conn_id = aws_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.account_ids = account_ids
        self.insight_fields = insight_fields
        self.breakdowns = breakdowns
        self.since = since
        self.until = until
        self.time_increment = time_increment
        self.level = level
        self.limit = limit

    def execute(self, context):
        facebook_conn = FacebookAdsHook(self.access_token, self.facebook_conn_id)
        s3_conn = S3Hook(self.aws_conn_id)

        logging.info("Fetch API since " + str(self.since))
        logging.info("Fetch API until " + str(self.until))

        logging.info("Breakdowns " + str(self.breakdowns))
        logging.info("Fields " + str(self.insight_fields))

        time_range = {"since": self.since, "until": self.until}

        file_name = "/tmp/{key}.jsonl".format(key=uuid.uuid4().hex)

        with open(file_name, "w") as insight_file:
            for account_id in self.account_ids:
                insights = facebook_conn.insights(
                    account_id,
                    self.insight_fields,
                    self.breakdowns,
                    time_range,
                    self.time_increment,
                    self.level,
                    self.limit,
                )

                if len(insights) > 0:
                    for insight in insights:
                        insight_file.write(json.dumps(insight) + "\n")

        s3_conn.load_file(file_name, self.s3_key, self.s3_bucket, True)
        os.remove(file_name)


class FacebookAdsToS3Operator(BaseOperator):
    """
    Facebook Ads Insights To S3 Operator
    :param facebook_conn_id:        The source facebook connection id.
    :type facebook_conn_id:         string
    :param aws_conn_id:             The destination s3 connection id.
    :type aws_conn_id:              string
    :param s3_bucket:               The destination s3 bucket.
    :type s3_bucket:                string
    :param s3_key:                  The destination s3 key.
    :type s3_key:                   string
    :param s3_key:                  An array of Facebook Ad Account Ids strings which
                                    own campaigns, ad_sets, and ads.
    :type s3_key:                   array
    :param account_ids:             An array of insight field strings to get back from
                                    the API.  Defaults to an empty array.
    :type account_ids:              array
    :param object_type:             Type of object returned: ad, adset, campaign, adgroup.
    :type object_type:              collections.Iterable
    :param breakdowns:              An array of breakdown strings for which to group insights.abs
                                    Defaults to an empty array.
    :type breakdowns:               array
    :param since:                   A datetime representing the start time to get Facebook data.
                                    Can use Airflow template for execution_date
    :type since:                    datetime
    :param until:                   A datetime representing the end time to get Facebook data.
                                    Can use Airflow template for next_execution_date
    :type until:                    datetime
    :param time_increment:          A string representing the time increment for which to get data,
                                    described by the Facebook Ads API. Defaults to 'all_days'.
    :type time_increment:           string
    :param level:                   A string representing the level for which to get Facebook Ads data,
                                    can be campaign, ad_set, or ad level.  Defaults to 'ad'.
    :type level:                    string
    :param limit:                   The number of records to fetch in each request. Defaults to 100.
    :type limit:                    integer
    """

    template_fields = ("s3_key", "s3_bucket", "s3_key")

    def __init__(
        self,
        access_token,
        facebook_conn_id,
        aws_conn_id,
        s3_bucket,
        s3_key,
        account_ids,
        fields,
        limit,
        object_type="ad",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        self.access_token = access_token
        self.facebook_conn_id = facebook_conn_id
        self.aws_conn_id = aws_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.account_ids = account_ids
        self.object_type = object_type
        self.fields = fields
        self.limit = limit

    def execute(self, context):
        facebook_conn = FacebookAdsHook(self.access_token, self.facebook_conn_id)
        s3_conn = S3Hook(self.aws_conn_id)

        logging.info("Fields " + str(self.fields))

        file_name = "/tmp/{key}.jsonl".format(key=uuid.uuid4().hex)

        with open(file_name, "w") as file:
            for account_id in self.account_ids:
                if self.object_type == "campaign":
                    result = facebook_conn.campaigns(account_id, self.fields, self.limit)
                elif self.object_type == "ad":
                    result = facebook_conn.ads(account_id, self.fields, self.limit)
                elif self.object_type == "adset":
                    result = facebook_conn.adsets(account_id, self.fields, self.limit)

                if len(result) > 0:
                    for item in result:
                        file.write(json.dumps(item) + "\n")

        s3_conn.load_file(file_name, self.s3_key, self.s3_bucket, True)
        os.remove(file_name)
