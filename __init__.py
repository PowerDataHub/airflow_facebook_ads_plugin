from airflow.plugins_manager import AirflowPlugin
from facebook_ads_plugin.hooks.facebook_ads_hook import FacebookAdsHook
from facebook_ads_plugin.operators.facebook_ads_to_s3_operator import (
    FacebookAdsInsightsToS3Operator,
    FacebookAdsToS3Operator,
)


class FacebookAdsPlugin(AirflowPlugin):
    name = "facebook_ads_plugin"
    hooks = [FacebookAdsHook]
    operators = [FacebookAdsInsightsToS3Operator, FacebookAdsToS3Operator]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
