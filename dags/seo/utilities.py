from datetime import datetime
from plugins.utilities.slack_service import success_alert


def seo_trending_slack_alert(context):
    current_time = datetime.now()
    if current_time.minute > 0 and current_time.minute < 10:
        success_alert(context)
