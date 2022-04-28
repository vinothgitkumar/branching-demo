from plugins.utilities.tardis_utils import update_tardis
from plugins.utilities.slack_service import success_alert, failure_alert


def update_tardis_status(post_type, source, status, comments, context=None):
    logdate = context['ds']
    update_tardis(post_type=post_type,
                  tardis_source=source,
                  logdate=logdate,
                  status=status,
                  comments=comments
                  )
    if status == 'FAILED':
        failure_alert(context)
    elif status == 'COMPLETE':
        success_alert(context)
