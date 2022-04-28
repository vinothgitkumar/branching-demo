from plugins.utilities.vault import vault_instance


def getDbtToken():
    return vault_instance.get_secret("dbt_cloud_api_token")


def getDbtAccntId():
    return vault_instance.get_secret("dbt_cloud_account_id")


def getDbtMessage(message):
    return {'cause': message}


def getDbtApiLink(jobId, accountId):
    return 'accounts/{0}/jobs/{1}/run/'.format(accountId, jobId)


def getDbtApiLinkforRun(accountId):
    return 'accounts/{0}/runs/'.format(accountId)


def getDbtJobId(projectname, env):
    return vault_instance.get_secret("dbt_cloud_{0}_{1}_job_id".format(projectname, env))
