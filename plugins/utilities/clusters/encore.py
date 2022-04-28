def get_existing_dbt_cluster(env):
    if env == 'dev':
        existing_cluster = "0325-143144-6i07pld2"
        return existing_cluster
    elif env == 'staging':
        existing_cluster = "0329-152309-i8nbyfr4"
        return existing_cluster
    elif env == 'production':
        existing_cluster = "0329-152309-5vur4c9"
        return existing_cluster
