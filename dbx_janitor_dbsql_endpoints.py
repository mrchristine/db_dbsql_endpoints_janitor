from dbclient import *
# python 3.6


def cleanup_sql_endpoints(url, token, env_name):
    sql_client = SQLAnalyticsClient(token, url)
    report = dict()
    report['env_name'] = (env_name, url)
    print("Creating shared endpoints ...")
    sql_client.create_shared_endpoints()
    print("Deleting stopped endpoints ...")
    del_endpoints = sql_client.delete_stopped_endpoints()
    report['endpoints'] = del_endpoints
    return report


def lambda_handler(event, context):
    # get a list of configurations in json format
    configs = get_job_configs()
    envs = {}
    log_bucket = []
    for env in configs:
        envs[env['desc']] = (env['url'], env['token'])
        log_bucket.append(env['s3_bucket'])

    # logging configuration
    bucket_name = log_bucket[0]
    table_name = "dbsql_endpoint_usage_logs"

    full_report = ""
    html_report = ""
    for x, y in envs.items():
        if x == 'Interview Workspace':
            continue
        print("Env name to cleanup sql endpoints: {0}".format(y[0]))
        report = cleanup_sql_endpoints(y[0], y[1], x)
        # log report to S3
        log_to_s3(bucket_name, table_name, report)
        full_report += pprint_j(report)
        full_report += "\n######################################################\n"
        html_report += get_html(report)

    print(full_report)
    email_list = ["mwc@databricks.com"]
    send_email("Databricks Automated Endpoints Usage Report", email_list, full_report, html_report)
    # Print Spark Versions
    message = "Completed running cleanup across all field environments!"
    return {
        'message': message
    }

