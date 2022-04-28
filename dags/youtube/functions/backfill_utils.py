import json
from airflow.utils import timezone
from datetime import datetime, timedelta
from plugins.config import YoutubeGlobal as Config
from airflow.api.common.experimental.trigger_dag import trigger_dag
import ast


def get_list_dates(start_date, end_date):
    start_date = parse_date(start_date)
    end_date = parse_date(end_date)
    delta = end_date - start_date
    date_list = []
    for i in range(delta.days + 1):
        day = start_date + timedelta(days=i)
        date_list.append(day.strftime("%Y-%m-%d"))
    return date_list


def get_date_chunks(date_list, n):
    new_list = [date_list[i:i + n] for i in range(0, len(date_list), n)]
    return new_list


def validate_market_and_report(params):
    market = params.get('market')
    if market is None or market not in Config.search_config.keys():
        raise Exception("Invalid market")

    report_name = params.get('report_name')
    if report_name is None or (report_name not in Config.reports and report_name not in Config.old_reports):
        raise Exception("Invalid Report Name")
    print("Market and report name is validated Successfully!!")


def parse_date(date_text):
    try:
        return datetime.strptime(date_text, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Incorrect data format, should be YYYY-MM-DD :", date_text)


def validate_date(params):
    start_date = params.get('start_date')
    end_date = params.get('end_date')

    p_start_date = parse_date(start_date)
    p_end_date = parse_date(end_date)

    if p_start_date > p_end_date:
        raise Exception("Start date must be before End date")
    print("Start_date and End_date validated Successfully!!")


def validate_params(**params):
    validate_market_and_report(params)
    validate_date(params)


def get_from_and_to_dates(chunks):
    chunck_dict = {}
    for i in range(0, len(chunks)):
        from_date = chunks[i][0]
        to_date = chunks[i][-1]
        chunck_dict["cycle_" + str(i + 1)] = str([from_date, to_date])
    return chunck_dict


def trigger_backfill_dag(date_chunks, market, report_name, vault_token):
    chunck_dict = get_from_and_to_dates(date_chunks)
    for cycle in chunck_dict.keys():
        run_id = f"{report_name}_{market}_{chunck_dict[cycle]}_{timezone.utcnow().isoformat()}"
        print("<The Run_id is>")
        print(run_id)
        dag_inputs = {'market': market,
                      'report_name': report_name,
                      'load_dates': chunck_dict[cycle],
                      'vault_string': vault_token}
        trigger_dag(dag_id="youtube_global_task_trigger",
                    run_id=run_id,
                    conf=json.dumps(dag_inputs),
                    replace_microseconds=False)
        print(
            f"Successfully triggered dag for {report_name} for ranges of dates {chunck_dict[cycle]} "
            f"and run_id = {run_id}")


def trigger_databricks_job(**params):
    start_date = params.get('start_date')
    end_date = params.get('end_date')
    market = params.get('market')
    report_name = params.get('report_name')
    vault_token = params.get('vault_token')

    date_list = get_list_dates(start_date, end_date)
    date_chunks = get_date_chunks(date_list, Config.backfill_date_range)
    print(f"Triggering Backfill databricks dag for report:{report_name} "
          f"from Start_date:{start_date} to End_date:{end_date}")

    # initialise_tardis_status(report_name, start_date, end_date)
    trigger_backfill_dag(date_chunks, market, report_name, vault_token)


def check_reports(**context):
    report_name = context['report_name']
    saturation_date = parse_date(Config.transfer_actual_date)
    dates = ast.literal_eval(context['load_dates'])
    start_date = dates[0]
    if str(report_name) in Config.old_reports:
        if str(report_name) in Config.reports and context['market'] == 'all':
            return "submit_backfill_databricks_run"
        else:
            if parse_date(start_date) >= saturation_date:
                print("NEW REPORT BACKFILL")
                return "submit_backfill_databricks_run"
            else:
                print("OLD REPORT MISSING DATA")
                return "submit_missing_data_databricks_run"
    elif str(report_name) in Config.reports:
        print("NEW REPORT BACKFILL")
        return "submit_backfill_databricks_run"
