import click

@click.group(help='Command group for configuring runs with airflow')
def airflow():
    pass

@click.command(help='Create dags for usage with airflow')
def make_dags():
    import carrot.workflows.airflow as airflow_helpers
    reports = airflow_helpers.get_reports()
    for report_id,report_name in reports.items():
        click.echo(f"creating {report_id} {report_name}")
        airflow_helpers.create_template(report_name,report_id)

@click.command(help='Create dags for usage with airflow')
def trigger_dags():
    import carrot.workflows.airflow as airflow_helpers
    airflow_helpers.trigger_etl_dags()
    

airflow.add_command(make_dags,'make_dags')
airflow.add_command(trigger_dags,'trigger_dags')



