import dagfactory
import os

AIRFLOW_HOME: str       = os.getenv('AIRFLOW_HOME') + '/dags/bi_dags/'
yml_lst = os.listdir(AIRFLOW_HOME)
yml_dag_path_lst = [os.path.join(AIRFLOW_HOME, yml) for yml in yml_lst if yml.endswith('.yml')]
dag_factory_list = [dagfactory.DagFactory(yml).generate_dags(globals()) for yml in yml_dag_path_lst]


