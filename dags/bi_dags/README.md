# Airflow dag creation by YML

## Description

- Под капотом работает модуль [dag-factory](https://github.com/astronomer/dag-factory/blob/main/README.md)
- Запуск методов модуля воспроизводится в файле [dag_factory_runner](https://gitlab.corp.mail.ru/HR/airflow-dags-plugins/-/blomaster/dags/dag_factory_runner.py?ref_type=heads)
- Все yml-файлы с дагами хрянятся в папке [bi_dags](https://gitlab.corp.mail.ru/HR/airflow-dags-plugins/-/tree/master/dags/bi_dags?ref_type=heads)
- Один даг может включать в себя несколько бизнес-сущностей, может создаваться последовательность обновления необходимых таблиц
- **yml-файл = Airflow Dag**
- В случае, если DAG не появляется в веб-интерфейсе Airflow, не нужно редактировать dag_dactory_runner. Источник проблемы: 
    - некорректное заполнение yml-файла
    - закоммиченное изменение еще не успело вступить в силу на стороне PROD-машины

## How-to Guide
**.yml** - это тот же формат json-словаря, только в человекочитаемом виде.
1. ### Каждый yml-файл должен начинаться с defaults - параметры по умолчанию при создании. Они не должны меняються! На данный момент не удалось вынести их в отдельный файл, поэтому их просто копируем в каждый yml-файл
    ```yaml
    default: #определение заголовка default
        default_args: #определение параметров default_args
            email_on_failure: False #параметр отправки email в случае падения задачи
            email_on_retry:  False #параметр отправки email в случае перезапуска задачи
            retries: 0 #параметр на количество повторений задачи
            depends_on_past: False #параметр зависимости задачи внутри дага от статуса этой же задачи в прошлом
            on_failure_callback: #параметр при падении дага
            callback: globals.globals.on_failure_callback #запуск функции, если даг упал
        catchup: False #запуск дагов за предыдущий период
        max_active_runs: 1  #максимальное количество активных запусков дага
    ```

2. ### Далее делаем отступ от defaults и прописываем название дага
    - Называем даг согласно бизнес-контексту. Если даг обновляет витрины под конкретный дашборд, можно назвать даг по типу dashboard_name_processing/dds_recruitment_processing
    Пример:
    ```yaml
    dds_sap_bu_manager_rls: 
    ```

3. ### Далее снова указываем default_args, чтобы передать дополнительные параметры: дату начала старта дага и имя владельца.
    1. После этого указываем параметр периодичности запуска дага: 
        - Если мы хотим, чтобы даг запускался каждый день, тогда можно указать @daily. Примечание: если указать только start_date и schedule_interval: @daily, в таком случае каждый день даг будет запускаться по дефолтному времени 3 часа ночи по мск (UTC +3)
        - Если мы хотим указать конкретное время запуска дага на каждый день, можно это сделать по такому параметру - '30 4 * * *' - даг будет запускаться в 7.30 утра. Подробнее инфу можно найти [здесь](https://airflow.apache.org/docs/apache-airflow/1.10.1/scheduler.html). Или можно посмотреть, как реализован это параметр, в других дагах в рамках нашего репо.
    2. Указываем тэг проекта - к какому источнику/бизнес-сущности относится даг (sap, huntflow, recruitment, internal communication)

    ```yaml
    dds_sap_bu_manager_rls: #название дага
        default_args:
            start_date: 2025-02-17 #дата создания дага
            owner: s.muratov #владелец
        schedule_interval: '@daily' #периодичность
        tags: [sap, rls] #тэг
    ```

4. ### Переходим к созданию последовательности тасок (и если необходимо - Task Groups)
    - Последовательность тасок создается внутри параметра tasks 
    - Создается название таски -> далее указываются ее оператор, ее аргументы, если есть, а также группу, к которой она относится (если есть) -> устанавливаем зависимость между тасками
    - Внутри групп указываем зависимость от тасок/групп
    1. Даг всегда начинается с таски **start_dag** и заканчивается **end_dag**. Оператор всегда пишется **по-дефолту!** *airflow.operators.dummy_operator.DummyOperator*
    2. После start_dag идет идет последовательность тасок, в которых применяется *airflow.operators.python.PythonOperator* **по-дефолту!**. Это означает, что в параметрах **operator**, **python_callable_name** и **python_callable_file** всегда будут одни и те же значения (*airflow.operators.python.PythonOperator*, *sql_script_execution* и */opt/airflow/dags/operators/common/pg_operator.py* соответственно)
    3. В параметре op_kwargs всегда **по-дефолту!** всегда будет **pg_hook_con: $ENV**. Остальные параметры:
        -  **directory **- путь до папки проекта, где лежит скрипт для таблицы.**/opt/airflow/dags/sql/** всегда будет по деволту, останется к нему добавить необходимую папку проекта (в примере - other/)
    4. **schema_folder** - одновременно и папка и схема в dwh, где лежит скрипт/таблица
    5. **table_name** - название таблицы без схемы
    6. **dependencies** - от какого таска зависит текущий таск. Можно прописывать через [] или - (см. пример ниже). Если таска зависит от выполнения нескольких предыдущих тасок, тогда указываем все зависимвые таски **[task1, task_2 и тд]**
    7. Если таска должна быть в группе тасок, тогда указываем параметр пренадлежности к группе - **task_group_name**
    Пример без группы из тасок:
    ```yaml
      tasks: #декларирование тасок
        start_dag: #название таски
            operator: airflow.operators.dummy_operator.DummyOperator #default, пустой оператор
        dds.sap_bu_manager_rls: #название таски
            operator: airflow.operators.python.PythonOperator #default, питоновский оператор
            python_callable_name: sql_script_execution # default, питоновская функция, которая читает и выполянет sql-скрипты
            python_callable_file: /opt/airflow/dags/operators/common/pg_operator.py #default, путь питоновского файла с кодом функции
            op_kwargs: #default, аргументы для выполнения функции
                pg_hook_con: $ENV #default, коннектор 
                directory: /opt/airflow/dags/sql/other/ #указываем путь до папки скрипта, /opt/airflow/dags/sql/ идут as default
                schema_folder: dds #схема и одновременно и папка, где лежит скрипт
                table_name: sap_bu_manager_rls # одновременно название таблицы в dwh и скрипта
            dependencies: [start_dag] #зависимость от предыдущей таски 
        end_dag:
            operator: airflow.operators.dummy_operator.DummyOperator
            dependencies: 
                - dds.sap_bu_manager_rls
    ```
    Пример с TaskGroup:
    ```yaml
        task_groups:
            task_group_v1:
                dependencies: [start_dag] #зависимость группы от предыдущей таски
            task_group_v2:
                dependencies: [task_group_v1] #зависимость группы от предыдущей таски
        tasks:
            start_dag:
                operator: airflow.operators.dummy_operator.DummyOperator
            get_value_v1: 
                operator: airflow.operators.python.PythonOperator
                python_callable_name: _get_value
                python_callable_file: /opt/airflow/dags/operators/example_task.py
                task_group_name: task_group_v1 #принадлежность таски к группе
            get_value_v2:
                operator: airflow.operators.python.PythonOperator
                python_callable_name: _get_value
                python_callable_file: /opt/airflow/dags/operators/example_task.py
                task_group_name: task_group_v1 #принадлежность таски к группе
            get_value_group_v:
                operator: airflow.operators.python.PythonOperator
                python_callable_name: get_next_value
                python_callable_file: /opt/airflow/dags/operators/example_task.py
                task_group_name: task_group_v2 #принадлежность таски к группе
            end_dag:
                operator: airflow.operators.dummy_operator.DummyOperator
                dependencies: [get_value_group_v] #зависимость таски от группы
    ```
5. ### Сохраняем файл и коммитим изменения в PROD
6. ### Находим даг в UI Airflow
7. ### Пользуемся дагом!

## Полные примеры yml-дагов
- dds_sap_bu_manager_rls - Пример с PROD
    ```yaml
    default:
        default_args:
            email_on_failure: False
            email_on_retry:  False
            retries: 0
            depends_on_past: False
            on_failure_callback: 
            callback: globals.globals.on_failure_callback
        catchup: False
        max_active_runs: 1  

    dds_sap_bu_manager_rls:
        default_args:
            start_date: 2025-02-17
            owner: n.muratov
        schedule_interval: '@daily'
        tags: [sap, rls]
        tasks:
            start_dag:
                operator: airflow.operators.dummy_operator.DummyOperator
            dds.sap_bu_manager_rls:
                operator: airflow.operators.python.PythonOperator
                python_callable_name: sql_script_execution
                python_callable_file: /opt/airflow/dags/operators/common/pg_operator.py
                op_kwargs:
                    pg_hook_con: $ENV
                    directory: /opt/airflow/dags/sql/other/
                    schema_folder: dds
                    table_name: sap_bu_manager_rls
                dependencies: [start_dag]
            end_dag:
                operator: airflow.operators.dummy_operator.DummyOperator
                task_id: end_dag
                dependencies: 
                    - dds.sap_bu_manager_rls
    ```
- test_dag  - Пример с DEV
    ```yaml
    default:
        default_args:
            retries: 0
            depends_on_past: False
            on_failure_callback: 
            callback: operators.example_task.on_failure_callback
        catchup: False
        max_active_runs: 1  
    
    test_new_dag:
        default_args:
            start_date: 2025-02-12
            owner: s.frolkin
        schedule_interval: '@once'
        tags: ['example']
        task_groups:
            task_group_v1:
                dependencies: [start_dag]
            task_group_v2:
                dependencies: [task_group_v1]
        tasks:
            start_dag:
                operator: airflow.operators.dummy_operator.DummyOperator
            get_value_v1: 
                operator: airflow.operators.python.PythonOperator
                python_callable_name: _get_value
                python_callable_file: /opt/airflow/dags/operators/example_task.py
                task_group_name: task_group_v1
            get_value_v2:
                operator: airflow.operators.python.PythonOperator
                python_callable_name: _get_value
                python_callable_file: /opt/airflow/dags/operators/example_task.py
                task_group_name: task_group_v1
            get_value_group_v:
                operator: airflow.operators.python.PythonOperator
                python_callable_name: get_next_value
                python_callable_file: /opt/airflow/dags/operators/example_task.py
                task_group_name: task_group_v2
            end_dag:
                operator: airflow.operators.dummy_operator.DummyOperator
                dependencies:
                - get_value_group_v
    ```











