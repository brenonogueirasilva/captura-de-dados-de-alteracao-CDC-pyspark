from airflow import DAG 
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id= 'Dag_Docker_TabelaCDC',
    description='Dag para Executar script que simula um assinatura CDC',
    start_date= days_ago(1) ,  
    schedule_interval="@daily",
    
) as dag:
    task_1 = DockerOperator( 
        task_id = 'Tabela_CDC',
        image='pyspark_image',
        container_name='pyspark_CDC',
        command= 'python3 /code/main.py', 
        docker_url= 'tcp://docker-socket-proxy:2375',
        network_mode='rede_projeto' ,
        auto_remove = True 
    )

    task_1 