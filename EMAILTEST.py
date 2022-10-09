#DAG con operador de Python y Bash se ejecuta cada hora desde hoy
#Correo desde ejemplo@gmail.com contraseña: 123456
#Correo a christopher.andrade4604@alumnos.udg.mx

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'Christopher Andrade',
    'depends_on_past': False,
    'start_date': datetime.now()
}

dag = DAG(
    'EMAIL',
    default_args=default_args,
    description='DAG con operador de Python y Bash',
    schedule_interval='0 * * * *'
)

def _send_email():
    
    os.system("echo 'Hola, este es un correo enviado desde Airflow' | mail -s 'Correo de prueba' christopher.andrade4604@alumnos.udg.mx")


t1 = PythonOperator(
    task_id='send_email',
    python_callable=_send_email,
    dag=dag
)

t2 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag
)

t1 >> t2

#En el código anterior, se crea una función que envía un correo electrónico utilizando el comando mail. 
# Luego, se crea una tarea de Python que llama a la función anterior. Por último, se crea una tarea de Bash que imprime la fecha y hora actual.

