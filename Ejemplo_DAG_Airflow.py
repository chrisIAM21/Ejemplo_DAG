# Ejemplo de DAG cuando se utiliza el operador de Python para enviar un correo electrónico despues de ejecutar una tarea de Bash

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText

# Argumentos
default_args = {
    'owner': 'Christopher Andrade',
    'depends_on_past': False,
    'start_date': datetime.now()
    'email': ['christopher.andrade4604@alumnos.udg.mx'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

# DAG
dag = DAG(
    'DAG_Prueba2.Py2',
    default_args=default_args,
    description='Ejemplo de un DAG para Airflow',
    schedule_interval= '@hourly'
)

# Tasks
task1 = BashOperator(
    task_id='Tarea1',
    bash_command='echo "Se ejecuta la tarea 1 que es una tarea de Bash"',
    dag=dag,
)

task2 = BashOperator(
    task_id='Tarea2',
    bash_command='echo "Se ejecuta la tarea 2 que es una tarea de Bash"',
    dag=dag,
)

task3 = BashOperator(
    task_id='Tarea3',
    bash_command='echo "Se ejecuta la tarea 3 que es una tarea de Bash"',
    dag=dag,
)

def send_email():
    sender = ['christopherivanandrademendozab@gmail.com']
    password = ['robotor8']
    receivers = ['christopher.andrade4604@alumnos.udg.mx', 'christopherivanandrademendozab@gmail.com']
    message = MIMEText('Mensaje de prueba, enviado desde Airflow')
    message['From'] = sender
    message['To'] = receivers
    message['Subject'] = 'Hola, este es un correo enviado desde Airflow'
    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(sender, password)
        server.sendmail(sender, receivers, message.as_string())
        print("Correo enviado exitosamente")
    except Exception as e:
        print("Error: no se pudo enviar el correo")
        print(e)
    finally:
        server.quit()

task4 = PythonOperator(
    task_id='Tarea4',
    python_callable=send_email,
    dag=dag,
)

task1 >> task2 >> task3 >> task4