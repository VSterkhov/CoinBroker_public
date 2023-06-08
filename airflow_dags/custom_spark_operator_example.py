from airflow.models import BaseOperator
import logging
from subprocess import Popen, STDOUT, PIPE
from airflow.exceptions import AirflowException, AirflowTaskTimeout
from airflow.utils.decorators import apply_defaults

'''
SparkOperator for airflow designed to simplify work with Spark on YARN. 
Simplifies using spark-submit in airflow DAGs, retrieves application id 
and tracking URL from logs and ensures YARN application is killed on timeout.
Example usage:
my_task = SparkOperator(script='hdfs:///location/of/pyspark/script.py',
    driver_memory='2g',
    extra_params='--files hdfs:///some/extra/file',
    execution_timeout = timedelta(minutes=30),
    dag=dag)  
    
LINK   ---   https://gist.github.com/szczeles/070d52dd6c82e92c8ee37ceed41e4885

Support:
https://github.com/puppetlabs/incubator-airflow/blob/master/airflow/contrib/hooks/spark_submit_hook.py#L310
https://groups.google.com/g/airbnb_airflow/c/-5WvigVS0ks

XCOM   ---   https://precocityllc.com/blog/airflow-and-xcom-inter-task-communication-use-cases/
    
'''
class SparkOperator(BaseOperator):

    @apply_defaults
    def __init__(self, script, driver_memory='1g', spark_version='2.0.2', extra_params='', yarn_queue='default', *args, **kwargs):
        self.task_id = script.split('/')[-1].split('.')[0]
        super(SparkOperator, self).__init__(*args, **dict(kwargs, task_id = self.task_id))
        self.env = {'HADOOP_CONF_DIR': '/etc/hadoop/conf', 'PYSPARK_PYTHON': '/opt/conda/bin/python3'}
        self.spark_version = spark_version
        self.spark_params = '--master yarn --deploy-mode cluster --queue {yarn_queue} --driver-memory {driver_memory} {extra_params} {script}'.format(
            yarn_queue=yarn_queue,
            driver_memory=driver_memory,
            extra_params=extra_params,
            script=script
        )

    def execute(self, context):
        try:
            self.execute_spark_submit(context)
        except AirflowTaskTimeout:
            self.on_kill()
            raise

    def execute_spark_submit(self, context):
        command = '/usr/local/bin/spark-submit{spark_version} {params}'.format(spark_version=self.spark_version, params=self.spark_params)
        logging.info("Running command: " + command)
        self.applicationId = None
        self.sp = Popen(command, stdout=PIPE, stderr=STDOUT, env=self.env, shell=True)

        logging.info("Output:")
        line = ''
        for line in iter(self.sp.stdout.readline, b''):
            line = line.decode('utf-8').strip()
            if 'impl.YarnClientImpl: Submitted application' in line:
                self.applicationId = line.split(' ')[-1]
                context['ti'].xcom_push(key='application-id', value=self.applicationId)
            if 'tracking URL: ' in line:
                context['ti'].xcom_push(key='tracking-url', value=line.split(' ')[-1])

            logging.info(line)

        self.sp.wait()
        logging.info("Spark-submit exited with "
                     "return code {0}".format(self.sp.returncode))

        if self.sp.returncode:
            raise AirflowException("Spark application failed")

    def on_kill(self):
        logging.info('Sending SIGTERM signal to spark-submit')
        self.sp.terminate()
        if self.applicationId:
            logging.info('Killing application on YARN...')
            yarn_kill = Popen('yarn application -kill ' + self.applicationId, stdout=PIPE, stderr=STDOUT, env=self.env, shell=True)
            logging.info('...done, yarn command return code: ' + str(yarn_kill.wait()))
        else:
            logging.info('Not killing any application on YARN')