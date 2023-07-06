from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator 
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from airflow.models import XCom
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import to_date, when, col, lower


# Definisikan DAG
default_args = {
    'owner': 'ahyar',
    'start_date': datetime(2023, 5, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'linkedin_scraping',
    default_args=default_args,
    description='Scrape job data from LinkedIn',
    schedule='@daily' # Run tiap hari
)

# task_1
def load_page(ti):
    # Define browser and action setup
    options = webdriver.ChromeOptions()
    options.add_argument('--start-maximized')
    driver = webdriver.Chrome(options=options)


    # Define URL
    linkedin_url = "https://www.linkedin.com/jobs/search/?keywords=Data%20Scientist&location=Indonesia&locationId=&geoId=102478259&f_TPR=r86400&position=1&pageNum=0"
    #linkedin_url = "https://www.linkedin.com/jobs/search/?keywords=data%20scientist&location=Indonesia"

    # Action Steps
    driver.maximize_window()
    driver.get(linkedin_url) # Open web page

    # Determine how many jobs we want to scrape, and calculate how many time we need to scroll down
    #no_of_jobs = 100
    no_of_jobs = int(driver.find_element(By.CSS_SELECTOR,'h1>span').get_attribute('innerText'))

    n_scroll = int(no_of_jobs/25)+1

    i = 1
    driver.execute_script("return document.body.scrollHeight") #scroll to top
    while i <= n_scroll:
        driver.execute_script("return document.body.scrollHeight")
        time.sleep(3) # wait for 3 seconds
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);") #scroll to the bottom of page
        time.sleep(4)
        i = i + 1
        try:
            button = driver.find_element(By.XPATH,"/html/body/div[1]/div/main/section[2]/button")
            #time.sleep(2)
            button.click()
            time.sleep(1)
        except:
            driver.execute_script("return document.body.scrollHeight")
            time.sleep(3)

    jobs = driver.find_element(By.CLASS_NAME,"jobs-search__results-list").find_elements(By.TAG_NAME,'li') # return a list
    
    # scrape main attribute
    job_title = []
    company_name = []
    location = []
    date = []
    job_link = []
    for job in jobs:

        job_title0 = job.find_element(By.CSS_SELECTOR,'h3').get_attribute('innerText')
        job_title.append(job_title0)

        company_name0 = job.find_element(By.CSS_SELECTOR,'h4').get_attribute('innerText')
        company_name.append(company_name0)

        location0 = job.find_element(By.CLASS_NAME,'job-search-card__location').get_attribute('innerText')
        location.append(location0)

        date0 = job.find_element(By.CSS_SELECTOR,'div>div>time').get_attribute('datetime')
        date.append(date0)

        job_link0 = job.find_element(By.CSS_SELECTOR,'a').get_attribute('href')
        job_link.append(job_link0)

    # buat dictionary
    df = {'Date': date,
          'Company': company_name,
          'Title': job_title,
          'Location': location,
          'Link' : job_link,                       
                         }
    print(df)
    
    # push ke xcom_push
    ti.xcom_push(key = 'data_frame',  value= df)



#task_2
def save_to_csv(ti):
    # tarik kembali dengan xcom_pull
    result = ti.xcom_pull(task_ids= 'load_page', key= 'data_frame')
    df = pd.DataFrame(result, columns=['Date', 'Company', 'Title', 'Location', 'Link'])
    today = str(date.today())
    df.to_csv(f'/mnt/c/Users/user/Documents/airflow/dags/{today}.csv',index=False, mode='w')


#spark = SparkSession.builder.appName("Spark_transform").getOrCreate()

task_4
def transformation():
    # load dataset
    today = str(date.today())
    df = spark.read.csv(f"hdfs://localhost:9000/user/ahyar/mp_airflow/{today}.csv", header =True, inferSchema=True)

    # change column 'Date' data type
    df = df.withColumn("Date", to_date("Date", "yyyy-MM-dd"))

    # Mengubah nama kolom "Title" menjadi "Job Title"
    df = df.withColumnRenamed("Title", "Job_Title")

    # Konversi Job_Title menjadi lowercase
    df = df.withColumn("Job_Title", lower(col("Job_Title")))

    # definisikan keyword
    keyword = ['data science', 'data scientist', 'machine learning', 'artificial intelligence', 'ai/ml', 'ml', 'ai engineer']

    # Buat kolom baru "Job_Category" berdasarkan keyword
    df = df.withColumn("Job_Category", when(col("Job_Title").rlike(fr'\b({"|".join(keyword)})\b'), "Data Science").otherwise("Other"))

    # save into hdfs
    df = df.write.format("csv").option("header", "true").mode("overwrite").save(f"hdfs://localhost:9000/user/ahyar/save_airflow/{today}_clean")



# Define the task
task_1 = PythonOperator(
    task_id='load_page',
    python_callable=load_page,
    dag=dag,
)

task_2 = PythonOperator(
    task_id='save_data',
    python_callable=save_to_csv,
    dag=dag,
)

today = str(date.today())
task_3 = BashOperator(
    task_id='save_to_hdfs',
    bash_command=f'hadoop fs -copyFromLocal -f /mnt/c/Users/user/Documents/airflow/dags/csv/{today}.csv /user/ahyar/mp_airflow',
    dag=dag,
)

task_4 = PythonOperator(
    task_id='transformation_and_save',
    python_callable=transformation,
    dag=dag,
)

# task_4 = BashOperator(
#     task_id='transformation',
#     bash_command='/mnt/c/Users/user/Documents/airflow/dags/transformasi.py',
#     dag=dag,
# )

# task_4 = SparkSubmitOperator(
#     task_id='transformation',
#     application='/mnt/c/Users/user/Documents/airflow/dags/transformasi.py', conn_id = 'spark_default',
#     dag=dag,
# )

# set urutan task
task_1 >> task_2 >> task_3 >> task_4 