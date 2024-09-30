import requests
import json
import psycopg2
from psycopg2 import sql

# PostgreSQL connection details
pg_host = 'localhost'  # Replace with your PostgreSQL host
pg_port = '5432'  # Default PostgreSQL port
pg_database = 'postgres'  # Replace with your PostgreSQL database name
pg_user = 'postgres'  # Replace with your PostgreSQL username
pg_password = 'admin'  # Replace with your PostgreSQL password

# Adzuna API configuration
app_id = '<adzuna_api_id>'
api_key = '<adzuna_api_key>'
url = '<adzuna_api_url>'

params = {
    'app_id': app_id,
    'app_key': api_key,
    'results_per_page': 50,
    'what': 'Data Engineer',
    'where': 'USA',
}

# Fetch job data from Adzuna API
response = requests.get(url, params=params)

if response.status_code == 200:
    data = response.json()
    jobs = data.get("results", [])

    # Connect to PostgreSQL database
    conn = psycopg2.connect(
        host=pg_host,
        port=pg_port,
        database=pg_database,
        user=pg_user,
        password=pg_password
    )
    cursor = conn.cursor()

    # Create table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS public.de_jobs (
        id SERIAL PRIMARY KEY,
        job_title VARCHAR(255),
        company VARCHAR(255),
        location VARCHAR(255),
        salary_min INT,
        salary_max INT,
        created_date TIMESTAMPTZ,
        job_category VARCHAR(255),
        job_description TEXT,
        redirect_url TEXT
    );
    """
    cursor.execute(create_table_query)

    for index, job in enumerate(jobs, start=1):
        # Prepare SQL insert statement
        insert_query = sql.SQL("""
            INSERT INTO public.de_jobs (job_title, company, location, salary_min, salary_max, created_date, job_category, job_description, redirect_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """)

        # Job details to insert
        job_data = (
            job['title'],
            job['company']['display_name'],
            job['location']['display_name'],
            job.get('salary_min'),
            job.get('salary_max'),
            job['created'],
            job['category']['label'],
            job['description'],  # Truncate for brevity
            job['redirect_url']
        )

        # Execute the insert statement
        cursor.execute(insert_query, job_data)

        # Print job details
        print(f"Job Number     : {index}")
        print(f"Job Title      : {job['title']}")
        print(f"Company        : {job['company']['display_name']}")
        print(f"Location       : {job['location']['display_name']}")
        print(f"Salary         : {job.get('salary_min')} - {job.get('salary_max')}")
        print(f"Created Date   : {job['created']}")
        print(f"Job Category   : {job['category']['label']}")
        print(f"Job Description: {job['description']}...")  # Truncate description for brevity
        print(f"More Details   : {job['redirect_url']}")
        print("-" * 50)

    # Commit the transaction and close the connection
    conn.commit()
    cursor.close()
    conn.close()
else:
    print(f"Failed to fetch jobs. Status Code: {response.status_code}")
