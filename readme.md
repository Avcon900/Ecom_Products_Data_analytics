# My Shopping Helper- ELT pipeline
I began making this Project as a way to keep my skills sharp as well as to build something actually useful for me. Personally, I have a difficult time making decisions whenever I have to buy something, the fear of "Oh no, I must buy the best possible thing in my budget" oftens ends up taking too much of my time, along with all the thousands of options Ecom platforms have made available, it just keeps getting harder to order something. <br>
So guess what? It's to build my own product recommendation system... **of sorts**.

## Goals Checklist
- [x] Scrape data from Ecommerce Platforms like Amazon 
- [x] Ingest raw scraped data to Bronze layer of the data lakehouse
- [x] Clean and standardize data to prepare for the Silver Layer 
- [x] Transform, typecast and ingest to Silver
- [ ] Build two separate data marts for Gold Layer
- [x] Transform data from Silver for Usecase 1: Dashboard
- [ ] Transform data from Silver for Usecase 2: GenAI model
- [ ] Implement data vlidation for each layer
- [ ] Orchestrate monthly jobs using Airflow
- [x] Containerize using Docker & kubernettes 

## Tools & Prequisites

1. **Python 3.10**
2. **Selenium**
3. **Spark (PySpark)**
4. **Hadoop**
5. **JDK 11.xx**
6. **Delta lake (delta-spark)**
7. **Jupyter Notebooks**
8. **Lucidchart (for diagrams)**
9. **Docker** 
10. **PowerBI**

## How to run (local setup - will get easier once automated) 

1. **Clone the repo with**  <br> ``` git clone https://github.com/Avcon900/Ecom_Products_Data_analytics.git ``` <br>

2. **Move into project directory** <br> ``` cd Ecom_Products_Data_analytics ``` <br>

3. **Change branch to dockerSpark_image** <br> ``` git checkout dockerSpark_image ```

4. **Run the Scraper Script manually (optional- only if you want to update data)** <br>
    ~~~
        python -m venv env
        env\Scripts\activate.bat
        pip -r requirements.txt
        cd Scraper
        python main.py 
    ~~~ 
    <br>

5. **Build docker image and start the containers** <br> ``` docker-compose up --build -d ``` <br>

6. **Use bash inside container to execute scripts** <br> ``` docker exec -it my_shopping_helper_container bash ``` <br>

7. **Once inside container, run the following commands once** <br>
    ~~~
        python3 ingest_raw_to_bronze.py

        python3 transformations_for_silver.py

        python 3 transformations_for_db_mart_gold.py
    ~~~
8. **Exit from the container** <br> ``` exit ``` <br>

9. **Your datalake has been built, you can either choose to view the dashboard yourself by connecting the postgres container db in PowerBI or stop the containers** <br>

    **To connect PowerBI to our datamart, you will need the following credentials**
    ~~~
        server: localhost:5432
        database: hive_metastore
        user: hive
        password: hivepass123
        table: public.products_gold_latest_export
    ~~~

10. **Now you can open the file in the dashboard directory in PowerBI or create your own dashboard using the above credentials**

## Architecture
![Data Architecture.png](https://github.com/Avcon900/Ecom_Products_Data_analytics/blob/master/architecture%20diagrams/Data%20architecture.png?raw=true)
<br>

## Project Workflow (till current development)
- The WebScraper is scheduled to run on a monthly basis and stores the data for the user specified list of categories as .csv files in the **data/** folder.
- The csv files are named by the **TIMESTAMP** of the Scraping job.
- The ingest_raw_to_bronze script checks for files in the **data/** folder each time its run, if a new file is found, it writes the file in append mode in the bronze layer as a delta table and partitions the data by a derived column called **date**.
- The transformations_for_silver script checks for any new data by comparing the partition date with the **last_processed_timestamp** and loads the raw data from bronze layer. This data is then cleaned, standardized and written into the Silver Layer of data lake.
- The transformations_for_db_mart_gold script will register the delta tables in metadata, extract a copy of it and perform transformations required for the dashboard data mart. This transformed dataset is first stored in the gold layer of data lake holding the entirety of scraped data. From this gold layer, only the most recent partition of scraped data is made accessible to the dashboard by materializing it into postgres database. 
<br>
<center>

# WORK IN PROGRESS
![Work in Progress gif](https://cdn.dribbble.com/userupload/22866416/file/original-79954486027de6600487dfbc4eb0f7a1.gif)
</center>
