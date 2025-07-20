# FDA Drug Event ETL Pipeline



This project is an ETL (Extract, Transform, Load) pipeline that fetches drug event data from the FDA API, processes it, and loads it into a MySQL database. It handles extracting drug event and label data, cleaning and transforming it, and then storing the results for further analysis.

 Key Features:
- **Extract**: Fetches adverse event data for drugs from the FDA API.
- **Transform**: Cleans and processes the data (removes nulls, formats data, extracts useful features).
- **Load**: Loads the transformed data into a MySQL database for storage and further analysis.

---

Project Structure

/FDA-Drug-ETL-Pipeline │ 
├── /extract.py # Script to fetch data from the FDA API 
├── /transform.py # Script to clean and transform the extracted data 
├── /load.py # Script to load data into MySQL 
├── /main.py # Main script to run the entire ETL pipeline 
├── /config.env # Environment variables for database connection and ETL settings 
├── /requirements.txt # List of dependencies 
── /README.md # Project documentation





---

 Requirements

Make sure you have the following Python libraries installed:

- `requests`: To interact with the FDA API.
- `pandas`: For data manipulation and transformation.
- `mysql-connector-python`: To connect and insert data into a MySQL database.
- `dotenv`: To load environment variables for database credentials.

To install the required dependencies, run the following:

```bash
pip install -r requirements.txt


Setup

Clone the repository:

git clone https://github.com/your-username/FDA-Drug-ETL-Pipeline.git
cd FDA-Drug-ETL-Pipeline


Create a .env file in the root directory and add your database credentials:

env
Copy
Edit
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=your_password
DB_NAME=fda_drug_data

# ETL Settings
MAX_ROWS=1000
Install the dependencies:

bash
Copy
Edit
pip install -r requirements.txt
Running the Pipeline
To run the entire ETL process, simply execute the main.py script:

bash
Copy
Edit
python main.py
This will trigger the extraction of data from the FDA API, transform the data, and load it into your MySQL database.

Files Overview
extract.py: Contains functions to fetch drug event and label data from the FDA API.

transform.py: Handles data transformation, cleaning, and feature extraction.

load.py: Handles inserting the transformed data into a MySQL database.

main.py: Main script that runs the complete ETL process.

config.env: Contains environment variables such as database credentials and ETL settings.

requirements.txt: List of Python dependencies required for the project.

