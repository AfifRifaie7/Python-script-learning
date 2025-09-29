# Section 1: Importng libraries

import os                           # For make folder, file path and directories
import glob                         # For finding files pattenr like *.lis
import time                         # Timing utilities
import logging                      # Write message to a log file with level (info, error)
import concurrent.futures           # simple high level parallelism
from dotenv import load_dotenv      # load setting from a .env file into enviroment variables
from datetime import datetime       # Current date/time and formatting
from multiprocessing import Manager # share small pieces of data safely between processes

import pandas as pd                 # tables in dataframe and easy DB / IO
from sqlalchemy import text         # Wrap raw SQL strings for execution with SQLAlchemy
from tqdm import tqdm               # Progress bars or loops, show batch progress when processing files

if not os.path.exists('log'):
    os.mkdir('log')

log_timestamp = datetime.now().strftime('%Y%m%d')
log_file_path = f"log/pdn_{log_timestamp}.log"
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

from db import DB
from google_drive import GoogleDrive
from backup import MySQLDatabaseBackup
from gmail_send_report import send_email
from panther import Panther, PantherLIS, PantherPDF

load_dotenv()

def main():
    try:
        t1 = time.perf_counter()
        engine = DB.get_engine(db="pdn")
        
        get_new_files(engine)
        execute_sql("sql/adhoc/drop_temp.sql", engine)
        
        a = process_lis(engine)
        b = process_pdf(engine)
  
        if a + b > 0: # a+b > 1 if there are new files
            post_procedure(engine)
        
        dbs = [
            "pdn",
            "pdn_activity"
        ]
        
        for db in dbs:
            logging.info(f"Exporting {db} database")
            backup_database(db)
            
        logging.info("Backup complete")
        t2 = time.perf_counter()
        execution_time = t2 - t1
        logging.info(f"Total execution time: {execution_time}")
        print(f"Time taken:{execution_time}")
    
        send_email(log_file_path)
        
        t2 = time.perf_counter()
        print(f"Time taken:{t2 - t1}")
    
    except Exception as e:
        logging.info(f"Operation aborted: {e}")
        
        files = glob.glob("data/new/lis/*.lis")
        
        for file in files:
            os.remove(file)
        
        logging.info(f"Files removed from download folder: {len(files)}")
    
        send_email(log_file_path)
        
def get_new_files(engine):
    drive = GoogleDrive()
    folder_id = os.getenv("FOLDER_ID")
    
    try:
        df = pd.read_sql("data_log", con=engine)
        uploaded_set = {f"{row['file_name']}_{row['file_hash']}" for _, row in df.iterrows()}
    except:
        uploaded_set = set()

    return drive.get_files_and_download_async(folder_id, uploaded_set)

def lis_wrapper(file_path, data_log):
    panther = PantherLIS(data_log)
    return panther.read_file(file_path)

def activtiy_wrapper(file_path, data_log):
    panther = PantherPDF(data_log)
    return panther.process_activity(file_path)

def result_wrapper(file_path, data_log):
    panther = PantherPDF(data_log)
    return panther.process_result(file_path)

def process_lis(engine):
    panther = Panther()
    panther.preprocess_filter("lis", engine)
    file_paths = panther.get_file_paths("lis")
    batch_size = 500
    
    with Manager() as manager:
        data_log = manager.dict({
            "serial_number":manager.list(),
            "file_name":manager.list(),
            "row_count":manager.list(),
            "file_hash":manager.list(),
            "period_start":manager.list(),
            "period_end":manager.list()
        })

        if len(file_paths) > 0:
            logging.info("Initializing LIS processing")
            execute_sql("sql/adhoc/create_lis_temp.sql", engine)
            for i in tqdm(range(0, len(file_paths), batch_size), desc="Batch Processing LIS files", position=0):
                batch_paths = file_paths[i:i+batch_size]
                with concurrent.futures.ProcessPoolExecutor(4) as executor:
                    results = executor.map(lis_wrapper, batch_paths, [data_log] * len(batch_paths))
                    dfs = [x for x in results if x is not None]
                    df = pd.concat(dfs, ignore_index=True)
                    with engine.connect() as conn:
                        trans = conn.begin()
                        try:
                            df.to_sql("lis_temp", con=conn, if_exists="append", index=False)
                            trans.commit()
                            
                        except Exception as e:
                            trans.rollback()
                            logging.info(f"LIS process error: {e}")
        
            logging.info(f"LIS process complete: total_files: {len(file_paths)}, total_rows: {len(df)}")
            execute_sql("sql/adhoc/insert_raw_lis.sql", engine)
            push_data_log(data_log, engine, file_type="raw_lis_file")
            return 1
        else:
            logging.info("No new LIS files to process")
            return 0

def process_pdf(engine):
    panther = PantherPDF(dict())
    panther.preprocess_filter("pdf", engine)
    file_paths = panther.get_file_paths("pdf")
    if len(file_paths) > 0:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            logging.info("Initializing PDF files categorization")
            executor.map(panther.categorize_pdf, file_paths)
        
        if any(panther.other_list["file_name"]):
            save_other_pdf(engine, panther.get_timestamp(), panther.other_list)
        
        logging.info(f"PDF files categorization completed. Activity Log:{len(panther.activity_list)}, Result report:{len(panther.result_list)}")

        process_activity(engine, panther.activity_list)
        process_result(engine, panther.result_list)
        return 1
    
    else:
        logging.info("No new PDF files to process")
        return 0
        
def process_result(engine, result_list):
    logging.info("Initializing Result Report processing")
    batch_size = 60
    
    with Manager() as manager:
        data_log = manager.dict({
            "serial_number":manager.list(),
            "file_name":manager.list(),
            "row_count":manager.list(),
            "file_hash":manager.list(),
            "period_start":manager.list(),
            "period_end":manager.list()
        })
        if len(result_list) > 0 :
            for i in tqdm(range(0, len(result_list), batch_size), desc="Batch Processing RESULT files", position=0):
                batch_paths = result_list[i:i+batch_size]
                
                with concurrent.futures.ProcessPoolExecutor(4) as executor:
                    results = executor.map(result_wrapper, batch_paths, [data_log] * len(batch_paths))
                    df_worklists, df_rows = zip(*[result for result in results])
                    
                if len(df_worklists) >0:
                    df_worklist = pd.concat(df_worklists, ignore_index=True)
                    df_row = pd.concat(df_rows, ignore_index=True)

                    with engine.connect() as conn:
                        trans = conn.begin()
                        try:
                            df_worklist.to_sql("worklist_temp", con=conn, if_exists="append", index=False)
                            df_row.to_sql("row_temp", con=conn, if_exists="append", index=False)
                        
                            trans.commit()
                            
                        except Exception as e:
                            trans.rollback()
                            logging.info(f"Result process error: {e}")
                            
            logging.info(f"Result process complete: total_files: {len(result_list)}, total_result_rows: {len(df_row)}, total_worklist_rows: {len(df_worklist)}")
            execute_sql("sql/adhoc/insert_result_report.sql", engine)
            push_data_log(data_log, engine, file_type="raw_result_file")
                
def process_activity(engine, activity_list):
    logging.info("Initializing Activity Log Report processing")
    batch_size = 60
    
    with Manager() as manager:
        data_log = manager.dict({
            "serial_number":manager.list(),
            "file_name":manager.list(),
            "row_count":manager.list(),
            "file_hash":manager.list(),
            "period_start":manager.list(),
            "period_end":manager.list()
        })
        
        if len(activity_list) > 0 :
            for i in tqdm(range(0, len(activity_list), batch_size), desc="Batch Processing ACTIVITY files", position=0):
                batch_paths = activity_list[i:i+batch_size]
                with concurrent.futures.ProcessPoolExecutor(4) as executor:
                    results = executor.map(activtiy_wrapper, batch_paths, [data_log] * len(batch_paths))
                    dfs = [x for x in results if x is not None]
              
                if len(dfs) >0:
                    df = pd.concat(dfs, ignore_index=True)
                    with engine.connect() as conn:
                        trans = conn.begin()
                        try:
                            df.to_sql("activity_temp", con=conn, if_exists="replace", index=False)
                            trans.commit()
                            
                        except Exception as e:
                            trans.rollback()
                            logging.info(f"Activity process error: {e}")
        
            logging.info(f"Activity process complete: total_files: {len(activity_list)}, total_rows: {len(df)}")
            execute_sql("sql/adhoc/insert_raw_activity_log.sql", engine)
            push_data_log(data_log, engine, file_type="raw_activity_file")
        
        else:
            logging.info("Activity process complete: total_files: 0")


def push_data_log(data_log, conn, file_type):
    panther = Panther()
   
    serial_number = list(data_log["serial_number"])
    file_name = list(data_log["file_name"])
    row_count = list(data_log["row_count"])
    file_hash = list(data_log["file_hash"])
    period_start = list(data_log["period_start"])
    period_end = list(data_log["period_end"])

    df_data_log = pd.DataFrame({
        "serial_number": serial_number,
        "file_name": file_name,
        "row_count": row_count,
        "file_hash": file_hash,
        "period_start": period_start,
        "period_end": period_end
    })
    
    df_data_log["log_timestamp"] = panther.get_timestamp()
    df_data_log["file_type"] = file_type
    df_data_log["row_count_validate"] = False
    
    df_data_log.to_sql("data_log", conn, if_exists="append", index=False)
    
def execute_sql(sql, engine):
    with open(sql, "r") as f:
        scripts = f.read()
        queries = [query.strip() for query in scripts.split(";") if query.strip()]
   
    with engine.connect() as conn: 
        for query in queries:
            conn.execute(text(query))

def reset_data_log(panther):
    panther.data_log = {
            "serial_number":[],
            "file_name":[],
            "row_count":[],
            "file_hash":[],
            "period_start":[],
            "period_end":[]
        }

def post_procedure(engine):
    with engine.connect() as conn:
        logging.info("Inserting data into staging table")
        conn.execute(text("CALL insert_staging();"))
        conn.commit()
        
        logging.info("Normalizing data")
        conn.execute(text("CALL insert_dimension();"))
        conn.commit()
        
        logging.info("Inserting data into master_table table")
        conn.execute(text("CALL insert_master();"))
        conn.commit()
        
        conn.execute(text("DROP TABLE staging_lis;"))
        conn.commit()
        
        logging.info("Updating flag description")
        conn.execute(text("CALL update_flag_description();"))
        conn.commit()
        
        logging.info("Normalizing activity_log table")
        conn.execute(text("CALL insert_activity();"))
        conn.commit()
        
        logging.info("Updating flag description")
        conn.execute(text("CALL update_flag_description();"))
        conn.commit()
        
        
def save_other_pdf(engine, log_timestamp, list):
    query = text("""
        INSERT INTO data_log (log_timestamp, file_type, file_name, file_hash)
        VALUES (:timestamp, :col1, :col2, :col3)
    """)
    
    data = [
        {'timestamp': log_timestamp, 'col1': col1, 'col2': col2, 'col3': col3}
        for col1, col2, col3 in zip(list["file_type"], list["file_name"], list["file_hash"])
    ]
    
    with engine.connect() as conn:
        conn.execute(query, data)
        conn.commit()

def backup_database(database):
    dbb = MySQLDatabaseBackup(database, datetime.now().strftime('%Y%m%d%H%M%S'))
    if not os.path.exists(dbb.backup_folder):
        os.makedirs(dbb.backup_folder)
    dbb.backup_database()

if __name__ == "__main__":
    main()