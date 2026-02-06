import mysql.connector
from mysql.connector import Error

from config import db_params


def create_database_tables():
    try:
        connection = mysql.connector.connect(db_params)
        
        if connection.is_connected():
            cursor = connection.cursor()
            
            tables = {
                    'crawl_sessions': """
                        CREATE TABLE IF NOT EXISTS crawl_sessions (
                            id INT PRIMARY KEY AUTO_INCREMENT,
                            etld VARCHAR(255) NOT NULL,
                            url TEXT NOT NULL,
                            category ENUM('global', 'country_coded', 'country_specific', 'global-validation', 'country_coded-validation', 'country_specific-validation') NOT NULL,
                            location ENUM('GERMANY', 'UAE', 'INDIA', 'USA', 'ALGERIA', 'FRANCE', 'INDIA-NORDVPN') NOT NULL,
                            fingerprinting JSON DEFAULT NULL,
                            is_fingerprinting BOOLEAN DEFAULT NULL,
                            user_identifiers JSON DEFAULT NULL,
                            is_user_identifiers BOOLEAN DEFAULT NULL,
                            INDEX idx_etld_location (etld, location),
                            INDEX idx_location (location)
                        )
                    """,

                    'cookies': """
                        CREATE TABLE IF NOT EXISTS cookies (
                            id INT PRIMARY KEY AUTO_INCREMENT,
                            session_id INT NOT NULL,
                            edge_id VARCHAR(50),
                            event_type TEXT,
                            storage_key TEXT,
                            storage_value TEXT,
                            parsed_value JSON,
                            caller_id VARCHAR(50),
                            caller_type TEXT,
                            caller_hash TEXT,
                            script_type TEXT,
                            caller_url LONGTEXT,
                            is_identifier BOOLEAN,
                            FOREIGN KEY (session_id) REFERENCES crawl_sessions(id) ON DELETE CASCADE,
                            INDEX idx_session (session_id)
                        )
                    """,

                    'local_storage': """
                        CREATE TABLE IF NOT EXISTS local_storage (
                            id INT PRIMARY KEY AUTO_INCREMENT,
                            session_id INT NOT NULL,
                            storage_key TEXT,
                            storage_value TEXT,
                            parsed_value JSON,
                            caller_id VARCHAR(50),
                            caller_type TEXT,
                            caller_hash TEXT,
                            caller_url TEXT,
                            is_identifier BOOLEAN,
                            FOREIGN KEY (session_id) REFERENCES crawl_sessions(id) ON DELETE CASCADE,
                            INDEX idx_session (session_id),
                            INDEX idx_key (storage_key(255))
                        )
                    """,

                    'session_storage': """
                        CREATE TABLE IF NOT EXISTS session_storage (
                            id INT PRIMARY KEY AUTO_INCREMENT,
                            session_id INT NOT NULL,
                            storage_key TEXT,
                            storage_value TEXT,
                            parsed_value JSON,
                            caller_id VARCHAR(50),
                            caller_type TEXT,
                            caller_hash TEXT,
                            caller_url TEXT,
                            is_identifier BOOLEAN,
                            FOREIGN KEY (session_id) REFERENCES crawl_sessions(id) ON DELETE CASCADE,
                            INDEX idx_session (session_id),
                            INDEX idx_key (storage_key(255))
                        )
                    """,

                    'scripts': """
                        CREATE TABLE IF NOT EXISTS scripts (
                            id INT PRIMARY KEY AUTO_INCREMENT,
                            session_id INT NOT NULL,
                            script_id VARCHAR(50),
                            script_type TEXT,
                            script_hash TEXT,
                            executor_id VARCHAR(50),
                            executor_tag VARCHAR(50),
                            executor_attrs JSON,
                            frame_id VARCHAR(50),
                            frame_main BOOLEAN,
                            frame_url TEXT,
                            frame_origin VARCHAR(500),
                            frame_blink_id INT,
                            FOREIGN KEY (session_id) REFERENCES crawl_sessions(id) ON DELETE CASCADE,
                            INDEX idx_session (session_id),
                            INDEX idx_script_type (script_type(100))
                        )
                    """,

                    'requests': """
                        CREATE TABLE IF NOT EXISTS requests (
                            id INT PRIMARY KEY AUTO_INCREMENT,
                            session_id INT NOT NULL,
                            request_id INT,
                            request_type VARCHAR(100),
                            request_url MEDIUMTEXT,
                            result_size INT,
                            result_hash TEXT,
                            result_headers JSON,
                            redirects JSON,
                            result_status VARCHAR(50),
                            frame_id VARCHAR(50),
                            frame_main BOOLEAN,
                            frame_url TEXT,
                            frame_origin TEXT,
                            is_tracker BOOLEAN,
                            FOREIGN KEY (session_id) REFERENCES crawl_sessions(id) ON DELETE CASCADE,
                            INDEX idx_session (session_id),
                            INDEX idx_request_type (request_type)
                        )
                    """,

                    'js_calls': """
                        CREATE TABLE IF NOT EXISTS js_calls (
                            id INT PRIMARY KEY AUTO_INCREMENT,
                            session_id INT NOT NULL,
                            caller_id VARCHAR(50),
                            caller_type VARCHAR(100),
                            caller_hash VARCHAR(100),
                            caller_url LONGTEXT,
                            executor_id VARCHAR(50),
                            executor_tag VARCHAR(50),
                            executor_attrs JSON,
                            call_method VARCHAR(200),
                            call_args JSON,
                            call_result MEDIUMTEXT,
                            context_id VARCHAR(50),
                            context_main BOOLEAN,
                            context_url TEXT,
                            context_origin VARCHAR(500),
                            FOREIGN KEY (session_id) REFERENCES crawl_sessions(id) ON DELETE CASCADE,
                            INDEX idx_session (session_id),
                            INDEX idx_call_method (call_method)
                        )
                    """,

                    'url_tracking_classification' : """
                        CREATE TABLE IF NOT EXISTS url_tracking_classification (
                            id INT PRIMARY KEY AUTO_INCREMENT,
                            url LONGTEXT,
                            url_hash CHAR(64) UNIQUE,
                            is_tracker BOOLEAN
                        );
                    """
            }  
            for table_name, table_sql in tables.items():
                try:
                    cursor.execute(table_sql)
                    print(f"Table '{table_name}' created successfully")
                except Error as e:
                    print(f"Error creating table {table_name}: {e}")
            
            connection.commit()
            print("All tables created successfully!")
            

            cursor.close()
            connection.close()
            print("MySQL connection closed")

    except Error as e:
        print(f"Error connecting to MySQL: {e}")
 

if __name__ == "__main__":
    create_database_tables()


