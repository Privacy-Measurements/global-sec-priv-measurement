import mysql.connector
from mysql.connector import Error

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'utils'))
from config import db_params


def create_database_tables():
    try:
        # Remap pymysql-style keys to mysql.connector keys
        mc_params = {
            'host': db_params.get('host'),
            'user': db_params.get('user'),
            'password': db_params.get('password'),
        }
        database_name = db_params.get('db') or db_params.get('database')
        
        # Connect without database first to check/create DB
        connection = mysql.connector.connect(**mc_params)
        
        if connection.is_connected():
            cursor = connection.cursor()
            
            # Check if database exists
            cursor.execute(f"SHOW DATABASES LIKE '{database_name}'")
            result = cursor.fetchone()
            
            if not result:
                print(f"Database '{database_name}' does not exist. Creating it...")
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
                print(f"Database '{database_name}' created successfully!")
            else:
                print(f"Database '{database_name}' already exists.")
            
            cursor.close()
            connection.close()
            
            # Now reconnect WITH the database
            mc_params['database'] = database_name
            connection = mysql.connector.connect(**mc_params)
            cursor = connection.cursor()
            
            tables = {
                    # ------------------------------------------------------------------ #
                    # crawl_sessions: one row per (etld, access_id, graphml attempt)     #
                    # ------------------------------------------------------------------ #
                    'crawl_sessions': """
                        CREATE TABLE IF NOT EXISTS crawl_sessions (
                            id INT PRIMARY KEY AUTO_INCREMENT,
                            etld VARCHAR(255) NOT NULL,
                            url TEXT NOT NULL,
                            etld_url_rel VARCHAR(16) DEFAULT NULL,
                            access_id VARCHAR(50) DEFAULT NULL,
                            graphml_path TEXT DEFAULT NULL,
                            category ENUM('global', 'country_coded', 'country_specific', 'global-validation', 'country_coded-validation', 'country_specific-validation') NOT NULL,
                            location ENUM('GERMANY', 'UAE', 'INDIA', 'USA', 'ALGERIA', 'FRANCE', 'FRANCE-VPN', 'UAE-VPN', 'INDIA-VPN', 'GERMANY-VPN', 'ALGERIA-VPN', 'USA-VPN') NOT NULL,
                            fingerprinting JSON DEFAULT NULL,
                            is_fingerprinting BOOLEAN DEFAULT NULL,
                            user_identifiers JSON DEFAULT NULL,
                            is_user_identifiers BOOLEAN DEFAULT NULL,
                            is_cloudflare BOOLEAN DEFAULT NULL,
                            INDEX idx_etld_location (etld, location),
                            INDEX idx_location (location),
                            INDEX idx_category (category),
                            INDEX idx_access (etld, access_id)
                        )
                    """,

                    # ------------------------------------------------------------------ #
                    # cookies: from pagegraph `cookies` command                          #
                    # ------------------------------------------------------------------ #
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
                            INDEX idx_session (session_id),
                            INDEX ix_cookie_event_type (event_type(64))
                        )
                    """,

                    # ------------------------------------------------------------------ #
                    # local_storage                                                       #
                    # ------------------------------------------------------------------ #
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

                    # ------------------------------------------------------------------ #
                    # session_storage                                                     #
                    # ------------------------------------------------------------------ #
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

                    # ------------------------------------------------------------------ #
                    # scripts: pagegraph `scripts` command enriched with creator/parent  #
                    # Fields: script.{id,type,hash,executor}, frame.{id,main,url,...}    #
                    #         + creator_id/type, executor_type, parent_id (enriched)     #
                    # ------------------------------------------------------------------ #
                    'scripts': """
                        CREATE TABLE IF NOT EXISTS scripts (
                            id INT PRIMARY KEY AUTO_INCREMENT,
                            session_id INT NOT NULL,
                            script_id VARCHAR(50),
                            script_type TEXT,
                            script_hash TEXT,
                            script_src TEXT,
                            etld_script_src_rel VARCHAR(16) DEFAULT NULL,
                            creator_id VARCHAR(50) DEFAULT NULL,
                            creator_type TEXT DEFAULT NULL,
                            executor_id VARCHAR(50),
                            executor_type TEXT DEFAULT NULL,
                            executor_tag VARCHAR(50),
                            executor_attrs JSON,
                            parent_id VARCHAR(50) DEFAULT NULL,
                            frame_id VARCHAR(50),
                            frame_main BOOLEAN,
                            frame_url TEXT,
                            frame_origin VARCHAR(500),
                            frame_blink_id INT,
                            FOREIGN KEY (session_id) REFERENCES crawl_sessions(id) ON DELETE CASCADE,
                            INDEX idx_session (session_id),
                            INDEX idx_script_type (script_type(100)),
                            INDEX script_src_idx (script_src(64)),
                            INDEX idx_scripts_session_hash (session_id, script_hash(64))
                        )
                    """,

                    # ------------------------------------------------------------------ #
                    # requests: pagegraph `requests` command enriched with requester     #
                    # Fields: request.{id,type,url,redirects,result}, frame, requester  #
                    # ------------------------------------------------------------------ #
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
                            security_headers JSON DEFAULT NULL,
                            redirects JSON,
                            result_status VARCHAR(50),
                            frame_id VARCHAR(50),
                            frame_main BOOLEAN,
                            frame_url TEXT,
                            frame_origin TEXT,
                            frame_blink_id INT DEFAULT NULL,
                            requester_id VARCHAR(50) DEFAULT NULL,
                            requester_type TEXT DEFAULT NULL,
                            parent_id VARCHAR(50) DEFAULT NULL,
                            is_tracker BOOLEAN,
                            FOREIGN KEY (session_id) REFERENCES crawl_sessions(id) ON DELETE CASCADE,
                            INDEX idx_session (session_id),
                            INDEX idx_request_type (request_type)
                        )
                    """,

                    # ------------------------------------------------------------------ #
                    # js_calls: pagegraph `js-calls` command                             #
                    # caller.{id,type,hash,executor}, call.{method,args,result,context} #
                    # ------------------------------------------------------------------ #
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
                            INDEX idx_call_method (call_method),
                            INDEX idx_js_session_caller_hash (session_id, caller_id)
                        )
                    """,

                    # ------------------------------------------------------------------ #
                    # html_elements: pagegraph `html-elements` command (ELM)             #
                    # Fields: id, tag, src, attrs, creator_id/type, dom_parent, parent  #
                    # ------------------------------------------------------------------ #
                    'html_elements': """
                        CREATE TABLE IF NOT EXISTS html_elements (
                            id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                            session_id INT NOT NULL,
                            element_id VARCHAR(50) NOT NULL,
                            tag VARCHAR(20) NOT NULL,
                            site TEXT DEFAULT NULL,
                            url TEXT DEFAULT NULL,
                            src TEXT DEFAULT NULL,
                            attrs_json LONGTEXT DEFAULT NULL,
                            creator_id VARCHAR(50) DEFAULT NULL,
                            creator_type VARCHAR(50) DEFAULT NULL,
                            dom_parent_id VARCHAR(50) DEFAULT NULL,
                            dom_parent_type VARCHAR(50) DEFAULT NULL,
                            parent_id VARCHAR(50) DEFAULT NULL,
                            parent_type VARCHAR(50) DEFAULT NULL,
                            UNIQUE KEY uniq_session_element (session_id, element_id),
                            KEY idx_session_id (session_id),
                            KEY idx_tag (tag),
                            CONSTRAINT fk_html_elements_session FOREIGN KEY (session_id) REFERENCES crawl_sessions(id) ON DELETE CASCADE ON UPDATE CASCADE
                        )
                    """,


                    'har_requests': """
                        CREATE TABLE IF NOT EXISTS har_requests (
                            id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
                            session_id INT NOT NULL,
                            url TEXT NOT NULL,
                            domain VARCHAR(255) DEFAULT NULL,
                            party_type VARCHAR(16) DEFAULT NULL,
                            resource_type VARCHAR(64) DEFAULT NULL,
                            is_main_frame BOOLEAN DEFAULT NULL,
                            is_sub_frame BOOLEAN DEFAULT NULL,
                            frame_origin VARCHAR(2048) DEFAULT NULL,
                            method VARCHAR(16) DEFAULT NULL,
                            status INT DEFAULT NULL,
                            status_text VARCHAR(128) DEFAULT NULL,
                            mime_type VARCHAR(255) DEFAULT NULL,
                            size INT DEFAULT NULL,
                            time DOUBLE DEFAULT NULL,
                            started_datetime VARCHAR(64) DEFAULT NULL,
                            request_headers JSON DEFAULT NULL,
                            response_headers JSON DEFAULT NULL,
                            initiator JSON DEFAULT NULL,
                            server_ip VARCHAR(64) DEFAULT NULL,
                            connection VARCHAR(64) DEFAULT NULL,
                            cache JSON DEFAULT NULL,
                            CONSTRAINT fk_har_requests_session FOREIGN KEY (session_id) REFERENCES crawl_sessions(id) ON DELETE CASCADE,
                            INDEX idx_session (session_id),
                            INDEX idx_domain (domain),
                            INDEX idx_party_type (party_type),
                            INDEX idx_resource_type (resource_type),
                            INDEX idx_har_session_domain (session_id, domain)
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


