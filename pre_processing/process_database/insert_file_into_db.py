import json
import mysql.connector
from mysql.connector import Error
import sys
import os
import ijson  
import decimal
from urllib.parse import urlparse, urlunparse
import zstandard as zstd
from utils.config import db_params


MAX_TEXT_FIELD = 64000


class CrawlDataImporter:

    def __init__(self, host, database, user, password):
        self.connection = None
        try:
            self.connection = mysql.connector.connect(
                host=host,
                database=database,
                user=user,
                password=password
            )
            print("Successfully connected to MySQL database")
        except Error as e:
            print(f"Error connecting to MySQL: {e}")
            sys.exit(1)



    def normalize_url(self, url: str) -> str:
        try:
            parsed = urlparse(url)
            return urlunparse((parsed.scheme, parsed.netloc, parsed.path, '', '', ''))
        except Exception:
            return url  # fallback if parsing fails

    def get_existing_urls(self, location, category):
        query = "SELECT DISTINCT url FROM crawl_sessions WHERE location = %s AND category = %s"
        cursor = self.connection.cursor()
        cursor.execute(query, (location, category))
        res = cursor.fetchall()
        cursor.close()
        return {self.normalize_url(item[0]) for item in res if item[0]}

    def print_size_of_packet(self, values):
        packet_size = 0
        for v in values:
            if isinstance(v, str):
                packet_size += len(v.encode('utf-8'))
            elif isinstance(v, (dict, list)):
                packet_size += len(json.dumps(v).encode('utf-8'))
            elif v is None:
                continue
            else:
                packet_size += sys.getsizeof(v)

        print(f"Packet size for this insert: {packet_size / (1024*1024):.2f} MB")

    def insert_session(self, etld, url, location, category):

        print('Inserting session')

        cursor = self.connection.cursor()
        query = """
        INSERT INTO crawl_sessions (etld, url, location, category) 
        VALUES (%s, %s, %s, %s)
        """
        cursor.execute(query, (etld, url, location, category))
        session_id = cursor.lastrowid
        cursor.close()
        return session_id

    def insert_cookies(self, session_id, cookies_data):

        print('Inserting cookies')

        if not cookies_data or not cookies_data['report']:
            return

        cookies_data = cookies_data["report"]        

        cursor = self.connection.cursor()
        query = """
        INSERT INTO cookies (session_id, edge_id, event_type, storage_key, 
                           storage_value, caller_id, caller_type, caller_hash, 
                           script_type, caller_url) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        for cookie in cookies_data:

            values = (
                session_id,
                cookie.get('edge id'),
                cookie.get('event type'),
                cookie.get('cookie key'),
                cookie.get('cookie value'),
                cookie.get('caller', {}).get('id'),
                cookie.get('caller', {}).get('type'),
                cookie.get('caller', {}).get('hash'),
                cookie.get('caller', {}).get('type script'),
                cookie.get('caller', {}).get('url')
            )

            try:
                cursor.execute(query, values)
            except:
                print('Error in cookies')


        cursor.close()

    def insert_storage(self, session_id, storage_data, table_name):

        print('Inserting storage', table_name)

        if not storage_data or not storage_data['report']:
            return

        storage_data = storage_data["report"]
            
        cursor = self.connection.cursor()
        query = f"""
        INSERT INTO {table_name} (session_id, edge_id, event_type, storage_key, 
                                storage_value, caller_id, caller_type, caller_hash, 
                                script_type, caller_url)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        for item in storage_data:
            values = (
                session_id,
                item.get('edge id'),
                item.get('event type'),
                item.get('storage key'),
                item.get('storage value'),
                item.get('caller', {}).get('id'),
                item.get('caller', {}).get('type'),
                item.get('caller', {}).get('hash'),
                item.get('caller', {}).get('type script'),
                item.get('caller', {}).get('url')
            )
            cursor.execute(query, values)
        cursor.close()

    def insert_scripts(self, session_id, scripts_data):

        print('Inserting scripts')

        if not scripts_data or not scripts_data['report']:
            return

        scripts_data = scripts_data["report"]
            
        cursor = self.connection.cursor()
        query = """
        INSERT INTO scripts (session_id, script_id, script_type, script_hash,
                           executor_id, executor_tag, executor_attrs, frame_id,
                           frame_main, frame_url, frame_origin, frame_blink_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        for script in scripts_data:
            script_info = script.get('script', {})
            frame_info = script.get('frame', {})
            executor = script_info.get('executor', {})
            
            values = (
                session_id,
                script_info.get('id'),
                script_info.get('type'),
                script_info.get('hash'),
                executor.get('id'),
                executor.get('tag'),
                json.dumps(executor.get('attrs', {})),
                frame_info.get('id'),
                frame_info.get('main frame', False),
                frame_info.get('url'),
                frame_info.get('security origin'),
                frame_info.get('blink id')
            )
            cursor.execute(query, values)
        cursor.close()

    def insert_requests(self, session_id, requests_data):

        print('inserting requests')


        if not requests_data or not requests_data['report']:
            return

        requests_data = requests_data["report"]
            
        cursor = self.connection.cursor()
        query = """
        INSERT INTO requests (session_id, request_id, request_type, request_url,
                            result_size, result_hash, result_headers, result_status,
                            frame_id, frame_main, frame_url, frame_origin, redirects)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        for req in requests_data:
            request_info = req.get('request', {})
            redirects = json.dumps(request_info.get('redirects', []))

            request_detail = request_info.get('request', {})

            result = request_info.get('result', {})

            frame_info = req.get('frame', {})

            
            values = (
                session_id,
                request_info.get('request id'),
                request_info.get('request type'),
                request_detail.get('url'),
                result.get('size'),
                result.get('hash'),
                json.dumps(result.get('headers', [])),
                result.get('status'),
                frame_info.get('id'),
                frame_info.get('main frame', False),
                frame_info.get('url'),
                frame_info.get('security origin'),
                redirects
            )

            try:
                cursor.execute(query, values)
            except:
                print('Error in requests')

        cursor.close()

    def insert_js_calls(self, session_id, js_calls_data):


        print('inserting js-calls')

        if not js_calls_data or not js_calls_data['report']:
            return


        js_calls_data = js_calls_data["report"]
        cursor = self.connection.cursor()
        query = """
        INSERT INTO js_calls (session_id, caller_id, caller_type, caller_hash,
                            caller_url, executor_id, executor_tag, executor_attrs,
                            call_method, call_args, call_result, context_id,
                            context_main, context_url, context_origin)
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        for call in js_calls_data:

            caller = call.get('caller', {})
            executor = caller.get('executor', {})
            call_info = call.get('call', {})
            context = call_info.get('call context', {})

            result = call_info.get('result')
            if isinstance(result, (dict, list)):
                result = json.dumps(result, default=lambda x: float(x) if isinstance(x, decimal.Decimal) else x, ensure_ascii=False)

            elif result is not None:
                result = str(result)  # force into safe string
    
            call_args = json.dumps(call_info.get('args', []),
                default=lambda x: float(x) if isinstance(x, decimal.Decimal) else x,
                ensure_ascii=False)
            
            executor_attrs = json.dumps(executor.get('attrs', {}),
                            default=lambda x: float(x) if isinstance(x, decimal.Decimal) else x,
                            ensure_ascii=False)

            values = (
                session_id,
                caller.get('id'),
                caller.get('type'),
                caller.get('hash'),
                caller.get('url'),
                executor.get('id'),
                executor.get('tag'),
                executor_attrs,
                call_info.get('method'),
                call_args,
                result,
                context.get('id'),
                context.get('main frame', False),
                context.get('url'),
                context.get('security origin')
            )

   #         print("Executing query:", query)
   #         print("With values:", tuple(
   #             v
   #             for v in values
   #         ))
            
            try: 
                cursor.execute(query, values)
            except:
                print('Error in js-call')

        cursor.close()

    def get_already_treated_etlds(self, location, category):

        query = "select distinct etld from crawl_sessions where location = %s and category = %s"
        values = (location, category)
        cursor = self.connection.cursor()
        cursor.execute(query, values)
        res = cursor.fetchall()
        return [item[0] for item in res]



    def get_urls_for_etld_in_location(self, etld, location):
        query = "select distinct url from crawl_sessions where etld = %s and location = %s"
        values = (etld, location)
        cursor = self.connection.cursor()
        cursor.execute(query, values)
        res = cursor.fetchall()
        return [item[0] for item in res]


    def parse_line_for_etld(self, line):
        """Extract etld from a JSON line without parsing the entire line."""
        try:
            parser = ijson.parse(line)
            for prefix, event, value in parser:
                if prefix == "etld" and event == "string":
                    return value
        except Exception as e:
            print(f"Error parsing etld: {e}")
        return None


    def parse_line(self, line):
        """Parse a JSON line into (etld, urls_data). Returns (None, None) if invalid."""
        line = line.strip()
        if not line:
            return None, None

        try:
            obj = json.loads(line)
        except json.JSONDecodeError as e:
            print(f"Skipping invalid JSON line: {e}")
            return None, None

        return obj.get("etld"), obj.get("data")


    def process_etld_data(self, etld, urls_data, location, category):
        """Process one ETLD and insert all relevant data into DB."""
        for url, url_data in urls_data.items():

            print(f"Processing {etld} - {url} - {location} - {category}")
            session_id = self.insert_session(etld, url, location, category)
            self.insert_cookies(session_id, url_data.get('cookies') or [])
            self.insert_storage(session_id, url_data.get('local_storage') or [], 'local_storage')
            self.insert_storage(session_id, url_data.get('session_storage') or [], 'session_storage')
            self.insert_scripts(session_id, url_data.get('scripts') or [])
            self.insert_requests(session_id, url_data.get('requests') or [])
            self.insert_js_calls(session_id, url_data.get('js-calls') or [])


    def import_zst_file(self, zst_file_path, location, category):
        try:
            already_done = self.get_already_treated_etlds(location, category)
            print('Number of already treated etlds:', len(already_done))

            total_etlds = 0

            with open(zst_file_path, 'rb') as fh:  # open in binary
                dctx = zstd.ZstdDecompressor(max_window_size=2**31)
                with dctx.stream_reader(fh) as reader:
                    import io
                    text_stream = io.TextIOWrapper(reader, encoding='utf-8')
    
                    for line in text_stream:

                        total_etlds += 1

                        etld = self.parse_line_for_etld(line)

                        if not etld or etld in already_done:
                            print(f"Passing ETLD: {etld} | Total processed so far: {total_etlds}")
                            continue

                        print(f"Executing ETLD: {etld} | Total processed so far: {total_etlds}")

                        etld, urls_data = self.parse_line(line)

                        if not etld or not urls_data:
                            continue

                        self.process_etld_data(etld, urls_data, location, category)

                        if total_etlds % 10 == 0:
                            self.connection.commit()
                            print(f"Processed {total_etlds} etlds")

            self.connection.commit()
            print(f"Successfully imported {total_etlds} etlds")

        except Exception as e:
            print(f"Error: {e}")
            self.connection.rollback()


    def close_connection(self):
        if self.connection.is_connected():
            self.connection.close()

# Usage
if __name__ == "__main__":


    if len(sys.argv) != 4:
        print("Usage: python script.py <filename> <country> <category>")
        sys.exit(1)

    filename = sys.argv[1]  # just file name, not full path
    country = sys.argv[2]
    category = sys.argv[3]

    
    base_path = "../../data/files_to_analyze"
    file_path = os.path.join(base_path, filename)

    if not os.path.isfile(file_path):
        print(f"Error: file does not exist: {file_path}")
        sys.exit(1)


    importer = CrawlDataImporter(
        host=db_params["host"],
        database=db_params["db"],   # NOTE: db -> database
        user=db_params["user"],
        password=db_params["password"],
    )



    importer.import_zst_file(file_path, country, category)
    importer.close_connection()


