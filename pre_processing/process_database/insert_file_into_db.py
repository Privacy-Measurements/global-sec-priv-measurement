import json
import mysql.connector
from mysql.connector import Error
import sys
import os
import decimal
import io
import math
import traceback
import zstandard as zstd
from utils.config import db_params


MAX_TEXT_FIELD = 64000
BATCH_SIZE = 1


class CrawlDataImporter:

    COUNTRY_TO_LOCATION = {
        'DZ': 'ALGERIA',
        'DE': 'GERMANY',
        'AE': 'UAE',
        'IN': 'INDIA',
        'US': 'USA',
        'FR': 'FRANCE',
        'ALGERIA': 'ALGERIA',
        'GERMANY': 'GERMANY',
        'UAE': 'UAE',
        'INDIA': 'INDIA',
        'USA': 'USA',
        'FRANCE': 'FRANCE',
        'ALGERIA-VPN': 'ALGERIA-VPN',
        'GERMANY-VPN': 'GERMANY-VPN',
        'UAE-VPN': 'UAE-VPN',
        'INDIA-VPN': 'INDIA-VPN',
        'USA-VPN': 'USA-VPN',
        'FRANCE-VPN': 'FRANCE-VPN',
    }

    def __init__(self, host, database, user, password):
        self.connection = None
        self.db_config = {
            'host': host,
            'database': database,
            'user': user,
            'password': password,
        }
        try:
            self.connection = mysql.connector.connect(**self.db_config)
            print("Successfully connected to MySQL database")
        except Error as e:
            print(f"Error connecting to MySQL: {e}")
            sys.exit(1)

    def is_connection_lost_error(self, err):
        errno = getattr(err, 'errno', None)
        text = str(err).lower()
        return errno in (2006, 2013, 2055) or 'lost connection' in text or 'connection not available' in text

    def reconnect(self):
        try:
            if self.connection and self.connection.is_connected():
                self.connection.close()
        except Exception:
            pass
        self.connection = mysql.connector.connect(**self.db_config)
        print("Reconnected to MySQL database")

    def execute_with_retry(self, cursor, query, values, context='query', retries=1):
        attempt = 0
        while True:
            try:
                if self.connection is None or not self.connection.is_connected():
                    self.reconnect()
                    try:
                        cursor.close()
                    except Exception:
                        pass
                    cursor = self.connection.cursor()
                cursor.execute(query, values)
                return cursor
            except Error as err:
                if self.is_connection_lost_error(err) and attempt < retries:
                    attempt += 1
                    print(f"Lost MySQL connection during {context}; reconnecting and retrying ({attempt}/{retries})")
                    self.reconnect()
                    try:
                        cursor.close()
                    except Exception:
                        pass
                    cursor = self.connection.cursor()
                    continue
                raise

    def safe_commit(self):
        try:
            self.connection.commit()
        except Error as err:
            if self.is_connection_lost_error(err):
                print("Lost MySQL connection during commit; reconnecting and retrying commit")
                self.reconnect()
                self.connection.commit()
            else:
                raise

    def clamp_text(self, value):
        if isinstance(value, str) and len(value) > MAX_TEXT_FIELD:
            return value[:MAX_TEXT_FIELD]
        return value

    def sanitize_for_json(self, value):
        if isinstance(value, dict):
            return {k: self.sanitize_for_json(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self.sanitize_for_json(v) for v in value]
        if isinstance(value, decimal.Decimal):
            return float(value)
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return None
        return value

    def dumps_json_strict(self, value):
        cleaned = self.sanitize_for_json(value)
        # Strict JSON for MySQL JSON columns: no NaN/Infinity; ASCII-escaped for robustness.
        return json.dumps(cleaned, ensure_ascii=True, allow_nan=False)

    def chunked(self, items, chunk_size):
        for i in range(0, len(items), chunk_size):
            yield items[i:i + chunk_size]

    def execute_many_with_retry(self, cursor, query, values_list, context='query', retries=1):
        attempt = 0
        while True:
            try:
                if self.connection is None or not self.connection.is_connected():
                    self.reconnect()
                    try:
                        cursor.close()
                    except Exception:
                        pass
                    cursor = self.connection.cursor()
                cursor.executemany(query, values_list)
                return cursor
            except Error as err:
                if self.is_connection_lost_error(err) and attempt < retries:
                    attempt += 1
                    print(f"Lost MySQL connection during {context}; reconnecting and retrying ({attempt}/{retries})")
                    self.reconnect()
                    try:
                        cursor.close()
                    except Exception:
                        pass
                    cursor = self.connection.cursor()
                    continue
                raise

    def insert_rows_batched(self, cursor, query, rows, context):
        for batch in self.chunked(rows, BATCH_SIZE):
            try:
                cursor = self.execute_many_with_retry(cursor, query, batch, context=f"{context} batch", retries=1)
            except Exception as e:
                # Keep existing behavior: skip bad rows rather than fail the whole import.
                print(f"Error in {context} batch: {e}; falling back to row-by-row")
                for values in batch:
                    try:
                        cursor = self.execute_with_retry(cursor, query, values, context=context, retries=1)
                    except Exception as row_err:
                        print(f"Error in {context}: {row_err}")
        return cursor

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

    def extract_report_items(self, section_data):
        if not section_data:
            return []

        if isinstance(section_data, dict):
            report = section_data.get('report')
            if isinstance(report, list):
                return report
            return []

        if isinstance(section_data, list):
            return section_data

        return []

    def normalize_location(self, raw_location):
        if raw_location is None:
            return None

        return self.COUNTRY_TO_LOCATION.get(str(raw_location).strip().upper(), raw_location)

    def extract_request_url(self, request_value):
        if isinstance(request_value, dict):
            return request_value.get('url')

        if isinstance(request_value, str):
            return request_value

        return None

    def insert_session(self, etld, url, location, category, graphml_path, access_id):

        print('Inserting session')

        cursor = self.connection.cursor()
        query = """
        INSERT INTO crawl_sessions (etld, url, location, category, graphml_path, access_id) 
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(query, (etld, url, location, category, graphml_path, access_id))
        session_id = cursor.lastrowid
        cursor.close()
        return session_id

    def insert_cookies(self, session_id, cookies_data):

        print('Inserting cookies')

        cookies_data = self.extract_report_items(cookies_data)
        if not cookies_data:
            return

        cursor = self.connection.cursor()
        query = """
        INSERT INTO cookies (session_id, edge_id, event_type, storage_key, 
                           storage_value, caller_id, caller_type, caller_hash, 
                           script_type, caller_url) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        rows = []
        for cookie in cookies_data:
            rows.append((
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
            ))

        cursor = self.insert_rows_batched(cursor, query, rows, context='cookies')


        cursor.close()

    def insert_storage(self, session_id, storage_data, table_name):

        print('Inserting storage', table_name)

        storage_data = self.extract_report_items(storage_data)
        if not storage_data:
            return
            
        cursor = self.connection.cursor()
        query = f"""
        INSERT INTO {table_name} (session_id, storage_key, storage_value, parsed_value,
                                caller_id, caller_type, caller_hash, caller_url,
                                is_identifier)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        rows = []
        for item in storage_data:
            rows.append((
                session_id,
                item.get('storage key'),
                item.get('storage value'),
                json.dumps(item.get('parsed_value')) if item.get('parsed_value') is not None else None,
                item.get('caller', {}).get('id'),
                item.get('caller', {}).get('type'),
                item.get('caller', {}).get('hash'),
                item.get('caller', {}).get('url'),
                item.get('is_identifier'),
            ))

        cursor = self.insert_rows_batched(cursor, query, rows, context=f'storage {table_name}')
        cursor.close()

    def insert_scripts(self, session_id, scripts_data):

        print('Inserting scripts')

        scripts_data = self.extract_report_items(scripts_data)
        if not scripts_data:
            return
            
        cursor = self.connection.cursor()
        query = """
        INSERT INTO scripts (session_id, script_id, script_type, script_hash, script_src, executor_id, executor_tag, executor_attrs, frame_id,
                           frame_main, frame_url, frame_origin, frame_blink_id, creator_id, creator_type, executor_type, parent_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        rows = []
        for script in scripts_data:
            script_info = script.get('script', {})
            frame_info = script.get('frame', {})
            executor = script_info.get('executor', {})
            
            rows.append((
                session_id,
                script_info.get('id'),
                script_info.get('type'),
                script_info.get('hash'),
                script_info.get('src'),  
                executor.get('id'),
                executor.get('tag'),
                json.dumps(executor.get('attrs', {})),
                frame_info.get('id'),
                frame_info.get('main frame', False),
                frame_info.get('url'),
                frame_info.get('security origin'),
                frame_info.get('blink id'),
                script.get('creator_id'),
                script.get('creator_type'),
                script.get('executor_type'),
                script.get('parent_id')

            ))

        cursor = self.insert_rows_batched(cursor, query, rows, context='scripts')
        cursor.close()

    def insert_requests(self, session_id, requests_data):

        print('inserting requests')


        requests_data = self.extract_report_items(requests_data)
        if not requests_data:
            return
            
        cursor = self.connection.cursor()
        query = """
        INSERT INTO requests (session_id, request_id, request_type, request_url,
                            result_size, result_hash, result_headers, result_status,
                            requester_id, requester_type, parent_id,
                            frame_id, frame_main, frame_url, frame_origin, frame_blink_id,
                            redirects)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        rows = []
        for req in requests_data:
            request_info = req.get('request', {})
            redirects = json.dumps(request_info.get('redirects', []))

            request_detail = request_info.get('request')

            result = request_info.get('result', {})

            frame_info = req.get('frame', {})

            
            rows.append((
                session_id,
                request_info.get('request id'),
                request_info.get('request type'),
                self.extract_request_url(request_detail),
                result.get('size'),
                result.get('hash'),
                json.dumps(result.get('headers', [])),
                result.get('status'),
                req.get('requester_id'), 
                req.get('requester_type'), 
                req.get('parent_id'),
                frame_info.get('id'),
                frame_info.get('main frame', False),
                frame_info.get('url'),
                frame_info.get('security origin'),
                frame_info.get('blink id'),
                redirects
        ))

        cursor = self.insert_rows_batched(cursor, query, rows, context='requests')

        cursor.close()

    def insert_js_calls(self, session_id, js_calls_data):


        print('inserting js-calls')

        js_calls_data = self.extract_report_items(js_calls_data)
        if not js_calls_data:
            return

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
        
        rows = []
        for call in js_calls_data:

            caller = call.get('caller', {})
            executor = caller.get('executor', {})
            call_info = call.get('call', {})
            context = call_info.get('call context', {})

            result = call_info.get('result')
            if isinstance(result, (dict, list)):
                result = self.dumps_json_strict(result)
            elif isinstance(result, decimal.Decimal):
                result = str(float(result))
            elif result is not None:
                result = str(result)

            call_args = self.dumps_json_strict(call_info.get('args', []))
            executor_attrs = self.dumps_json_strict(executor.get('attrs', {}))

            rows.append((
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
                self.clamp_text(result),
                context.get('id'),
                context.get('main frame', False),
                context.get('url'),
                context.get('security origin')
            ))

        cursor = self.insert_rows_batched(cursor, query, rows, context='js-call')

        cursor.close()

    def insert_html_elements(self, session_id, html_elements_data):
        """Insert HTML elements from pagegraph html-elements command"""
        print('inserting html-elements')

        html_elements_data = self.extract_report_items(html_elements_data)
        if not html_elements_data:
            return

        cursor = self.connection.cursor()
        query = """
        INSERT INTO html_elements (session_id, element_id, tag, site, url, src, 
                                  attrs_json, creator_id, creator_type, 
                                  dom_parent_id, dom_parent_type, parent_id, parent_type)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        rows = []
        for elem in html_elements_data:
            rows.append((
                session_id,
                elem.get('id'),
                elem.get('tag'),
                elem.get('site'),
                elem.get('url'),
                elem.get('src'),
                json.dumps(elem.get('attrs', {})),
                elem.get('creator_id'),
                elem.get('creator_type'),
                elem.get('dom_parent_id'),
                elem.get('dom_parent_type'),
                elem.get('parent_id'),
                elem.get('parent_type')
            ))

        cursor = self.insert_rows_batched(cursor, query, rows, context='html-elements')

        cursor.close()

    def insert_har_requests(self, session_id, har_data):
        """Insert HAR requests from data_from_har"""
        print('inserting har_requests')

        if not har_data or not isinstance(har_data, dict):
            return

        requests = har_data.get('requests', [])
        if not requests:
            return

        cursor = self.connection.cursor()
        query = """
        INSERT INTO har_requests (session_id, url, domain, party_type, resource_type,
                                 is_main_frame, is_sub_frame, frame_origin, method, 
                                 status, status_text, mime_type, size, time, 
                                 started_datetime, request_headers, response_headers,
                                 initiator, server_ip, connection, cache)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        rows = []
        for har_req in requests:
            rows.append((
                session_id,
                har_req.get('url'),
                har_req.get('domain'),
                har_req.get('party_type'),
                har_req.get('resource_type'),
                har_req.get('is_main_frame'),
                har_req.get('is_sub_frame'),
                har_req.get('frame_origin'),
                har_req.get('method'),
                har_req.get('status'),
                har_req.get('status_text'),
                har_req.get('mime_type'),
                har_req.get('size'),
                har_req.get('time'),
                har_req.get('started_datetime'),
                json.dumps(har_req.get('request_headers', [])),
                json.dumps(har_req.get('response_headers', [])),
                json.dumps(har_req.get('initiator', {})),
                har_req.get('server_ip'),
                har_req.get('connection'),
                json.dumps(har_req.get('cache', {}))
            ))

        cursor = self.insert_rows_batched(cursor, query, rows, context='har_requests')

        cursor.close()

    def get_already_treated_etlds(self, location, category):

        query = "select distinct etld from crawl_sessions where location = %s and category = %s"
        values = (location, category)
        cursor = self.connection.cursor()
        cursor.execute(query, values)
        res = cursor.fetchall()
        cursor.close()
        return [item[0] for item in res]

    def process_entry(self, etld, access_id, url, graphml_path, entry_data, location, category):
        """Process one entry and insert all relevant data into DB."""
        print(f"Processing {etld} - {access_id} - {url} - {location} - {category}")
        
        session_id = self.insert_session(etld, url, location, category, 
                                        graphml_path=graphml_path, access_id=access_id)
        self.insert_cookies(session_id, entry_data.get('cookies') or {})
        self.insert_storage(session_id, entry_data.get('local_storage') or {}, 'local_storage')
        self.insert_storage(session_id, entry_data.get('session_storage') or {}, 'session_storage')
        self.insert_scripts(session_id, entry_data.get('scripts') or {})
        self.insert_requests(session_id, entry_data.get('requests') or {})
        self.insert_js_calls(session_id, entry_data.get('js-calls') or {})
        self.insert_html_elements(session_id, entry_data.get('html_elements') or {})
        self.insert_har_requests(session_id, entry_data.get('data_from_har') or {})

    def iter_zst_jsonl(self, zst_file_path):
        """Yield JSON objects line-by-line from a .zst-compressed JSONL file."""
        with open(zst_file_path, 'rb') as compressed_file:
            dctx = zstd.ZstdDecompressor(max_window_size=2**31)
            with dctx.stream_reader(compressed_file) as reader:
                text_stream = io.TextIOWrapper(reader, encoding='utf-8')
                for line_no, raw_line in enumerate(text_stream, start=1):
                    line = raw_line.strip()
                    if not line:
                        continue
                    try:
                        parsed = json.loads(line)
                    except json.JSONDecodeError as exc:
                        raise RuntimeError(f'Invalid JSONL at line {line_no}: {exc}') from exc

                    if not isinstance(parsed, dict):
                        raise RuntimeError(f'Invalid JSONL record at line {line_no}: expected object')
                    yield parsed

    def import_zst_file(self, zst_file_path):
        """Import data from a compressed JSONL .zst file (one site per line)."""
        try:
            print(f"Loading Zstandard JSONL file: {zst_file_path}")
            total_entries = 0
            total_sites = 0
            treated_cache = {}

            for record in self.iter_zst_jsonl(zst_file_path):
                raw_location = (
                    record.get('metadata', {}).get('location')
                    or record.get('location')
                    or record.get('country')
                )
                location = self.normalize_location(raw_location)
                category = record.get('metadata', {}).get('category') or record.get('category')
                etld = record.get('site') or record.get('etld')
                acc_map = record.get('data', {})

                if not location or not category or not etld:
                    print('Skipping JSONL record missing location/category/site')
                    continue
                if not isinstance(acc_map, dict):
                    print(f'Skipping invalid data block for site={etld}: expected object')
                    continue

                cache_key = (location, category)
                if cache_key not in treated_cache:
                    done_list = self.get_already_treated_etlds(location, category)
                    treated_cache[cache_key] = set(done_list)
                    print(
                        f'Loaded already treated etlds for location={location}, '
                        f'category={category}: {len(done_list)}'
                    )

                already_done = treated_cache[cache_key]
                if etld in already_done:
                    print(f'Passing ETLD: {etld} (already in DB)')
                    continue

                total_sites += 1
                print(f'Executing ETLD: {etld} | Sites processed so far: {total_sites}')

                for access_id, entries in acc_map.items():
                    if not isinstance(entries, list):
                        entries = [entries]

                    for entry in entries:
                        total_entries += 1

                        url = entry.get('url')
                        graphml_path = entry.get('graphml_path')

                        if not url:
                            print(f"Skipping entry without URL: ETLD={etld}, access_id={access_id}")
                            continue
                        
                        if etld in already_done:
                            print(f"Passing ETLD: {etld} | Total entries processed so far: {total_entries}")
                            continue

                        print(f"Executing ETLD: {etld} (access_id: {access_id}) | Total entries so far: {total_entries}")
                        
                        self.process_entry(etld, access_id, url, graphml_path, entry, location, category)

                        if total_entries % 1 == 0:
                            self.safe_commit()
                            print(f"Processed {total_entries} entries - committing")

                already_done.add(etld)

            self.safe_commit()
            print(f"Successfully imported {total_entries} entries across {total_sites} sites")

        except Exception as e:
            print(f"Error: {e}")
            traceback.print_exc()
            try:
                if self.connection and self.connection.is_connected():
                    self.connection.rollback()
            except Exception:
                pass

    def close_connection(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()

# Usage
if __name__ == "__main__":

    if len(sys.argv) != 2:
        print("Usage: python insert_file_into_db.py <input_file.zst>")
        print("Note: Expects compressed JSONL (.jsonl.zst); location and category are read per JSONL row")
        sys.exit(1)

    zst_file_path = sys.argv[1]

    if not os.path.isfile(zst_file_path):
        print(f"Error: file does not exist: {zst_file_path}")
        sys.exit(1)

    importer = CrawlDataImporter(
        host=db_params["host"],
        database=db_params.get("db") or db_params.get("database"),
        user=db_params["user"],
        password=db_params["password"],
    )

    importer.import_zst_file(zst_file_path)
    importer.close_connection()