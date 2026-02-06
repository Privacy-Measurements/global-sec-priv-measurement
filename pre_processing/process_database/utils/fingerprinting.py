
import threading, pymysql, json
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from urllib.parse import parse_qs, unquote_plus
from collections import defaultdict
from config import db_params


def get_non_treated_sessions(db_params):

    conn = pymysql.connect(**db_params)
    cursor = conn.cursor()
    query = 'select id from crawl_sessions where fingerprinting is null or is_fingerprinting is null '
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    return [item['id'] for item in rows]

def split_list(lst, n):
    k, m = divmod(len(lst), n)
    return [lst[i*k + min(i, m):(i+1)*k + min(i+1, m)] for i in range(n)]

def get_js_calls_for_session(session_id, cursor):

    query = 'select * from js_calls where session_id = %s'
    cursor.execute(query, session_id)
    return cursor.fetchall()

def get_scripts_for_session(session_id, cursor):

    query = 'select script_id, script_hash, executor_attrs, script_type from scripts where session_id = %s'
    cursor.execute(query, session_id)
    return cursor.fetchall()

# Return true if all given js_calls are from the same script (caller)
def sanity_check_js_calls(js_calls, fields=['caller_id', 'caller_type', 'caller_hash', 'caller_url', 'executor_id', 'executor_tag', 'executor_attrs']):
   
    if not js_calls:
        return  

    base = js_calls[0]

    for call in js_calls[1:]:
        for field in fields:
            if call.get(field) != base.get(field):
                raise Exception(f"Sanity error for field '{field}': mismatch between calls")

def detect_canvas_image_fingerprinting(js_calls):

    # Track by caller_id
    usage = defaultdict(lambda: {
        'text_drawn': False,
        'style_set': False,
        'exported': False,
        'used_save_restore_or_listener': False,
        'entries': []  
    })

    for entry in js_calls:
        method = entry.get('call_method')
        caller_id = entry.get('caller_id')

        if method in ('CanvasRenderingContext2D.fillText', 'CanvasRenderingContext2D.strokeText'):
            usage[caller_id]['text_drawn'] = True

        elif method in ('CanvasRenderingContext2D.fillStyle.set', 'CanvasRenderingContext2D.strokeStyle.set'):
            usage[caller_id]['style_set'] = True

        elif method == 'HTMLCanvasElement.toDataURL':
            usage[caller_id]['exported'] = True

        elif method in ('CanvasRenderingContext2D.save', 'CanvasRenderingContext2D.restore', 'HTMLCanvasElement.addEventListener'):
            usage[caller_id]['used_save_restore_or_listener'] = True

        usage[caller_id]['entries'].append(entry)

    fingerprint_calls = []

    for caller_id, flags in usage.items():
        if (
            flags['text_drawn']
            and flags['style_set']
            and flags['exported']
            and not flags['used_save_restore_or_listener']
        ):
    
            sanity_check_js_calls(flags['entries'])  #This ensures that all calls are of the same caller -- raises an exception if not

            sample_entry = flags['entries'][0]

            script_info = {
                'caller_id': sample_entry.get('caller_id'),
                'caller_type': sample_entry.get('caller_type'),
                'caller_hash': sample_entry.get('caller_hash'),
                'caller_url': sample_entry.get('caller_url'),
                'executor_id': sample_entry.get('executor_id'),
                'executor_tag': sample_entry.get('executor_tag'),
                'executor_attrs': sample_entry.get('executor_attrs'),
            }

            fingerprint_calls.append(script_info)

    return fingerprint_calls

def detect_canvas_font_fingerprinting(js_calls):

    font_usage = defaultdict(set)      # caller_id -> set of font strings
    measure_counts = defaultdict(int)  # caller_id -> count of measureText calls
    caller_details = {}                # caller_id -> first seen metadata

    for entry in js_calls:
        method = entry.get('call_method')
        caller_id = entry.get('caller_id')

        if caller_id not in caller_details:
            caller_details[caller_id] = {
                'caller_id': entry.get('caller_id'),
                'caller_type': entry.get('caller_type'),
                'caller_hash': entry.get('caller_hash'),
                'caller_url': entry.get('caller_url'),
                'executor_id': entry.get('executor_id'),
                'executor_tag': entry.get('executor_tag'),
                'executor_attrs': entry.get('executor_attrs'),
            }

        if method == 'CanvasRenderingContext2D.font.set':

            args = json.loads(entry.get('call_args', '[]'))
            font = args[0]
            font_usage[caller_id].add(font)

        elif method == 'CanvasRenderingContext2D.measureText':
            measure_counts[caller_id] += 1


    fingerprint_calls = []
    for caller_id in font_usage:
        if len(font_usage[caller_id]) > 20 and measure_counts[caller_id] > 20:
            entry = caller_details[caller_id]
            fingerprint_calls.append(entry)

    return fingerprint_calls

def detect_webrtc_fingerprinting(js_calls):

    # Track by caller_id
    usage = defaultdict(lambda: {
        'channel_creation': False,
        'candidates_query': False,
        'entries': []  
    })

    for entry in js_calls:

        method = entry.get('call_method')
        caller_id = entry.get('caller_id')

        if method in ('RTCPeerConnection.createDataChannel', 'RTCPeerConnection.createOffer'):
            usage[caller_id]['channel_creation'] = True

        elif method in ('RTCPeerConnection.localDescription.get', 'RTCPeerConnection.onicecandidate.get'):
            usage[caller_id]['candidates_query'] = True

        usage[caller_id]['entries'].append(entry)

    fingerprint_calls = []

    for caller_id, flags in usage.items():

        if (flags['channel_creation'] and flags['candidates_query']):

            sanity_check_js_calls(flags['entries'])  #This ensures that all calls are of the same caller -- raises an exception if not

            sample_entry = flags['entries'][0]

            script_info = {
                'caller_id': sample_entry.get('caller_id'),
                'caller_type': sample_entry.get('caller_type'),
                'caller_hash': sample_entry.get('caller_hash'),
                'caller_url': sample_entry.get('caller_url'),
                'executor_id': sample_entry.get('executor_id'),
                'executor_tag': sample_entry.get('executor_tag'),
                'executor_attrs': sample_entry.get('executor_attrs'),
            }

            fingerprint_calls.append(script_info)

    return fingerprint_calls

def detect_audio_fingerprinting(js_calls):

    audio_apis = {
        'BaseAudioContext.createOscillator',
        'BaseAudioContext.createDynamicsCompressor'
        'BaseAudioContext.destination'
        'BaseAudioContext.startRendering',
        'BaseAudioContext.oncomplete',
        'AudioContext.createOscillator',
        'AudioContext.createDynamicsCompressor'
        'AudioContext.destination',
        'AudioContext.startRendering',
        'AudioContext.oncomplete',
        'OfflineAudioContext.createOscillator',
        'OfflineAudioContext.createDynamicsCompressor',
        'OfflineAudioContext.destination',
        'OfflineAudioContext.startRendering', 
        'OfflineAudioContext.oncomplete', #CHECK THE ONCOMPLETE EVENT
    }

    fingerprint_calls = {}
    
    for entry in js_calls:
        method = entry.get('call_method')
        caller_id = entry.get('caller_id')

        if method in audio_apis:
            if caller_id not in fingerprint_calls:
                fingerprint_calls[caller_id] = {
                    'caller_id': caller_id,
                    'caller_type': entry.get('caller_type'),
                    'caller_hash': entry.get('caller_hash'),
                    'caller_url': entry.get('caller_url'),
                    'executor_id': entry.get('executor_id'),
                    'executor_tag': entry.get('executor_tag'),
                    'executor_attrs': entry.get('executor_attrs'),
                }



    return list(fingerprint_calls.values())

def load_umar_list():

    unique_hashes = set()
    unique_urls = set()

    with open('privacy_lists/umar_iqbal_fingerprinting_list.json', 'r') as f:
        script_data = json.load(f)

    for _hash, entries in script_data.items():
        unique_hashes.add(_hash)
        for entry in entries:
            unique_urls.add(entry["script_url"])

    return list(unique_hashes), list(unique_urls)

def detect_umar_fingerprinting(scripts):

    ground_truth_hashes, ground_truth_urls = load_umar_list()

    matches = []

    for row in scripts:
        script_id = row["script_id"]
        script_type = row["script_type"]
        script_hash = row["script_hash"]
        executor_attrs = row.get("executor_attrs", {})

        if isinstance(executor_attrs, str):
            try:
                executor_attrs = json.loads(executor_attrs)
            except json.JSONDecodeError:
                executor_attrs = {}
        

        script_src = executor_attrs.get('src', '')


        if script_hash in ground_truth_hashes or script_src in ground_truth_urls:
            match_element = {
                "script_hash": script_hash, 
                "script_id": script_id,
                "script_type": script_type,
                "script_src": script_src
            }

            match_element["match_type"] = []

            if script_hash in ground_truth_hashes:
                match_element["match_type"].append('hash')

            if script_src in ground_truth_urls:
                match_element["match_type"].append('src')


            matches.append(match_element)

        
    return matches


def process_chunk(sessions, db_params, thread_id):
    conn = pymysql.connect(**db_params)
    cursor = conn.cursor()


    total = len(sessions)
    count = 0
    updates = []

    for session_id in sessions:

        js_calls_for_session = get_js_calls_for_session(session_id, cursor)
        scripts_for_session = get_scripts_for_session(session_id, cursor)

        ## JS APIS FINGERPRINTING
        canvas_image_fingerprinters = detect_canvas_image_fingerprinting(js_calls_for_session)
        canvas_font_fingerprinters = detect_canvas_font_fingerprinting(js_calls_for_session)
        webrtc_fingerprinters = detect_webrtc_fingerprinting(js_calls_for_session)
        audio_fingerprinters = detect_audio_fingerprinting(js_calls_for_session)


        # MATCHING AGAINST UMAR IQBAL'S FINGERPRINTING SCRIPTS 
        fingerprinting_scripts_umar = detect_umar_fingerprinting(scripts_for_session)



        if len(canvas_image_fingerprinters) + len(canvas_font_fingerprinters) + len(webrtc_fingerprinters) + len(audio_fingerprinters) + len(fingerprinting_scripts_umar) > 0:
            is_fingerprinting = True
        else:
            is_fingerprinting = False
            

        summary = {
            'canvas_image': canvas_image_fingerprinters,
            'canvas_font': canvas_font_fingerprinters,
            'webrtc': webrtc_fingerprinters,
            'audio': audio_fingerprinters,
            'matched_scripts': fingerprinting_scripts_umar
        }


        updates.append( (is_fingerprinting, json.dumps(summary), session_id) )


        count += 1
        if count % 1000 == 0:
            cursor.executemany(
                "UPDATE crawl_sessions SET is_fingerprinting = %s, fingerprinting = %s WHERE id = %s;",
                updates
            )
            conn.commit()
            print(f"[Thread {thread_id}] Treated {count} / {total} sessions.")
            updates.clear()


    if updates:
        cursor.executemany(
            "UPDATE crawl_sessions SET is_fingerprinting = %s, fingerprinting = %s WHERE id = %s;",
            updates
        )
        conn.commit()
        print(f"[Thread {thread_id}] Final commit. Total treated: {count}.")

  

    cursor.close()
    conn.close()


if __name__ == "__main__":

    
    sessions = get_non_treated_sessions(db_params)

    
    num_threads = 32
    sessions_chunks = split_list(sessions, num_threads)

    with ProcessPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for i, chunk in enumerate(sessions_chunks):
            futures.append(
                executor.submit(process_chunk, chunk, db_params, i)
            )

        for future in futures:
            future.result()


    print('done')
