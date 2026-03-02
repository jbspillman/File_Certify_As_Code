# exec_secrets.py
import os
import json

def get_creds(log, user_to_return) -> dict:
    folder_name = os.path.dirname(os.path.abspath(__file__))
    credentials_cache = os.path.join(folder_name, '.secrets')
    
    safe_file = os.path.join(credentials_cache, "credentials.safe")
    if not os.path.exists(safe_file):
        log.warning(f'There is no credentials stored for this application.')
        log.info(f'Please create: {safe_file}.')
        example = {
                    "id": "key_id_or_account_name",
                    "device": "supermodel.beastmode.local.net",
                    "domain": "KANSAS",
                    "username": "pheel",
                    "password": "morebutts"
                  }
        pretty_example = json.dumps(example, indent=4)
        log.info(f'Example Dictionary:\n {pretty_example}')
        return None
    
    if user_to_return is None or len(user_to_return) < 3:
        return None


    try:
        with open(safe_file, 'r', encoding="utf-8") as file:
            secrets_list = json.load(file)
    except FileNotFoundError:
        log.critical(f"Error: The file '{safe_file}' was not found.") # Handles a missing file
        return {}
    except json.JSONDecodeError:
        log.critical("Error: Failed to decode JSON from the file. Check for malformed JSON data.") # Handles invalid JSON syntax
        return {}

    def get_credentials(creds, target_id):
        return next((c for c in creds if c['id'] == target_id), None)

    result = get_credentials(secrets_list, user_to_return)
    if result is None:
        return None
    else:
        return result




