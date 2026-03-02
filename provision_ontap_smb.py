from exec_logger import title_large , title_small
from exec_secrets import get_creds
from exec_api import API_Connection_Manager
from exec_ssh import SSHClient
import json
import time
import os

def create_ontap_smb(log, key_info, smb_access_info):
    
    """
    We can use qtrees to simplify the setup here.
    +--------------------+-------------+----------------+------------------------+-----------------------------------------------+
    | Share Name         | Volume Size | Auth Type      | Special Config         | Purpose                                       |
    +--------------------+-------------+----------------+------------------------+-----------------------------------------------+
    | CERTIFY_ANON       | 5 GB        | Anonymous      | Guest access enabled   | Anonymous enumeration + access-blocked tests  |
    | CERTIFY_GENERAL    | 5 GB        | Domain + Local | Standard RW            | File ops, locking, concurrent I/O,            |
    |                    |             |                |                        | dialect/signing/encryption                    |
    | CERTIFY_LARGE_IO   | 5 GB        | Domain         | Standard RW            | Large file write + checksum, many small files |
    | CERTIFY_ACL_ALLOW  | 5 GB        | Domain         | Explicit allow ACE     | ACL allow, inherit tests                      |
    | CERTIFY_ACL_DENY   | 5 GB        | Domain         | Explicit deny ACE      | ACL deny tests                                |
    | CERTIFY_AUDIT      | 5 GB        | Domain         | SACL auditing enabled  | Audit event tests (create, delete, deny)      |
    +--------------------+-------------+----------------+------------------------+-----------------------------------------------+    
    """
    def pj(input_json):
        return json.dumps(input_json, indent=4)


    # pretty_key1 = 
    # pretty_key2 = json.dumps(smb_access_info, indent=4)
    # log.info(f'\n {pretty_key1}')
    # log.info(f'\n {pretty_key2}')

    device = key_info["storage-mgmt"]
    storage_api = key_info["storage-api"]
    namespace_smb = key_info["storage-namespace_smb"]
    svm_name = namespace_smb
    api_base = storage_api.replace('<MGMT>', device)
    
    # log.info(device)
    # log.info(api_base)
    # log.info(namespace_smb)

    mgmt_name = device.split('.')[0]
    creds = get_creds(log, mgmt_name)
    super_user = creds["username"]
    super_pass = creds["password"]

    star1 = f'*'
    star2 = f'**'

    
    
    
    with API_Connection_Manager(api_base, username=super_user, password=super_pass, verify_ssl=False) as api:

        def get_svm_uuid(svm_name):
            ep = f'/svm/svms?name={svm_name}'
            data = api.get(ep)
            svm_uuid = None
            try:
                records = data["records"]
                for record in records:           
                    if record["name"] == svm_name:
                        svm_uuid = record["uuid"]
                        return svm_uuid
                return svm_uuid
            except KeyError:
                log.error('✗ unable to get svms from cluster.')
                return None
        
        def get_volumes(svm_name, svm_uuid):
            volume_name_for_testing = f'certify_smb_access'
            vol_increment = False
            volume_number = 1
            ep = f'/storage/volumes?is_constituent=false&svm.name={svm_name}&svm.uuid={svm_uuid}&fields={star1}&return_records=true&return_timeout=120'
            data = api.get(ep)
            try:
                records = data["records"]
                for record in records:
                    volume_name = record["name"]
                    if volume_name_for_testing.lower() in str(volume_name).lower():
                        if vol_increment:
                            volume_number += 1
                            log.step(f'Increment volume: {volume_name}')
                        else:
                            log.step(f'Skipping volume: {volume_name}')
                zvol_num = str(volume_number).zfill(3)
                test_volume_name = f"{volume_name_for_testing}_{zvol_num}"
                return test_volume_name
            except KeyError:
                log.error('✗ Unable to get volumes from cluster.')
                return None

        def get_aggrs_least_used():
            ep = f'/storage/aggregates?state=online&fields={star1}&return_records=true&return_timeout=120'
            data = api.get(ep)
            pct_used = 1000
            aggr_least_used = {}
            try:
                records = data["records"]
                for item in records:
                    aggregate_uuid = item['uuid']
                    aggregate_name = item['name']
                    aggregate_total_gib = item['space']['block_storage']['size'] / (1024 ** 3)
                    aggregate_used_gib = item['space']['block_storage']['used'] / (1024 ** 3)
                    aggregate_avail_gib = item['space']['block_storage']['available'] / (1024 ** 3)
                    aggregate_used_p = item['space']['block_storage']['used_percent']
                    
                    if aggregate_avail_gib > 9 and aggregate_used_p < pct_used:
                        log.step(f'utilization lowest: aggregate_name: {aggregate_name} | aggregate_total_gib: {aggregate_total_gib} | aggregate_used_p: {aggregate_used_p}')
                        pct_used = aggregate_used_p
                        aggr_least_used = {
                            "aggregate_name": aggregate_name,
                            "aggregate_uuid": aggregate_uuid,
                            "aggregate_uuid": aggregate_uuid,
                            "aggregate_total_gib": aggregate_total_gib,
                            "aggregate_avail_gib": aggregate_avail_gib,
                            "aggregate_used_gib": aggregate_used_gib,
                            "aggregate_used_p": aggregate_used_p
                        }
                return aggr_least_used
            except KeyError:
                log.error('✗ Unable to get aggregates from cluster.')
                return aggr_least_used

        def create_test_volume(svm_name, svm_uuid, certify_test_volume_name, size_str, sec_style, aggregate_name, aggregate_uuid):
            ''' POST to /storage/volumes '''

            volume_payload = {
                "_tags": [
                    "team:storage_engineering",
                    "environment:certification"
                ],
                "svm": {
                    "name": f"{svm_name}",
                    "uuid": f"{svm_uuid}"
                },
                "aggregates": [
                    {
                        "name": f"{aggregate_name}",
                        "uuid": f"{aggregate_uuid}"
                    }
                ],
                "name": f"{certify_test_volume_name}",
                "size": f"{size_str}",
                "state": "online",
                "style": "flexvol",
                "type": "rw",
                "nas": {
                    "security_style": f"{sec_style}",
                    "path": f"/{certify_test_volume_name}",
                    "export_policy": {
                        "name": "default"
                    },
                },
                "snapshot_policy": {
                    "name": "none"
                },
                "space": {
                    "snapshot": {
                        "reserve_percent": 0
                    }
                },
                "qos": {
                    "policy": {
                        "name": "performance"
                    }
                }
            }

            vol_rc, vol_msg = api.post(f'/storage/volumes?return_timeout=120&return_records=false', payload=volume_payload)
            if vol_rc == 201:
                log.success(f' ✓ Success - Volume Created (201) for SMB Testing: {svm_name}:{certify_test_volume_name}')
                job_state = "success"
                job_uuid = vol_msg["job"]["uuid"]
                return job_state, job_uuid
            elif vol_rc == 409:
                log.success(f' ✓ Success - Volume Exists (409) for SMB Testing: {svm_name}:{certify_test_volume_name}')
                job_state = "success"
                job_uuid = 409
                return job_state, job_uuid
            else:
                log.error(f' ✗ Failure - Volume provisioning failed : {vol_rc} | {vol_msg}')
                return None, None

        def create_qtree(svm_name, certify_test_volume_name, qtree_name):
            ''' POST to /storage/qtrees '''
            qtree_payload = {
                "svm": {
                    "name": f"{svm_name}"
                },
                "volume": {
                    "name": f"{certify_test_volume_name}",
                },
                "name": f"{qtree_name}"
            }

            qt_rc, qt_msg = api.post(f'/storage/qtrees?return_timeout=120&return_records=false', payload=qtree_payload)
            if qt_rc == 201:
                log.success(f' ✓ Success - Qtree Created (201) for SMB Testing: {svm_name}:/{certify_test_volume_name}/{qtree_name}')
                job_state = "success"
                job_uuid = qt_msg["job"]["uuid"]
                return job_state, job_uuid
            elif qt_rc == 400:
                log.success(f' ✓ Success - Qtree Exists (400) for SMB Testing: {svm_name}:/{certify_test_volume_name}/{qtree_name}')
                job_state = "success"
                job_uuid = 400
                return job_state, job_uuid
            else:
                log.error(f' ✗ Failure - Qtree provisioning failed : {qtree_name} > {qt_rc} | {qt_msg}')
                return None, None

        def create_shares(svm_name, share_name, share_path, share_comment, share_properties, acls_list):
            ''' POST to /protocols/cifs/shares '''
            
            share_payload = {
                "svm": {
                    "name": f"{svm_name}"
                },
                "name": f"{share_name}",
                "path": f"{share_path}",
                "comment": f"{share_comment}",
                "acls": acls_list
            }
            merge_payload = share_payload | share_properties
            
            shr_rc, shr_msg = api.post(f'/protocols/cifs/shares?return_timeout=120&return_records=false', payload=merge_payload)
            if shr_rc == 201:
                log.success(f' ✓ Success - Share Created (201) for SMB Testing: \\\\{svm_name}\\{share_name}')
                job_state = "success"
                try:
                    job_uuid = shr_msg["job"]["uuid"]
                except KeyError:
                    job_uuid = None
                # log.debug(shr_msg)
                return job_state, job_uuid
            elif shr_rc == 409:
                log.success(f' ✓ Success - Share Exists (409) for SMB Testing: \\\\{svm_name}\\{share_name}')
                job_state = "success"
                job_uuid = 409
                return job_state, job_uuid
            else:
                log.error(f' ✗ Failure - Share provisioning failed : {share_name} > {shr_rc} | {shr_msg}')
                # log.debug(shr_msg)
                return None, None

        def check_job_status(job_state, job_uuid):
            max_loops = 9
            loop_int = 0
            while job_state != "success":
                loop_int += 1
                jobs_ep = f'/cluster/jobs/{job_uuid}?fields=*'
                data = api.get(jobs_ep)
                job_state = data["state"]
                job_message = data["message"]
                job_code = data["code"]
                if job_state == 'success':
                    log.success(f'✓ Job Completed. {loop_int} {job_code} {job_message}')
                    return True
                else:
                    log.step(f'⌛ Waiting on Job Completion status. {loop_int} {job_code} {job_message}')
                    time.sleep(3)
                if loop_int > max_loops:
                    log.error(f'✗ Unable to determine job status:  {data}')
                    return False



        ''' Return the data needed to create a volume for testing '''
        svm_uuid = get_svm_uuid(svm_name)
        volume_name = get_volumes(svm_name, svm_uuid)
        aggr_least_used = get_aggrs_least_used()

        aggregate_name = aggr_least_used["aggregate_name"]
        aggregate_uuid = aggr_least_used["aggregate_uuid"]
        # total_gib = aggr_least_used["aggregate_total_gib"]
        # used_gib = aggr_least_used["aggregate_used_gib"]
        # avail_gib = aggr_least_used["aggregate_avail_gib"]

        ''' Send for volume creation. '''
        size_str = "1G"
        sec_style = "ntfs"
        job_check = True

        log.step(f'Sending Volume Creation on SVM: {svm_name} VOL: {volume_name} | AGGR: {aggregate_name} | SIZE: {size_str} | SECURITY: {sec_style}')
        job_state, job_uuid = create_test_volume(svm_name, svm_uuid, volume_name, size_str, sec_style, aggregate_name, aggregate_uuid)

        if job_state == "success" and job_uuid != 409:
            log.step(f'Checking for Volume creation job status: {job_state} {job_uuid}')
            job_check = check_job_status(job_state, job_uuid)
        

        if not job_check:
            log.error(f'✗ Will not continue to provision Qtrees until after volume is created successfully..')
            return False


        # log.info(f'{pj(smb_access_info)}')
        
        ad_domain_name = smb_access_info["smb_domain_name"]
        smb_storage_admin = smb_access_info["smb_storage_admin"]
        for domain_item in smb_access_info["domain_groups"]:
            if 'FULL' in domain_item["name"]:
                ad_full_access_group = domain_item["name"]               
            elif 'MODIFY' in domain_item["name"]:
                ad_modify_access_group = domain_item["name"]
            elif 'READ' in domain_item["name"]:
                ad_read_access_group = domain_item["name"]
                
        share_acl_list = [
            {
                "user_or_group": "Everyone",
                "type": "windows",
                "permission": "no_access"
            },
            {
                "user_or_group": f"{ad_domain_name}\\{ad_read_access_group}",
                "type": "windows",
                "permission": "read"
            },
            {
                "user_or_group": f"{ad_domain_name}\\{ad_modify_access_group}",
                "type": "windows",
                "permission": "change"
            },       
            {
                "user_or_group": f"{ad_domain_name}\\{ad_full_access_group}",
                "type": "windows",
                "permission": "full_control"
            },        
            {
                "user_or_group": f"{ad_domain_name}\\{smb_storage_admin}",
                "type": "windows",
                "permission": "full_control"
            }
        ]

        
        # "allow_unencrypted_access": True,    Specifies whether or not the SMB2 clients are allowed to access the encrypted share.
        # "encryption": False,                 Specifies that SMB encryption must be used when accessing this share. Clients that do not support encryption are not able to access this share.
        # "access_based_enumeration": False    If enabled, all folders inside this share are visible to a user based on that individual user access right; prevents the display of folders or other shared resources that the user does not have access to.
        # "continuously_available": False,     Specifies whether or not the clients connecting to this share can open files in a persistent manner. Files opened in this way are protected from disruptive events, such as, failover and giveback.
        # "oplocks": True,                     Specify whether opportunistic locks are enabled on this share. "Oplocks" allow clients to lock files and cache content locally, which can increase performance for file operations.
        # "show_snapshot": False,              Specifies whether or not the snapshots can be viewed and traversed by clients.
        # "show_previous_versions": True,      Specifies that the previous version can be viewed and restored from the client.

        ''' Send for qtree shares creations. '''    
        qtree_list = [
            {
                "qtree": "smbX_none",
                "share_description": "Allow SMB2,3 Clients with no need for encryption.",
                "share_properties": {
                    "allow_unencrypted_access": True,
                    "encryption": False,
                },
                "share_acls": share_acl_list
            },
            {
                "qtree": "smb3_none",
                "share_description": "Deny SMB2 Allow SMB3 Clients with no need for encryption.",
                "share_properties": {
                    "allow_unencrypted_access": False,
                    "encryption": False,
                },
                "share_acls": share_acl_list
            },
            {
                "qtree": "smbX_encr",
                "share_description": "Allow SMB2,3 Clients but require encryption <might forces smb2 clients to be denied>.",
                "share_properties": {
                    "allow_unencrypted_access": True,
                    "encryption": True,
                },
                "share_acls": share_acl_list
            },
            {
                "qtree": "smbX_abe",
                "share_description": "Demonstrate Acess Based Enumeration by enabling it.",
                "share_properties": {
                    "allow_unencrypted_access": True,
                    "encryption": False,
                    "access_based_enumeration": True
                },
                "share_acls": share_acl_list
            },
            {
                "qtree": "smbX_cont_avail",
                "share_description": "Test continous availabilty.",
                "share_properties": {
                    "allow_unencrypted_access": True,
                    "encryption": False,
                    "continuously_available": True
                },
                "share_acls": share_acl_list
            },    
            {
                "qtree": "smbX_oplocks",
                "share_description": "Test oplocks.",
                "share_properties": {
                    "allow_unencrypted_access": True,
                    "encryption": False,
                    "oplocks": True
                },
                "share_acls": share_acl_list
            },               
            {
                "qtree": "smbX_snapshots",
                "share_description": "See if clients can access snapshots.",
                "share_properties": {
                    "allow_unencrypted_access": True,
                    "encryption": False,
                    "show_snapshot": True
                },
                "share_acls": share_acl_list
            },
            {
                "qtree": "smbX_prev_versions",
                "share_description": "See if clients can restore snapshots.",
                "share_properties": {
                    "allow_unencrypted_access": True,
                    "encryption": False,
                    "show_snapshot": True,
                    "show_previous_versions": True
                },
                "share_acls": share_acl_list
            },                      

        ]
        for qitem in qtree_list:

            qtree_name = qitem["qtree"]
            share_name = qtree_name
            share_path = f'/{volume_name}/{qtree_name}'
            share_comment = qitem["share_description"]
            share_properties = qitem["share_properties"]
            share_acls = qitem["share_acls"]

            log.step(f'Sending Qtree Creation: {svm_name}:{share_path} of {qtree_name}')
            job_state, job_uuid = create_qtree(svm_name, volume_name, qtree_name)
            job_check = None
            if job_state == "success" and job_uuid != 400:
                log.step(f'Checking Qtree creation job status: {job_state} {job_uuid}')
                job_check = check_job_status(job_state, job_uuid)
            if job_check is not None:
                log.info(job_check)
                log.blank()
            
            job_state, job_uuid = create_shares(svm_name, share_name, share_path, share_comment, share_properties, share_acls)
            # log.step(f'Checking Share creation job status: {job_state} {job_uuid}')
            # job_check = check_job_status(job_state, job_uuid)





        # ''' Send for Share Creations. '''
        # for volqtrpath in volume_qtree_paths:
        #     log.info(f' > {svm_name} >> {volqtrpath}')




  
# CERTIFY_ANON     
# CERTIFY_GENERAL  
                 
# CERTIFY_LARGE_IO 
# CERTIFY_ACL_ALLOW
# CERTIFY_ACL_DENY 
# CERTIFY_AUDIT    

        #gib = byte_count / (1024 ** 3)

            # log.info(f'{pj(data)}')

        # log.info(f'{pj(volume_payload)}')


        # data = api.get('/cluster')
        # log.info(data)
        # log.info(f'{pj(records)}')
        # for k, v in records.items():
                    #     print(k, v)




    
















    return None
