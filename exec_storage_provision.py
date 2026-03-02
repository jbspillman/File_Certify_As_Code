#exec_storage_provision.py

from exec_logger import title_large , title_small
from provision_ontap_smb import create_ontap_smb
import json



"""
+------------------------------+--------+---------------+--------------------------------------------------+
| Volume Name                  | Size   | Export Type   | Purpose                                          |
+------------------------------+--------+---------------+--------------------------------------------------+
| nfs_certify_rw               | 5  GB  | RW, NFS3+NFS4 | General + NFS3 + NFS4 behavioral tests           |
| nfs_certify_ro               | 5  GB  | RO, NFS3+NFS4 | Readonly enforcement and read-operation tests    |
| nfs_certify_large_io         | 5  GB  | RW, NFS3+NFS4 | Large sequential I/O and parallel I/O perf       |
| nfs_certify_nfs4_acl         | 5  GB  | RW, NFS4 only | NFSv4 ACL tests (ACL mode may need explicit set) |
| nfs_certify_nfs4_delegation  | 5  GB  | RW, NFS4 only | Delegation tests (delegation enabled on export)  |
+------------------------------+--------+---------------+--------------------------------------------------+


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

def storage_provision_suite(log, vendor_suite):
    # title_large(log, f'Provision Test Env for: {vendor_suite}')

    vendor_details = [
        {
            'vendor': 'NetApp',
            'software': 'ONTAP 9.16.1P1',
            'storage-mgmt': 'ontap001-mgmt.beastmode.local.net',
            'storage-api': 'https://<MGMT>/api',
            'storage-namespace_nfs': 'svm01',
            'storage_nfs_server': 'svm01.beastmode.local.net',
            'storage-namespace_smb': 'svm02',
            'storage_nfs_server': 'svm02.beastmode.local.net'
        },
        {
            'vendor': 'Dell',
            'software': 'PowerScale OneFS 9.10.0.0',
            'storage-mgmt': 'onefs002-2.beastmode.local.net',
            'storage-api': 'https://<MGMT>:8080/platform',
            'storage-namespace_nfs': 'protocol',
            'storage_nfs_server': 'protocol.onefs002.beastmode.local.net',
            'storage-namespace_smb': 'protocol',
            'storage_smb_server': 'protocol.onefs002.beastmode.local.net'
        }
    ]

    
    smb_access_info = {
        "smb_domain_name": "BEASTMODE.LOCAL.NET",
        "smb_domain_admin": "Administrator",
        "smb_storage_admin": "storage_admin",
        "local_users": [
            {
                "name": "lcl_storage_user",
                "type": "local_sam_account",  
                "purpose": "Local user created on the storage appliance."
            }
        ],
        "domain_users": [
            {
                "name": "certify_user_full",
                "type": "domain_user",
                "purpose": "To be used when testing full control on smb shares."
            },
            {
                "name": "certify_user_modify",
                "type": "domain_user",
                "purpose": "To be used when testing modify access on smb shares."
            },
            {
                "name": "certify_user_read",
                "type": "domain_user",
                "purpose": "To be used when testing read access on smb shares."
            }
        ],
        "domain_groups": [
            {
                "name": "UG_Certify_FULL",
                "members": ["certify_user_full"],
                "purpose": "ACL Testing on Shares."
            },            
            {
                "name": "UG_Certify_MODIFY",
                "members": ["certify_user_modify"],
                "purpose": "ACL Testing on Shares."
            },
            {
                "name": "UG_Certify_READ",
                "members": ["certify_user_read"],
                "purpose": "ACL Testing on Shares."
            }
        ]
    }
    
    key_info = {}
    for v in vendor_details:
        if vendor_suite.upper() in v["software"].upper():
            key_info = v
            break
    
    if vendor_suite == "ONTAP":
        title_large(log, f'Provision SMB Test Env for: {vendor_suite}')

        create_ontap_smb(log, key_info, smb_access_info)


    return






