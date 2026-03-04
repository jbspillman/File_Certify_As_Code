from exec_logger import get_logger
from exec_secrets import get_creds
from exec_storage_provision import storage_provision_suite
from test_nfs import nfs_test_suite
from test_smb import smb_test_suite
from test_sem_ontap import ontap_sem_suite
import time
import os


"""

app_main.py                      # ties it all together, runs the suite
                                 
exec_logger.py                   # logging setup and helper functions
exec_ssh.py                      # SSH shell-level checks, CLI commands
exec_api.py                      # REST API connection manager
exec_mounts.py                   # NFS mount/unmount helper functions, options dataclass for all the Mount options
exec_syslog.py                   # Helper function to host syslog destination server for event captures
exec_secrets.py                  # Helper to return connection credentials for devices, shares, etc
exec_storage_provision.py        # Initiate storage creations for NFS, SMB, etc. to be used with other test suites.

# test_allocate.py               # Provisioning the exports and shares (create/delete exports, shares, users, groups, permissions)

test_nfs.py                      # NFS3/NFS4 protocol tests
test_smb.py                      # SMB2/SMB3 protocol tests
test_sem_ontap.py                # Security event tests, ensure they are sent to the specified event collectors
test_sem_onefs.py                # Security event tests, ensure they are sent to the specified event collectors
test_benchmark.py                # Capture simple throughput/latency baselines
                                 
exec_report.py                   # summarize results across all clusters

"""





def main():    
    os.system('clear')

    log = get_logger("File_Certification_As_Code", level=1, log_dir="logs", log_file="test.log")
    log.blank()    

    vendor_software = "ONTAP"  # ONTAP - ONEFS
    
    ontap_sem_suite(log, vendor_software)
    # onefs_sem_suite(log, vendor_software)  # no exist.
    
    # storage_provision_suite(log, vendor_software)  # still pending a lot of items.
    # nfs_test_suite(log, vendor_software)           # mostly working.
    # smb_test_suite(log, vendor_software)           # difficult to do at home.
    



if __name__ == "__main__":
    main()


