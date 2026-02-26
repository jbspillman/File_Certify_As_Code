from exec_logger import get_logger
from test_nfs import nfs_test_suite
from test_smb import smb_test_suite
import time
import os


"""

app_main.py       # ties it all together, runs the suite

exec_logger.py    # logging setup and helper functions
exec_ssh.py       # SSH shell-level checks, CLI commands
exec_api.py       # OneFS PAPI / ONTAP REST API connections
exec_mounts.py    # mount/unmount helper functions, options dataclass for all the Mount options
exec_syslog.py    # helper functions for parsing syslogs and audit logs

test_allocate.py  # Provisioning the exports and shares (create/delete exports, shares, users, groups, permissions)
test_nfs.py       # NFS3/NFS4 protocol tests
test_smb.py       # SMB2/SMB3 protocol tests
test_rbac.py      # RBAC and audit tests
test_benchmark.py # Capture simple throughput/latency baselines

exec_report.py    # summarize results across all clusters

"""



def main():    
    log = get_logger("File_Certification_As_Code", level=1)
    log.blank()    

    # nfs_test_suite(log)

    # smb_test_suite(log)
    



if __name__ == "__main__":
    main()


