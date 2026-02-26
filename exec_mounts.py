"""
exec_mounts.py — NFS mount/unmount operations for storage validation
"""

import os
import subprocess
from dataclasses import dataclass
from typing import Optional
import tempfile



# ─────────────────────────────────────────────
#  Mount Options Dataclass
# ─────────────────────────────────────────────
@dataclass
class MOUNT_OPTIONS:
    mount_mode = "rw"               # read-only (ro), read-write (rw)
    majorvers: int      = 3         # 3 for NFSv3, 4 for NFSv4
    minorversion: int   = 0         # NFSv4: 0, 1, or 2 for NFSv4.0, 4.1, 4.2
    pnfs: bool          = False     # enable parallel NFS (NFSv4.1+)
    nconnect: bool      = False     # enable multiple TCP connections
    nconnect_count: int = 4         # number of parallel connections (NFSv4.1+)
    transport: str      = 'tcp'     # tcp, udp, rdma
    rsize: int          = 1048576   # 1MB read size
    wsize: int          = 1048576   # 1MB write size
    timeo: int          = 600       # 600 deciseconds (60 seconds)
    retrans: int        = 2         # retransmissions before giving upR
    soft: bool          = False     # soft mount fails after retrans, hard retries indefinitely
    intr: bool          = True      # allow interrupts on hard mounts
    noac: bool          = False     # disable attribute caching
    actimeo: int        = None      # override acregmin/acdirmin with single value
    acregmin: int       = 3         # min seconds to cache file attributes
    acregmax: int       = 60        # max seconds to cache file attributes
    acdirmin: int       = 30        # min seconds to cache directory attributes
    acdirmax: int       = 60        # max seconds to cache directory attributes
    nosharecache: bool  = False     # disable client-side file handle caching
    nordirplus: bool    = False     # disable NFSv4.1+ directory delegations
    sec: str            = 'sys'     # sys, krb5, krb5i, krb5p


# ─────────────────────────────────────────────
#  Build mount options string
# ─────────────────────────────────────────────
def _build_mount_options(options: MOUNT_OPTIONS) -> str:
    """
    Converts a MOUNT_OPTIONS dataclass into a comma-separated mount options string.
    Handles version-specific options and skips invalid combinations.
    """
    opts = []

    # Mount mode (ro/rw)
    opts.append(options.mount_mode) 

    # NFS version
    if options.majorvers == 3:
        opts.append("vers=3")
    elif options.majorvers == 4:
        if options.minorversion > 2:
            raise ValueError("Invalid minor version for NFSv4: must be 0, 1, or 2")
        opts.append(f"vers=4.{options.minorversion}")

    # Transport
    opts.append(f"proto={options.transport}")

    # Read/write sizes
    opts.append(f"rsize={options.rsize}")
    opts.append(f"wsize={options.wsize}")

    # Timeout and retransmissions
    opts.append(f"timeo={options.timeo}")
    opts.append(f"retrans={options.retrans}")

    # Mount behavior
    opts.append("soft" if options.soft else "hard")
    if options.intr and options.majorvers == 3:
        opts.append("intr")

    # Attribute caching
    if options.noac:
        opts.append("noac")
    elif options.actimeo is not None:
        opts.append(f"actimeo={options.actimeo}")
    else:
        opts.append(f"acregmin={options.acregmin}")
        opts.append(f"acregmax={options.acregmax}")
        opts.append(f"acdirmin={options.acdirmin}")
        opts.append(f"acdirmax={options.acdirmax}")

    # NFSv4.1+ specific options
    if options.majorvers == 4 and options.minorversion >= 1:
        if options.pnfs:
            opts.append("pnfs")
        if options.nconnect:
            opts.append(f"nconnect={options.nconnect_count}")
        if options.nosharecache:
            opts.append("nosharecache")
        if options.nordirplus:
            opts.append("nordirplus")

    # Security flavor
    opts.append(f"sec={options.sec}")

    return ",".join(opts)


# ─────────────────────────────────────────────
#  Mount
# ─────────────────────────────────────────────
def mount_nas(
        log,
        vendor: str, 
        software: str,  
        nfs_server: str, 
        nfs_export: str, 
        mount_point: str = "TMP", 
        uid: int = 0, 
        gid: int = 0, 
        options: Optional[MOUNT_OPTIONS] = None, 
        dry_run: bool = False
        ) -> bool:
    
    """
    Mount an NFS export using the provided MOUNT_OPTIONS.

    Parameters
    ----------
    log         : Logger instance for logging steps and results
    vendor      : Storage vendor name (e.g. NetApp, Dell EMC)
    software    : Storage software name and version (e.g. ONTAP 9.10)
    nfs_server  : NFS server hostname or IP
    nfs_export  : Export path on the server (e.g. /ifs/data/prod)
    mount_point : Local path to mount to (e.g. /mnt/test) or auto-generated temp directory if "TMP"
    uid         : UID for mount ownership
    gid         : GID for mount ownership
    options     : MOUNT_OPTIONS dataclass instance (defaults to NFSv3 if not provided)
    dry_run     : If True, log the command but do not execute

    Returns
    -------
    bool : True if mount succeeded, False if it failed
    """

    if os.geteuid() != 0:
        log.error("✗ Mount operations require root privileges. Please run as root or with sudo.")
        return False

    if options is None:
        options = MOUNT_OPTIONS()

    if mount_point == "TMP":
        mount_point = tempfile.mkdtemp()         # find safe mounting location with random suffix
        mp_zero = os.path.dirname(mount_point)   # get just the temp folder
        mp_middle = 'zCertTMP'                   # fixed middle portion for easy identification in logs and cleanup
        mp_name = os.path.basename(mount_point)  # get just the random suffix
        mount_point = os.path.join(mp_zero, mp_middle, mp_name)

    opts_str = _build_mount_options(options)
    source   = f"{nfs_server}:{nfs_export}"
    version  = f"NFSv{options.majorvers}" if options.majorvers == 3 else f"NFSv4.{options.minorversion}"
    command  = ["mount", "-t", "nfs", "-o", opts_str, source, mount_point]

    log.step(f"Mounting {version} export")
    log.info(f"Source      : {source}")
    log.info(f"Mount point : {mount_point}")
    log.info(f"Options     : {opts_str}")
    log.info(f"Command     : {' '.join(command)}")

    if dry_run:
        log.warning("Dry run — mount command not executed")
        return True, mount_point

    # Create mount point if it doesn't exist
    if not os.path.isdir(mount_point):
        try:
            os.makedirs(mount_point, exist_ok=True)
            log.success(f"✓ Created mount point: {mount_point}")
        except OSError as e:
            log.error(f"✗ Failed to create mount point {mount_point}: {e}")
            return False, mount_point

    # Execute mount
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=30,
        )

        if result.returncode == 0:
            log.success(f"✓ {version} mount successful: {mount_point}")

            if uid != 0 or gid != 0:
                try:
                    os.chown(mount_point, uid, gid)
                    log.success(f"✓ Set ownership of {mount_point} to UID:{uid} GID:{gid}")
                except OSError as e:
                    log.error(f"✗ Failed to set ownership on {mount_point}: {e}")
                    # Not returning False here since the mount itself succeeded     

            return True, mount_point
        else:
            log.error(f"✗ Mount failed (exit {result.returncode}): {result.stderr.strip()}")
            return False, mount_point

    except subprocess.TimeoutExpired:
        log.error(f"✗ Mount timed out after 30 seconds: {source}")
        return False, mount_point
    except Exception as e:
        log.error(f"✗ Unexpected error during mount: {e}", exc_info=True)
        return False, mount_point


# ─────────────────────────────────────────────
#  Unmount
# ─────────────────────────────────────────────
def unmount_nas(
        log, 
        vendor: str, 
        software: str,  
        mount_point: str, 
        force: bool = False, 
        lazy: bool = False, 
        dry_run: bool = False
        ) -> bool:
    """
    Unmount an NFS mount point.

    Parameters
    ----------
    mount_point : Local path to unmount
    force       : Pass -f flag (force unmount, useful for unresponsive servers)
    lazy        : Pass -l flag (lazy unmount, detaches immediately, cleans up later)
    dry_run     : If True, log the command but do not execute

    Returns
    -------
    bool : True if unmount succeeded, False if it failed
    """

    if os.geteuid() != 0:
        log.error("✗ Mount operations require root privileges. Please run as root or with sudo.")
        return False

    command = ["umount"]
    if force:
        command.append("-f")
    if lazy:
        command.append("-l")
    command.append(mount_point)

    log.step(f"Unmounting: {mount_point}")
    log.info(f"Command : {' '.join(command)}")

    if dry_run:
        log.warning("Dry run — unmount command not executed")
        return True

    if not os.path.isdir(mount_point):
        log.warning(f"✗ Mount point does not exist: {mount_point}")
        return False, False

    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=30,
        )

        if result.returncode == 0:
            log.success(f"✓ Unmount successful: {mount_point}")
            
            os.rmdir(mount_point)  # clean up the mount point directory after unmounting
            if os.path.exists(mount_point):
                log.warning(f"✗ Mount point directory still exists after unmount: {mount_point}")
                return True, False
            else:
                log.success(f"✓ Mount point directory removed: {mount_point}")
                return True, True
        else:
            log.error(f"✗ Unmount failed (exit {result.returncode}): {result.stderr.strip()}")
            return False, False

    except subprocess.TimeoutExpired:
        log.error(f"✗ Unmount timed out after 30 seconds: {mount_point}")
        return False, False
    except Exception as e:
        log.error(f"✗ Unexpected error during unmount: {e}", exc_info=True)
        return False, False


# ─────────────────────────────────────────────
#  Usage examples
# ─────────────────────────────────────────────
# if __name__ == "__main__":

#     # NFSv3
#     mount_nas(
#         nfs_server  = "cluster01.dc1.local",
#         nfs_export  = "/ifs/data/prod",
#         mount_point = "/mnt/test_nfs3",
#         uid         = 1000,
#         gid         = 1000,
#         options     = MOUNT_OPTIONS(majorvers=3),
#         dry_run     = True,
#     )

#     log.blank()

#     # NFSv4.1 with pNFS and nconnect
#     mount_nas(
#         nfs_server  = "cluster01.dc1.local",
#         nfs_export  = "/ifs/data/prod",
#         mount_point = "/mnt/test_nfs41",
#         uid         = 1000,
#         gid         = 1000,
#         options     = MOUNT_OPTIONS(
#             majorvers    = 4,
#             minorversion = 1,
#             pnfs         = True,
#             nconnect     = True,
#             nconnect_count = 8,
#             sec          = "krb5",
#         ),
#         dry_run     = True,
#     )

#     log.blank()

#     # NFSv4.2
#     mount_nas(
#         nfs_server  = "cluster01.dc1.local",
#         nfs_export  = "/ifs/data/prod",
#         mount_point = "/mnt/test_nfs42",
#         uid         = 1000,
#         gid         = 1000,
#         options     = MOUNT_OPTIONS(
#             majorvers    = 4,
#             minorversion = 2,
#             transport    = "rdma",
#         ),
#         dry_run     = True,
#     )

#     log.blank()

#     # Unmount examples
#     unmount_nas("/mnt/test_nfs3",  dry_run=True)
#     unmount_nas("/mnt/test_nfs41", lazy=True, dry_run=True)
#     unmount_nas("/mnt/test_nfs42", force=True, dry_run=True)



















# from dataclasses import dataclass
# @dataclass
# class MOUNT_OPTIONS:
#     majorvers: int = 3          # 3 for NFSv3, 4 for NFSv4
#     minorversion: int = 0       # NFSv4  0, 1, or 2 for NFSv4.0, 4.1, 4.2
#     pnfs: bool = False          # enable parallel NFS (NFSv4.1+)
#     nconnect: bool = False      # use NFS over TCP instead of UDP (NFSv4 defaults to TCP)
#     nconnect_connect: int = 4   # number of parallel connections for pNFS (NFSv4.1+)
#     transport: str = 'tcp'      # tcp, udp, rdma
#     rsize: int = 1048576        # 1MB read size
#     wsize: int = 1048576        # 1MB write size
#     timeo: int = 600            # 600 deciseconds (60 seconds)
#     retrans: int = 2            # number of retransmissions before giving up
#     soft: bool = False          # hard mounts will retry indefinitely, soft mounts will fail after 'retrans' attempts
#     intr: bool = True           # allow interrupts on hard mounts (deprecated in NFSv4, ignored by most implementations)
#     noac: bool = False          # disable attribute caching (not recommended for performance)
#     actimeo: int = None         # set both acregmin and acdirmin to the same value (overrides individual settings)
#     acregmin: int = 3           # minimum time (in seconds) to cache regular file attributes
#     acregmax: int = 60          # maximum time (in seconds) to cache regular file attributes
#     acdirmin: int = 30          # minimum time (in seconds) to cache directory attributes
#     acdirmax: int = 60          # maximum time (in seconds) to cache directory attributes
#     nosharecache: bool = False  # disable client-side caching of file handles (NFSv4.1+)
#     nordirplus: bool = False    # disable NFSv4.1+ directory delegations (improves consistency at the cost of performance)
#     sec: str = 'sys'            # sys, krb5, krb5i, krb5p
#    
# def mount_nas(nfs_server, nfs_export, mount_type, uid, gid):
#     """
#     This function would contain the logic to perform an NFS mount using the specified options.
#     It would likely use subprocess to call the 'mount' command on Linux.
#     The MOUNT_OPTIONS dataclass can be used to construct the mount command with the desired parameters.
#     For example, to mount an NFSv4.1 export with pNFS and TCP transport, you might do something like:
#     options = MOUNT_OPTIONS(majorvers=4, minorversion=1, pnfs=True, transport='tcp')
#     Then you would build the mount command based on these options and execute it.
#     """
#     pass    
#     unmount_nas("/mnt/test_nfs41", lazy=True, dry_run=True)
#     unmount_nas("/mnt/test_nfs42", force=True, dry_run=True)


