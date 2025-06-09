import boto3

def get_all_instance_volume_ids(ec2):
    """
    Collects all volume IDs that are currently attached to running or stopped EC2 instances.
    """
    volumes_in_use = set()
    for instance in ec2.instances.all():
        for dev in instance.block_device_mappings:
            volumes_in_use.add(dev['Ebs']['VolumeId'])  # Get volume IDs attached to instance
    return volumes_in_use

def get_detached_volumes(ec2, used_volumes):
    """
    Finds volumes that are not attached to any EC2 instance (i.e., not in use).
    """
    detached_volumes = []
    for volume in ec2.volumes.all():
        if volume.id not in used_volumes:
            detached_volumes.append(volume.id)  # Volume is not attached to any instance
    return detached_volumes

def delete_unused_snapshots():
    """
    Deletes:
    1. Snapshots that are not associated with any volume.
    2. Snapshots of volumes that are detached (i.e., volumes not in use by any EC2 instance).
    """
    ec2 = boto3.resource('ec2')
    client = boto3.client('ec2')

    print("Collecting in-use volume IDs...")
    in_use_volumes = get_all_instance_volume_ids(ec2)

    print("Finding detached volumes...")
    detached_volumes = get_detached_volumes(ec2, in_use_volumes)

    print("Scanning snapshots for deletion...")
    snapshots = client.describe_snapshots(OwnerIds=['self'])['Snapshots']  # Get all snapshots owned by you

    for snap in snapshots:
        snapshot_id = snap['SnapshotId']
        volume_id = snap.get('VolumeId')

        if not volume_id:
            # Case 1: Snapshot is orphaned (not linked to any volume)
            print(f"Deleting orphan snapshot {snapshot_id} (no volume)...")
            client.delete_snapshot(SnapshotId=snapshot_id)

        elif volume_id in detached_volumes:
            # Case 2: Snapshot is linked to a detached volume
            print(f"Deleting snapshot {snapshot_id} (volume {volume_id} is detached)...")
            client.delete_snapshot(SnapshotId=snapshot_id)

        else:
            # Skip snapshot if volume is still in use by an instance
            print(f"Skipping snapshot {snapshot_id} (volume {volume_id} is attached)")

if __name__ == '__main__':
    delete_unused_snapshots()
