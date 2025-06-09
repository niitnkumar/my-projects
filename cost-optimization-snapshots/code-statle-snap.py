import boto3

def lambda_handler(event, context):
    ec2 = boto3.client('ec2')

    # Step 1: Get all snapshots owned by the account
    snapshots = ec2.describe_snapshots(OwnerIds=['self'])['Snapshots']
    print(f"Found {len(snapshots)} snapshots.")

    # Step 2: Get all volumes and create a mapping from snapshot_id to volume_id
    volumes = ec2.describe_volumes()['Volumes']
    snapshot_to_volume = {}
    volume_attachment = {}

    for vol in volumes:
        snapshot_id = vol.get('SnapshotId')
        volume_id = vol['VolumeId']
        attachments = vol.get('Attachments', [])

        if snapshot_id:
            snapshot_to_volume[snapshot_id] = volume_id
            volume_attachment[volume_id] = bool(attachments)

    # Step 3: Get a list of used snapshot IDs to preserve
    used_snapshot_ids = set()

    for snapshot in snapshots:
        snapshot_id = snapshot['SnapshotId']
        volume_id = snapshot_to_volume.get(snapshot_id)

        if volume_id:
            attached = volume_attachment.get(volume_id, False)
            if attached:
                used_snapshot_ids.add(snapshot_id)

    # Step 4: Delete snapshots that are not in use
    deleted = 0
    for snapshot in snapshots:
        snapshot_id = snapshot['SnapshotId']
        if snapshot_id not in used_snapshot_ids:
            try:
                ec2.delete_snapshot(SnapshotId=snapshot_id)
                print(f"Deleted snapshot: {snapshot_id}")
                deleted += 1
            except Exception as e:
                print(f"Failed to delete {snapshot_id}: {e}")

    return {
        'statusCode': 200,
        'body': f"Deleted {deleted} unused snapshots."
    }
