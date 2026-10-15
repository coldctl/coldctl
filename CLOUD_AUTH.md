# Cloud storage authentication

The agent and archive gateway support automatically refreshed AWS role credentials
and Azure VM managed identity. PostgreSQL and MongoDB continue to use database
password environment references and TLS. These storage modes do not change cloud
portal login or the gateway's tenant bearer authentication.

## AWS EC2

Attach an IAM instance profile to the customer VM. Require IMDSv2 and allow the
process to reach the instance metadata service. Create the bucket beforehand.
Then, in PowerShell on that VM:

```powershell
coldctl destination add s3 --name aws-archive --bucket YOUR_BUCKET --region ap-south-1 --prefix coldctl --role-auth
coldctl destination test aws-archive
```

No AWS access-key environment variables are needed. The SDK retrieves and refreshes
temporary credentials. When both `AWS_WEB_IDENTITY_TOKEN_FILE` and `AWS_ROLE_ARN`
are present, web identity takes precedence over EC2 metadata; the platform must
maintain the token file. This is not a general AWS default credential chain:
shared profiles, SSO, ECS task credentials and arbitrary assume-role chains are
not supported by this option. IMDSv1 fallback is disabled.

For the gateway, configure the same bucket, region and prefix:

```toml
[remote]
prefix = "coldctl"
[remote.provider]
kind = "s3"
bucket = "YOUR_BUCKET"
region = "ap-south-1"
role_auth = true
```

Scope permissions to the archive prefix. The agent needs `s3:PutObject` and
`s3:GetObject` on archive objects. Its access test also creates and removes a
temporary probe, so grant `s3:DeleteObject` on the probe keys only where practical.
The gateway needs `s3:ListBucket` on the bucket (restricted to the configured
prefix) and `s3:GetObject` on archive objects. Customer-managed KMS encryption
also requires the corresponding KMS permissions and key policy. Use a separate,
read-only gateway role. Do not grant the gateway permission to delete archives.

See [AWS EC2 role setup](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_switch-role-ec2.html).

## Azure VM

Enable a system-assigned identity on the customer VM, or attach a user-assigned
identity. Create the storage account and blob container beforehand. Assign the
agent **Storage Blob Data Contributor** at container scope; assign the gateway
**Storage Blob Data Reader**, preferably using a separate identity. Allow time
for role assignments to propagate.

```powershell
coldctl destination add azure --name azure-archive --account YOUR_ACCOUNT --container archives --prefix coldctl --managed-identity
coldctl destination test azure-archive
```

For a user-assigned identity, append
`--managed-identity-client-id YOUR_CLIENT_UUID`. This is the identity's client ID,
not its object ID. No account key is required.

Gateway provider configuration:

```toml
[remote]
prefix = "coldctl"
[remote.provider]
kind = "azure"
account = "YOUR_ACCOUNT"
container = "archives"
managed_identity = true
# managed_identity_client_id = "YOUR_CLIENT_UUID"
```

This mode uses the Azure VM metadata endpoint, with automatic token refresh.
Azure CLI login, service-principal secrets, App Service identity endpoints and
sovereign-cloud endpoints are not part of this implementation.
See [Azure blob role assignment](https://learn.microsoft.com/en-us/azure/storage/blobs/assign-azure-role-data-access).

## Local workstation and emulators

Keep using the existing credential environment references when running on your
laptop. A laptop does not gain a VM identity by passing these flags. AWS temporary
keys can use `--session-token-env AWS_SESSION_TOKEN`, but their expiry is not
automatically renewed in static mode. Azure uses `AZURE_STORAGE_ACCOUNT_KEY`.
Existing saved configurations retain this behavior.

Cloud identity modes reject custom storage endpoints and `--allow-http` to avoid
sending identity credentials to an unintended server. Private networking may use
DNS for the standard service hostname. Explicit emulator endpoints continue to
work with static environment credentials. Secret values are never saved in
destination configuration, manifests or the gateway catalog.

## Remote end-to-end trial

1. Run the agent on the identity-enabled VM with network access to your databases
   and storage. Use TLS with certificate verification and database password
   environment references for both PostgreSQL and MongoDB.
2. Run `source test` and `source discover` for each source, then the destination
   access test above. Start with disposable data and a bounded archive policy.
3. Archive and verify without deletion first. Run the gateway with the matching
   bucket/container and prefix, trusted tenant layout, and a read-only identity.
   Refresh its catalog and query/download an archived record through its API.
4. Validate restoration. Only then enable verified deletion or recurring
   archival with the required source-stability acknowledgements. MongoDB still
   requires a quiescent copy; authentication does not remove that restriction.
5. Test a long-running cycle across credential expiry and a worker restart on
   the actual cloud VM before relying on unattended archival.

Local tests cover metadata credential retrieval and refresh using a mock HTTP
connector, legacy configuration compatibility, and endpoint rejection. They do
not establish that your IAM/RBAC, TLS, network rules or cloud account are correct.
