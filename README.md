# de-nano-project-4

### How to connect to EMR Cluster via SSH

1. Create and download '.pem' credentials.
2. Change file permissions on '.pem' file:
```$ chmod 0400 .ssh/my_private_key.pem```
3. Log in to cluster:
``` $ ssh -i my_private_key.pem hadoop@ec2-54-245-27-75.us-west-2.compute.amazonaws.com```

Copy files to emr cluster:


scp -i maclinux-ec2.pem etl.py hadoop@ec2-54-245-27-75.us-west-2.compute.amazonaws.com:src/
