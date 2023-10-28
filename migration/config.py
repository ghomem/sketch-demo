import os

# The following environment variables are REQUIRED
#
# SKETCH_DB_ADMIN_USER
# SKETCH_DB_ADMIN_PASS
# AWS_ACCESS_KEY_ID
# AWS_SECRET_ACCESS_KEY

# These variables are not part of the configuration since they are sensitive. The remaining variables should be added as necessary.

### Database related variables

DB_HOST = 'sketch-production-db-do-user-7447558-0.c.db.ondigitalocean.com'
DB_PORT = 25060
DB_NAME = 'proddatabase'

DB_USER = os.getenv('SKETCH_DB_ADMIN_USER')
DB_PASS = os.getenv('SKETCH_DB_ADMIN_PASS')

DB_CONN_STRING = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"

### S3 related variables

# S3 bucket names to use. They must exist and be accessible to your AWS credentials
S3_BUCKET_NAME_LEG = 'sketch-legacy-s3'
S3_BUCKET_NAME     = 'sketch-production-s3'

# S3 connection details
S3_BUCKET_DOMAIN    = 'fra1.digitaloceanspaces.com'
S3_ENDPOINT_URL     = f"https://{S3_BUCKET_NAME}.{S3_BUCKET_DOMAIN}"
S3_ENDPOINT_URL_LEG = f"https://{S3_BUCKET_DOMAIN}"  # for the legacy client connection
AWS_DEFAULT_REGION  = 'us-east-1'

AWS_ACCESS_KEY_ID     = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
