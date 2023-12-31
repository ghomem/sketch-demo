import os

# The following environment variables are REQUIRED
#
# SKETCH_DB_USER
# SKETCH_DB_PASS
# AWS_ACCESS_KEY_ID
# AWS_SECRET_ACCESS_KEY

# These variables are not part of the configuration since they are sensitive. The remaining variables should be added as necessary.

### Exit codes

E_OK  = 0
E_ERR = 1

### Database related variables

DB_HOST = 'sketch-production-db-do-user-7447558-0.c.db.ondigitalocean.com'
DB_PORT = 25060
DB_NAME = 'proddatabase'

DB_USER = os.getenv('SKETCH_DB_USER')
DB_PASS = os.getenv('SKETCH_DB_PASS')

DB_CONN_STRING = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"

### S3 related variables

# S3 bucket names to use. They must exist and be accessible to your AWS credentials
S3_BUCKET_NAME_LEG = 'sketch-legacy-s3'
S3_BUCKET_NAME     = 'sketch-production-s3'

# S3 connection details
S3_BUCKET_DOMAIN    = 'fra1.digitaloceanspaces.com'
S3_ENDPOINT_URL_LEG = f"https://{S3_BUCKET_DOMAIN}"

# maximum objects we are requesting at once (anything above 1000 is floored to 1000)
# lower this value only to debug pagination
S3_MAX_OBJECTS_REQ = 1000

AWS_DEFAULT_REGION  = 'us-east-1'

AWS_ACCESS_KEY_ID     = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

### Other variables

LOG_DIR = '/tmp'

CAPITAL_LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
LOWERCASE_LETTERS = "abcdefghijklmnopqrstuvwxyz"
LETTERS = CAPITAL_LETTERS + LOWERCASE_LETTERS
NUMBERS = "0123456789"

# used for the random component of the log file name
CHARSET_TMP = LETTERS + NUMBERS
