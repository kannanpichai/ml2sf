-- STAGE_SHA256: the SHA-256 of a file in a Snowflake stage, computed inside
-- Snowflake. `main.py migrate` uses it to prove each staged file is byte for
-- byte what MarkLogic holds, by comparing it with MarkLogic's own SHA-256.
--
-- Why a function: the stage encrypts client-side (SHOW STAGES type INTERNAL),
-- so LIST reports the size and MD5 of the encrypted copy, which never match the
-- file. This reads the file after Snowflake decrypts it. Nothing is downloaded.
--
-- Run once, as a role that can create functions in this schema and has READ on
-- the stage (the function reads files as its owner). Python functions need the
-- Anaconda package terms accepted in the account.
--
-- If you create it under another name, set SF_HASH_FUNCTION in .env to it.

CREATE OR REPLACE FUNCTION GDX_DOCUMENTS_DB.GDX_DOCUMENTS.STAGE_SHA256(path STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS $$
import hashlib
from snowflake.snowpark.files import SnowflakeFile

def run(path):
    h = hashlib.sha256()
    with SnowflakeFile.open(path, 'rb', require_scoped_url=False) as f:
        for chunk in iter(lambda: f.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()
$$;

-- The role `migrate` signs in with (SF_ROLE) must be able to call it.
GRANT USAGE ON FUNCTION GDX_DOCUMENTS_DB.GDX_DOCUMENTS.STAGE_SHA256(STRING) TO ROLE <migration role>;

-- Check: returns the SHA-256 of one staged file.
-- SELECT GDX_DOCUMENTS_DB.GDX_DOCUMENTS.STAGE_SHA256(
--   BUILD_STAGE_FILE_URL(@GDX_DOCUMENTS_DB.GDX_DOCUMENTS.GDX_ML_DOCUMENTS, '<documentuuid>/<file>'));
