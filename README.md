# Snowflake Protecto Mask

## Overview
This script connects to a Snowflake database, retrieves data from specified tables, and sends it to the Protecto Tokenization API for masking. It tracks the processing status asynchronously and saves the final masked table results into an Excel file.

For more details, refer to the [Protecto Tokenization API documentation](https://docs.protecto.ai/docs-category/tokenization-api/).

## Prerequisites
- Python 3.10+
- Required Python libraries:
  - requests
  - sqlalchemy
  - openpyxl
  - snowflake.sqlalchemy
  - python-dotenv
- Credentials stored in a JSON file
- Protecto API key for data tokenization

## Installation
1. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

## Configuration

### 1. Credentials File
Store your Snowflake credentials and Protecto API key in a JSON file (e.g., config/credentials.json) with the following format:

```json
{
    "account": "your_snowflake_account",
    "user": "your_snowflake_username",
    "password": "your_snowflake_password",
    "warehouse": "your_snowflake_warehouse",
    "role": "your_snowflake_role",
    "protecto_api_key": "your_protecto_api_key"
}
```

> **Note:** To obtain a Protecto API key, please contact help@protecto.ai

### 2. Table List File
Please enter the list of tables to be processed, with one table per line in `tables.txt`:

```
database_name.schema_name.table_name_1
database_name.schema_name.table_name_2
```

### 3. Mask Configuration

Create a `config/mask_configuration.json` file to define custom masking rules for specific table columns:

```json
{
  "DATABASE_NAME.SCHEMA_NAME.TABLE_NAME_1": {
    "0": { 
      "token_name": "Numeric Token"   # Column 0: Masking with numeric token only
    },
    "1": { 
      "token_name": "Text Token"      # Column 1: Masking with text token only
    },
    "2": { 
      "format": "Phone Number",       # Column 2: Masking phone numbers with numeric token
      "token_name": "Numeric Token"   
    },
    "3": { 
      "format": "URL",                # Column 3: Masking URLs with text token
      "token_name": "Text Token"      
    },
    "4": {},                          # Column 4: Auto-detection of sensitive data (no format and no token specified)
    "5": { 
      "format": null,                 # Column 5: Auto-detection of sensitive data (null values)
      "token_name": null                                        
    }
  },
  "DATABASE_NAME.SCHEMA_NAME.TABLE_NAME_2": {
    "0": { 
      "token_name": "Numeric Token"                             
    },
    "5": { 
      "token_name": "Text Token"                              
    },
    "7": { 
      "format": "Phone Number", 
      "token_name": "Numeric Token"                           
    },
    "8": { 
      "format": "URL", 
      "token_name": "Text Token"                               
    }
  }
}
```

The configuration follows this structure:
- Column indices start from 0
- Each column can have:
  - `token_name`: The type of token to use for masking
  - `format`: The data format to consider when masking
  - Both parameters can be omitted or set to `null` for automatic detection

#### Supported Formats and Tokens

For available options, refer to:
- [Supported Tokens](https://docs.protecto.ai/docs/supporting-token/)
- [Supported Formats](https://docs.protecto.ai/docs/supporting-format/)
- [Supported Entities](https://docs.protecto.ai/docs/supported-phi/)

### 4. Environment Variables
Create a `.env` file in the root directory with these parameters:

```
NUM_ROWS=100        # Maximum number of rows to be fetched per table
TABLE_CHUNK_SIZE=5  # Number of rows processed in each API call batch
```

## Usage

### Running the Masking Process
Execute the masking script:

```
python mask.py
```

### Processing Flow
1. The script reads the table list from `input_list.txt`
2. For each table:
   - Connects to Snowflake and retrieves data in configurable chunks
   - Applies masking configuration from `mask_configuration.json`
   - Sends data to Protecto's masking API
   - Receives tracking IDs for asynchronous processing
   - Saves tracking IDs to `tracking_ids.txt` for reference
   - Polls for completion status
   - Saves masked data to `output/[table name as given in input_list].xlsx`

## Advanced Customization
- `NUM_ROWS`: Controls the maximum number of rows fetched per table
- `TABLE_CHUNK_SIZE`: Sets the batch size for API processing
- `MAX_COLUMNS_PER_API_CALL`: Limits the number of columns in each API request

## Output
The tool generates:
- Excel files in the `output/` directory (one per processed table, named according to the table name in input_list.txt)
- A tracking log (`tracking_ids.txt`) for all processing requests

## Support
For assistance with this tool or the Protecto API:
- Email: help@protecto.ai
- Documentation: [Protecto Documentation](https://docs.protecto.ai/)
