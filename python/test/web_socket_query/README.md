## Web Socket Query

This script is a proof of concept demonstrating how to execute queries over a WebSocket connection to a Sleeper API endpoint. It utilises AWS SigV4 signing for authentication and supports flexible query parameters via command-line input.

**Note:**
- This implementation is not optimised and may not handle all error cases gracefully.
- Large result sets from range queries can cause errors, so usage should be tested with manageable data sizes.
- The code is intended for development and testing purposes, not production.

**Features:**
- Constructs and serializes queries into JSON format.
- Establishes a signed WebSocket connection to the API Gateway endpoint.
- Receives incremental results asynchronously.
- Supports configuration via environment variables and user input.
- Optionally saves results into a JSON file.

**Prerequisites:**
- Python 3.9+
- AWS credentials configured with necessary permissions.

**Setup:**
- Dependencies: `pip install -r requirements.txt`
- Populate your AWS credentials and API details in environment variables or `.env` file:
  - `AWS_ACCESS_KEY_ID`
  - `AWS_SECRET_ACCESS_KEY`
  - `AWS_SESSION_TOKEN`
  - `AWS_REGION` (defaulted to `eu-west-2`)
  - `AWS_API_GATEWAY_ID`
  - `AWS_API_GATEWAY_STAGE` (default `'live'`)

  **Running the application:**

  After everything is set up, ensure the script is executable and run: `web_socket_query.py`

