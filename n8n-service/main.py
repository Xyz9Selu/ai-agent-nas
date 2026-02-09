import os
import logging
from flask import Flask, jsonify, request as flask_request
from googleapiclient.errors import HttpError
from dotenv import load_dotenv

import sap_parser

# Load environment variables
load_dotenv()

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def _get_bearer_token():
    auth = flask_request.headers.get("Authorization", "")
    if not auth:
        return None

    prefix = "Bearer "
    if not auth.startswith(prefix):
        return None

    token = auth[len(prefix) :].strip()
    return token or None


@app.route("/parse-sap-sheet", methods=["POST"])
def parse_sap_sheet():
    """Parse SAP sheet and write rows to database."""
    try:
        access_token = _get_bearer_token()
        print(f"access_token: {access_token[0:4]}****{access_token[-4:]}")
        if not access_token:
            return jsonify(
                {
                    "error": "Unauthorized",
                    "message": "Missing or invalid Authorization header (expected: Bearer <token>)",
                }
            ), 401

        data = flask_request.get_json(silent=True)
        if not data:
            data = flask_request.form

        file_id = None
        dataset_id = None
        if data:
            file_id = data.get("file_id")
            dataset_id = data.get("dataset_id")
        if not file_id:
            file_id = flask_request.args.get("file_id")
        if dataset_id is None:
            dataset_id = flask_request.args.get("dataset_id")

        if not file_id:
            return jsonify({"error": "file_id parameter is required"}), 400

        print(f"file_id: {file_id}")
        result = sap_parser.write_sap_sheet_to_database(
            file_id=file_id,
            access_token=access_token,
            dataset_id=dataset_id,
        )
        return jsonify(result), 200

    except HttpError as e:
        status = getattr(getattr(e, "resp", None), "status", 500) or 500
        logger.exception("Google API error")
        return jsonify({"error": "Google API error", "message": str(e)}), status

    except Exception as e:
        logger.exception("Error processing /parse-sap-sheet")
        return jsonify({"error": "Failed to process request", "message": str(e)}), 500


@app.route("/parse-sap-sheet-jsonl", methods=["POST"])
def parse_sap_sheet_jsonl():
    """Parse SAP sheet and write rows to JSONL file only (no database)."""
    try:
        access_token = _get_bearer_token()
        if not access_token:
            return jsonify(
                {
                    "error": "Unauthorized",
                    "message": "Missing or invalid Authorization header (expected: Bearer <token>)",
                }
            ), 401

        data = flask_request.get_json(silent=True)
        if not data:
            data = flask_request.form

        file_id = data.get("file_id") if data else None
        if not file_id:
            file_id = flask_request.args.get("file_id")

        if not file_id:
            return jsonify({"error": "file_id parameter is required"}), 400

        result = sap_parser.write_sap_sheet_to_file(
            file_id=file_id,
            access_token=access_token,
        )
        return jsonify(result), 200

    except HttpError as e:
        status = getattr(getattr(e, "resp", None), "status", 500) or 500
        logger.error("Google API error: %s", str(e))
        return jsonify({"error": "Google API error", "message": str(e)}), status

    except Exception as e:
        logger.error("Error processing /parse-sap-sheet-jsonl: %s", str(e))
        return jsonify({"error": "Failed to process request", "message": str(e)}), 500


if __name__ == '__main__':
    port = int(os.getenv('PORT', '5000'))
    debug = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
    app.run(host='0.0.0.0', port=port, debug=debug)

