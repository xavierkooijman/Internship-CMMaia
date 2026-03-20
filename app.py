from flask import Flask, request, jsonify
import os
import subprocess

app = Flask(__name__)


def verify_password(password):
    expected_password = os.getenv('APP_PASSWORD')

    if password == expected_password:
        return True
    else:
        return False


@app.route('/')
def home():
    return "Flask is running"


@app.route('/xk_ipma', methods=['POST'])
def xk_ipma():
    data = request.json
    if not data or 'password' not in data:
        return jsonify({'error': 'Password is required'}), 400

    if not verify_password(data['password']):
        return jsonify({'error': 'Invalid password'}), 401

    try:
        result = subprocess.run(
            ['python', 'scripts/xk_ipma.py'], capture_output=True, text=True)

        return jsonify({
            "status": "completed",
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr
        })

    except Exception as e:
        return jsonify({
            "error": str(e)
        }), 500


@app.route('/xk_owm', methods=['POST'])
def xk_ipma():
    data = request.json
    if not data or 'password' not in data:
        return jsonify({'error': 'Password is required'}), 400

    if not verify_password(data['password']):
        return jsonify({'error': 'Invalid password'}), 401

    try:
        result = subprocess.run(
            ['python', 'scripts/xk_owm.py'], capture_output=True, text=True)

        return jsonify({
            "status": "completed",
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr
        })

    except Exception as e:
        return jsonify({
            "error": str(e)
        }), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
