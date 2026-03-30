#!pip install clts_pcp --quiet
#!pip install crate --quiet
#!pip install pymysql --quiet
#!pip install cryptography --quiet
import os
import sys
import requests
import json
import clts_pcp as clts
import socket
from datetime import datetime, timezone

tstart = clts.getts()
hostname = socket.gethostname()


def detect_environment():
    clts.elapt["Detect Environment"] = clts.deltat(tstart)
    if "COLAB_RELEASE_TAG" in os.environ:
        return "colab"
    elif "RENDER" in os.environ:
        return "render"
    elif sys.platform.startswith("win"):
        return "windows"
    else:
        return "linux"


env = detect_environment()
clts.elapt[f"Environment Detected: {env}"] = clts.deltat(tstart)
print("Running in:", env)

if env == "colab":
    from google.colab import userdata
    USER = userdata.get("USER")
    EMAIL_FROM = userdata.get("EMAIL_FROM")
    EMAIL_PASSWORD = userdata.get("EMAIL_PASSWORD")
    TOM_TOM_API_KEY = userdata.get("TOM_TOM_API_KEY")
    DB_LIST = json.loads(userdata.get(f"{USER}-dblist.json"))["databases"]
    RECEIVERS_LIST = json.loads(userdata.get(
        "xavier-receiverslist.json"))["receivers"]

elif env == "render":
    USER = os.getenv("USER")
    EMAIL_FROM = os.getenv("EMAIL_FROM")
    RESEND_API_KEY = os.getenv("RESEND_API_KEY")
    TOM_TOM_API_KEY = os.getenv("TOM_TOM_API_KEY")
    DB_LIST = json.load(open(f"/etc/secrets/{USER}-dblist.json"))["databases"]
    RECEIVERS_LIST = json.load(
        open(f"/etc/secrets/{USER}-receiverslist.json"))["receivers"]

else:
    from dotenv import load_dotenv
    load_dotenv()

    USER = os.getenv("USER")
    EMAIL_FROM = os.getenv("EMAIL_FROM")
    EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
    TOM_TOM_API_KEY = os.getenv("TOM_TOM_API_KEY")
    DB_LIST = json.load(open(f"{USER}-dblist.json"))["databases"]
    RECEIVERS_LIST = json.load(open(f"{USER}-receiverslist.json"))["receivers"]


clts.setcontext(f'TomTom Incidents Data Retrieval - Environment: {env}')


url = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"

lat = 41.232724
lon = -8.621900
timestamp = datetime.now(timezone.utc).isoformat()

params = {
    "point": f"{lat},{lon}",
    "key": TOM_TOM_API_KEY
}

data_status = "nok"

clts.elapt["Start Data Retrieval"] = clts.deltat(tstart)

try:
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    print("Data retrieved successfully")
    clts.elapt[f"Data Retrieved Successfully"] = clts.deltat(
        tstart)
    data_status = "ok"
except Exception as e:
    print(f"Error during data retrieval: {e}")
    clts.elapt[f"Data Retrieval Failed, Error: {e}"] = clts.deltat(
        tstart)

print("API Response:", data)


if data_status == "ok":

    flow = data["flowSegmentData"]

    values = (
        hostname,
        "TomTom",
        timestamp,
        lat,
        lon,
        flow.get("currentSpeed"),
        flow.get("freeFlowSpeed"),
        flow.get("currentTravelTime"),
        flow.get("freeFlowTravelTime"),
        flow.get("confidence"),
        flow.get("roadClosure"),
        flow.get("coordinates")
    )

    for db in DB_LIST:
        print(f"Processing database: {db}")
        status = "nok"
        clts.elapt[f"Connecting to {db}"] = clts.deltat(tstart)

        try:
            if env == "render":
                credentials_path = f"/etc/secrets/{USER}-{db}.json"
                dbcreds = json.load(open(credentials_path))
            elif env == "colab":
                dbcreds = json.loads(userdata.get(f"{USER}-{db}.json"))
            else:
                credentials_path = f"secrets/{USER}-{db}.json"
                dbcreds = json.load(open(credentials_path))

            if dbcreds["dbms"] == "mysql":
                import pymysql
                connection = pymysql.connect(
                    charset="utf8mb4",
                    connect_timeout=10,
                    cursorclass=pymysql.cursors.DictCursor,
                    database=dbcreds["database"],
                    host=dbcreds["host"],
                    password=dbcreds["password"],
                    read_timeout=10,
                    port=dbcreds["port"],
                    user=dbcreds["username"],
                    write_timeout=10,
                )

                sql = """
                INSERT INTO tomtom_traffic_flow (
                    hostfeed,
                    source,
                    tstamp,
                    lat,
                    lon,
                    current_speed,
                    free_flow_speed,
                    current_travel_time,
                    free_flow_travel_time,
                    confidence,
                    road_closure,
                    geometry
                ) VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s);
                """
            elif dbcreds["dbms"] == "tidb":

                import pymysql
                if env == "render":
                    CA_PATH = f"/etc/secrets/{dbcreds['ca_path']}"
                elif env == "colab":
                    CA_SECRET = userdata.get(f"{dbcreds['ca_path']}")
                    with open(f"/tmp/{USER}.pem", "w") as f:
                        f.write(CA_SECRET)
                    CA_PATH = f"/tmp/{USER}.pem"
                else:
                    CA_PATH = f"secrets/{dbcreds['ca_path']}"

                connection = pymysql.connect(
                    host=dbcreds["host"],
                    port=dbcreds["port"],
                    user=dbcreds["username"],
                    password=dbcreds["password"],
                    database=dbcreds["database"],
                    cursorclass=pymysql.cursors.DictCursor,
                    ssl_verify_cert=True,
                    ssl_verify_identity=True,
                    ssl_ca=CA_PATH,
                )

                sql = """
                INSERT INTO tomtom_traffic_flow (
                    hostfeed,
                    source,
                    tstamp,
                    lat,
                    lon,
                    current_speed,
                    free_flow_speed,
                    current_travel_time,
                    free_flow_travel_time,
                    confidence,
                    road_closure,
                    geometry
                ) VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s);
                """

            elif dbcreds["dbms"] == "crate":
                sql = """
                INSERT INTO tomtom_traffic_flow (
                    hostfeed,
                    source,
                    tstamp,
                    lat,
                    lon,
                    current_speed,
                    free_flow_speed,
                    current_travel_time,
                    free_flow_travel_time,
                    confidence,
                    road_closure,
                    geometry
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                """

                from crate import client
                connection = client.connect(dbcreds["host"], username=dbcreds["username"],
                                            password=dbcreds["password"],             verify_ssl_cert=True, timeout=10)

        except Exception as e:
            print(f"Error for {db}: {e}")
            clts.elapt[f"Connection to {db} Failed, Error: {e}"] = clts.deltat(
                tstart)
            continue

        cursor = connection.cursor()
        print(f"Connected to {db} successfully")
        clts.elapt[f"Connection to {db} Successful"] = clts.deltat(tstart)
        status = "ok"

        try:
            if status == "ok":

                sql_check_duplicate = """
                SELECT COUNT(*) AS count FROM tomtom_traffic_flow
                WHERE lat = %s AND lon = %s AND tstamp = %s
                """

                values_check_duplicate = (
                    lat, lon, timestamp
                )

                if dbcreds["dbms"] == "crate":
                    sql_check_duplicate = """
                    SELECT COUNT(*) AS count FROM tomtom_traffic_flow
                    WHERE lat = ? AND lon = ? AND tstamp = ?
                    """

                cursor.execute(sql_check_duplicate, values_check_duplicate)
                result = cursor.fetchone()

                if dbcreds["dbms"] == "crate":
                    count = result[0]
                else:
                    count = result['count']

                if count == 0:
                    cursor.execute(sql, values)
                    connection.commit()
                    print(f"Data inserted into {db} successfully")
                    clts.elapt[f"Data Inserted into {db} Successfully"] = clts.deltat(
                        tstart)
                elif count == 1:
                    clts.elapt[f"Data for timestamp: {timestamp} in location: {lat}, {lon} already exists in {db}, Skipping Insertion"] = clts.deltat(
                        tstart)
                else:
                    clts.elapt[f"Duplicate Count in {db} for location: {lat}, {lon} and timestamp: {timestamp}, count: {count}"] = clts.deltat(
                        tstart)

        except Exception as e:
            print(f"Error inserting data into {db}: {e}")
            clts.elapt[f"Data Insertion into {db} Failed, Error: {e}"] = clts.deltat(
                tstart)

        connection.close()
        print(f"Connection to {db} closed")
        clts.elapt[f"Connection to {db} Closed"] = clts.deltat(tstart)


toemail = clts.listtimes()
print(toemail)

if env == "render":
    import resend
    resend.api_key = RESEND_API_KEY
    try:
        result = resend.Emails.send({
            "from": "Acme <onboarding@resend.dev>",
            "to": ["xavierkooijman@gmail.com"],
            "subject": "TomTom TrafficFlow Data Retrieval Report",
            "html": toemail,
        })

        print("Email sent successfully!")
        print(f"Email ID: {result['id']}")
    except Exception as e:
        print(f"Error sending email: {e}")
        exit(1)

else:

    import smtplib
    from email.mime.text import MIMEText

    SMTP_SERVER = "smtp.gmail.com"
    SMTP_PORT = 587

    try:
        msg = MIMEText(toemail, "html")
        msg["Subject"] = "TomTom TrafficFlow Data Retrieval Report"
        msg["From"] = EMAIL_FROM
        msg["To"] = ", ".join(RECEIVERS_LIST)

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_FROM, EMAIL_PASSWORD)
            server.sendmail(EMAIL_FROM, RECEIVERS_LIST, msg.as_string())

        print("Email sent!")
    except Exception as e:
        print(f"Error sending email: {e}")
