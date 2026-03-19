#!pip install clts_pcp --quiet
#!pip install crate --quiet
#!pip install pymysql --quiet
#!pip install cryptography --quiet
import os
import sys
import requests
import json
import clts_pcp as clts
import pymysql
import socket


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
    DB_LIST = json.loads(userdata.get(f"{USER}-dblist.json"))["databases"]

elif env == "render":
    USER = os.getenv("USER")
    EMAIL_FROM = os.getenv("EMAIL_FROM")
    RESEND_API_KEY = os.getenv("RESEND_API_KEY")
    DB_LIST = json.load(open(f"/etc/secrets/{USER}-dblist.json"))["databases"]

else:
    from dotenv import load_dotenv
    load_dotenv()

    USER = os.getenv("USER")
    EMAIL_FROM = os.getenv("EMAIL_FROM")
    EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
    DB_LIST = json.load(open(f"{USER}-dblist.json"))["databases"]


clts.setcontext(f'IPMA Weather Station Data Retrieval - Environment: {env}')


url = 'https://api.ipma.pt/open-data/observation/meteorology/stations/obs-surface.geojson'


clts.elapt["Start Data Retrieval"] = clts.deltat(tstart)
response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    print("Data retrieved successfully")
    print(f"Retrieved data from {len(data['features'])} stations")
    clts.elapt[f"Data Retrieved Successfully From {len(data['features'])} Stations"] = clts.deltat(
        tstart)
else:
    print(f"Failed to retrieve data {response.status_code}")
    clts.elapt[f"Data Retrieval Failed with Status Code {response.status_code}"] = clts.deltat(
        tstart)

for d in data['features']:
    if d['properties']['idEstacao'] == 1200545:
        single_station_data = d

print(single_station_data)


values = (
    hostname,
    'IPMA',
    single_station_data['properties']['idEstacao'],
    single_station_data['properties']['localEstacao'],
    single_station_data['geometry']['coordinates'][1],
    single_station_data['geometry']['coordinates'][0],
    single_station_data['properties']['time'],
    single_station_data['properties']['temperatura'],
    single_station_data['properties']['radiacao'],
    single_station_data['properties']['humidade'],
    single_station_data['properties']['pressao'],
    single_station_data['properties']['intensidadeVentoKM'],
    single_station_data['properties']['idDireccVento'],
    single_station_data['properties']['descDirVento'],
    single_station_data['properties']['precAcumulada']
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
			INSERT INTO ipma (
                hostfeed, fonte, idEstacao, localEstacao, lat, lon, tstamp,
                temperatura, radiacao, humidade, pressao, intensidadeVentoKM,
                idDireccVento, descDirVento, precAcumulada
			) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
			)
			"""
        elif dbcreds["dbms"] == "tidb":

            if env == "render":
                CA_PATH = f"/etc/secrets/{dbcreds['ca_path']}"
            elif env == "colab":
                CA_CONTENT = userdata.get(f"{dbcreds['ca_content']}")
                with open(f"/tmp/{USER}.pem", "w") as f:
                    f.write(CA_CONTENT)
                CA_PATH = f"/tmp/{USER}.pem"
            else:
                CA_PATH = f"secrets/{dbcreds['ca_path']}"

            connection = pymysql.connect(
                host=dbcreds["host"],
                port=dbcreds["port"],
                user=dbcreds["username"],
                password=dbcreds["password"],
                database=dbcreds["database"],
                ssl_verify_cert=True,
                ssl_verify_identity=True,
                ssl_ca=CA_PATH,
            )

            sql = """
			INSERT INTO ipma (
                hostfeed, fonte, idEstacao, localEstacao, lat, lon, tstamp,
                temperatura, radiacao, humidade, pressao, intensidadeVentoKM,
                idDireccVento, descDirVento, precAcumulada
			) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
			)
			"""

        elif dbcreds["dbms"] == "crate":
            sql = """
            INSERT INTO ipma (
                hostfeed, fonte, idEstacao, localEstacao, lat, lon, tstamp,
                temperatura, radiacao, humidade, pressao, intensidadeVentoKM,
                idDireccVento, descDirVento, precAcumulada
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
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
            cursor.execute(sql, values)
            connection.commit()

            print(f"Data inserted into {db} successfully")
            clts.elapt[f"Data Inserted into {db} Successfully"] = clts.deltat(
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
            "subject": "Hello from Resend Python!",
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
    receiver = "xavierkooijman@gmail.com"

    try:
        msg = MIMEText(toemail, "html")
        msg["Subject"] = "Python Email Test"
        msg["From"] = EMAIL_FROM
        msg["To"] = receiver

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_FROM, EMAIL_PASSWORD)
            server.sendmail(EMAIL_FROM, receiver, msg.as_string())

        print("Email sent!")
    except Exception as e:
        print(f"Error sending email: {e}")
