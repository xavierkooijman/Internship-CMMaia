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
    OPEN_WEATHER_MAP_API_KEY = userdata.get("OPEN_WEATHER_MAP_API_KEY")
    DB_LIST = json.loads(userdata.get(f"{USER}-dblist.json"))["databases"]

elif env == "render":
    USER = os.getenv("USER")
    EMAIL_FROM = os.getenv("EMAIL_FROM")
    RESEND_API_KEY = os.getenv("RESEND_API_KEY")
    OPEN_WEATHER_MAP_API_KEY = os.getenv("OPEN_WEATHER_MAP_API_KEY")
    DB_LIST = json.load(open(f"/etc/secrets/{USER}-dblist.json"))["databases"]

else:
    from dotenv import load_dotenv
    load_dotenv()

    USER = os.getenv("USER")
    EMAIL_FROM = os.getenv("EMAIL_FROM")
    EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
    OPEN_WEATHER_MAP_API_KEY = os.getenv("OPEN_WEATHER_MAP_API_KEY")
    DB_LIST = json.load(open(f"{USER}-dblist.json"))["databases"]


clts.setcontext(
    f'OpenWeatherMap Weather Station Data Retrieval - Environment: {env}')


geoCoding_url = f'http://api.openweathermap.org/geo/1.0/direct?q=Maia,PT&appid={OPEN_WEATHER_MAP_API_KEY}'


clts.elapt["Start Geocode Data Retrieval"] = clts.deltat(tstart)

data_status = "nok"

try:
    geoCoding_response = requests.get(geoCoding_url)
    geoCoding_response.raise_for_status()
    geoCode_data = geoCoding_response.json()
    print("Geocode data retrieved successfully")
    clts.elapt[f"Geocode Data Retrieved Successfully"] = clts.deltat(
        tstart)

    openWeatherMap_url = f'http://api.openweathermap.org/data/2.5/weather?lat={geoCode_data[0]["lat"]}&lon={geoCode_data[0]["lon"]}&units=metric&appid={OPEN_WEATHER_MAP_API_KEY}'
except Exception as e:
    print(f"Error during geocode data retrieval: {e}")
    clts.elapt[f"Geocode Data Retrieval Failed, Error: {e}"] = clts.deltat(
        tstart)
    clts.elapt[f"Using default coordinates on API call"] = clts.deltat(
        tstart)

    openWeatherMap_url = f'http://api.openweathermap.org/data/2.5/weather?lat=41.2357&lon=-8.6199&units=metric&appid={OPEN_WEATHER_MAP_API_KEY}'

try:
    weather_response = requests.get(openWeatherMap_url)
    weather_response.raise_for_status()
    weather_data = weather_response.json()
    print("Weather data retrieved successfully")
    clts.elapt[f"Weather Data Retrieved Successfully"] = clts.deltat(
        tstart)
    data_status = "ok"
except Exception as e:
    print(f"Error during weather data retrieval: {e}")
    clts.elapt[f"Weather Data Retrieval Failed, Error: {e}"] = clts.deltat(
        tstart)

print("API Response:", weather_data)


if data_status == "ok":

    values = (
        hostname,
        "OpenWeatherMap",
        weather_data["name"],
        weather_data["coord"]["lon"],
        weather_data["coord"]["lat"],
        weather_data["dt"],
        weather_data["main"]["temp"],
        weather_data["main"].get("sea_level"),
        weather_data["main"].get("grnd_level"),
        weather_data["main"]["humidity"],
        weather_data["wind"]["speed"],
        weather_data["wind"]["deg"],
        weather_data.get("wind", {}).get("gust"),
        weather_data["visibility"],
        weather_data["clouds"]["all"]
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
                INSERT INTO openWeatherMap (
                    hostfeed,
                    source,
                    station_location,
                    lon,
                    lat,
                    tstamp,
                    temperature,
                    sea_level_pressure,
                    ground_level_pressure,
                    humidity_percent,
                    wind_speed_m_s,
                    wind_direction_deg,
                    wind_gust_m_s,
                    visibility_meters,
                    cloudiness_percent
                ) VALUES (%s, %s, %s, %s, %s,FROM_UNIXTIME(%s), %s, %s, %s, %s, %s, %s, %s, %s, %s);
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
                INSERT INTO openWeatherMap (
                    hostfeed,
                    source,
                    station_location,
                    lon,
                    lat,
                    tstamp,
                    temperature,
                    sea_level_pressure,
                    ground_level_pressure,
                    humidity_percent,
                    wind_speed_m_s,
                    wind_direction_deg,
                    wind_gust_m_s,
                    visibility_meters,
                    cloudiness_percent
                ) VALUES (%s, %s, %s, %s, %s,FROM_UNIXTIME(%s), %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """

            elif dbcreds["dbms"] == "crate":
                sql = """
                INSERT INTO openWeatherMap (
                    hostfeed,
                    source,
                    station_location,
                    lon,
                    lat,
                    tstamp,
                    temperature,
                    sea_level_pressure,
                    ground_level_pressure,
                    humidity_percent,
                    wind_speed_m_s,
                    wind_direction_deg,
                    wind_gust_m_s,
                    visibility_meters,
                    cloudiness_percent
                ) VALUES (?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
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
                SELECT COUNT(*) AS count FROM openWeatherMap
                WHERE station_location = %s AND tstamp = %s
                """

                values_check_duplicate = (
                    weather_data["name"], weather_data['dt']
                )

                if dbcreds["dbms"] == "crate":
                    sql_check_duplicate = """
                    SELECT COUNT(*) AS count FROM openWeatherMap
                    WHERE station_location = ? AND tstamp = ?
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
                    clts.elapt[f"Data for station: {weather_data["name"]} and timestamp: {weather_data["dt"]} already exists in {db}, Skipping Insertion"] = clts.deltat(
                        tstart)
                else:
                    clts.elapt[f"Duplicate Count in {db} for station: {weather_data["name"]} and timestamp: {weather_data["dt"]}, count: {count}"] = clts.deltat(
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
            "subject": "OpenWeatherMap Weather Station Data Retrieval Report",
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
        msg["Subject"] = "OpenWeatherMap Weather Station Data Retrieval Report"
        msg["From"] = EMAIL_FROM
        msg["To"] = receiver

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_FROM, EMAIL_PASSWORD)
            server.sendmail(EMAIL_FROM, receiver, msg.as_string())

        print("Email sent!")
    except Exception as e:
        print(f"Error sending email: {e}")
