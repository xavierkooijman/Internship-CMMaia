#!pip install clts_pcp --quiet
#!pip install crate --quiet
#!pip install pymysql --quiet
#!pip install cryptography --quiet
#!pip install geopandas --quiet
#!pip install shapely --quiet
import os
import sys
import requests
import json
import clts_pcp as clts
import socket
from datetime import datetime
import geopandas as gpd
from shapely.geometry import Point


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


def filter_coordinates_within_maia(data, maia_gdf):
    clts.elapt["Filter Coordinates within Maia"] = clts.deltat(tstart)
    maia_polygon = maia_gdf.geometry.iloc[0]
    minx, miny, maxx, maxy = maia_polygon.bounds

    gdf_points = gpd.GeoDataFrame([
        {
            "globalid": feature["properties"]["globalid"].strip("{}"),
            "marca": feature["properties"]["Marca"],
            "geometry": Point(feature["geometry"]["coordinates"])

        }
        for feature in data["features"]
    ], geometry="geometry", crs="EPSG:4326")

    gdf_points = gdf_points.cx[minx:maxx, miny:maxy]
    clts.elapt["Coordinates Filtered by Bounding Box"] = clts.deltat(tstart)

    gdf_filtered = gdf_points[gdf_points.geometry.within(maia_polygon)]
    clts.elapt["Coordinates Filtered within Maia Polygon"] = clts.deltat(
        tstart)

    return gdf_filtered


def get_user():
    if "__file__" in globals():
        filename = os.path.basename(__file__)

    else:
        try:
            sessions = requests.get(
                "http://172.28.0.12:9000/api/sessions").json()
            filename = sessions[0]["name"]
        except:
            filename = os.path.basename(sys.argv[0])

    if "_" in filename:
        return filename.split("_")[0]

    return None


env = detect_environment()
clts.elapt[f"Environment Detected: {env}"] = clts.deltat(tstart)
print("Running in:", env)

USER = get_user()
print(f"User detected: {USER}")

if env == "colab":
    from google.colab import userdata
    EMAIL_FROM = userdata.get("EMAIL_FROM")
    EMAIL_PASSWORD = userdata.get("EMAIL_PASSWORD")
    DB_LIST = json.loads(userdata.get(f"{USER}-dblist.json"))["databases"]
    geojson_str = userdata.get("maia_polygon.geojson")
    geojson_dict = json.loads(geojson_str)
    maia_gdf = gpd.GeoDataFrame.from_features(
        geojson_dict["features"]
    ).set_crs(epsg=4326)

elif env == "render":
    EMAIL_FROM = os.getenv("EMAIL_FROM")
    RESEND_API_KEY = os.getenv("RESEND_API_KEY")
    DB_LIST = json.load(open(f"/etc/secrets/{USER}-dblist.json"))["databases"]
    maia_gdf = gpd.read_file("maia_polygon.geojson").to_crs(epsg=4326)


else:
    from dotenv import load_dotenv
    load_dotenv()

    EMAIL_FROM = os.getenv("EMAIL_FROM")
    EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
    DB_LIST = json.load(open(f"{USER}-dblist.json"))["databases"]
    maia_gdf = gpd.read_file("maia_polygon.geojson").to_crs(epsg=4326)


clts.setcontext(
    f'ServerGeo Postos de Abastecimento Data Retrieval - Environment: {env}')


url = f'https://servergeo.dgeg.gov.pt/arcgis/services/Visualizadores/PACVR/MapServer/WFSServer?request=GetFeature&service=WFS&typename=PACVR:Postos_Abastecimento&outputFormat=GEOJSON'


data_status = "nok"

try:
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    print("Postos de Abastecimento data retrieved successfully")
    clts.elapt[f"Postos de Abastecimento Data Retrieved Successfully"] = clts.deltat(
        tstart)
    data_status = "ok"
except Exception as e:
    print(f"Error during postos de abastecimento data retrieval: {e}")
    clts.elapt[f"Postos de Abastecimento Data Retrieval Failed, Error: {e}"] = clts.deltat(
        tstart)


if data_status == "ok":

    filtered_rows = filter_coordinates_within_maia(data, maia_gdf)

    current_timestamp = datetime.now().isoformat()

    values = []
    values_check_duplicate = []

    for idx, row in filtered_rows.iterrows():
        values.append(
            (
                row["globalid"],
                hostname,
                "ServerGeo GDEG",
                row.geometry.y,
                row.geometry.x,
                row["marca"],
                current_timestamp,
            )
        )
        values_check_duplicate.append(
            (
                row["globalid"],
            )
        )

    print(values)

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
                INSERT INTO postos_abastecimento
                (globalId, hostfeed, source, lat, lon, marca, tstamp)
                VALUES (UUID_TO_BIN(%s), %s, %s, %s, %s, %s, %s)
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
                INSERT INTO postos_abastecimento
                (globalId, hostfeed, source, lat, lon, marca, tstamp)
                VALUES (UUID_TO_BIN(%s), %s, %s, %s, %s, %s, %s)
                """

            elif dbcreds["dbms"] == "crate":
                sql = """
                INSERT INTO postos_abastecimento
                (globalId, hostfeed, source, lat, lon, marca, tstamp)
                VALUES (cast(? as uuid), ?, ?, ?, ?, ?, ?)
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

                cursor.executemany(sql, values)
                connection.commit()
                print(f"Data inserted into {db} successfully")
                clts.elapt[f"Data Inserted into {db} Successfully"] = clts.deltat(
                    tstart)

        except Exception as e:
            print(f"Error inserting data into {db}: {e}")
            clts.elapt[f"Data Insertion into {db} Failed, Error: {e}"] = clts.deltat(
                tstart)

        cursor.close()
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
