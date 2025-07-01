import os
import json
import base64
import hashlib
import hmac
from datetime import datetime, timedelta
from dotenv import load_dotenv
from apscheduler.schedulers.blocking import BlockingScheduler
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timezone
import httpx
import time
from sqlalchemy.orm import Session
from sqlalchemy import func, and_
from datetime import datetime, date
import logging
from datetime import datetime, time
import calendar

# billing meter stuff
import tinytuya
# ends


# Load environment variables
load_dotenv()
DATABASE_URL = "mssql+pyodbc://localhost/GSIDB?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
# Database setup
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

# ORM models
class PlantInformation(Base):
    __tablename__ = 'PlantInformation'
    Id = Column(BigInteger, primary_key=True, autoincrement=True)
    PlantId = Column(String(255), unique=True, nullable=False)
    Provider = Column(String)
    Name = Column(String)
    Capacity = Column(Float)
    TotalEnergy = Column(Float)
    TodayEnergy = Column(Float)
    UpdatedOn = Column(DateTime)
    CreatedOn = Column(DateTime)
    Address = Column(String)
    RoiInvestment = Column(Float)
    CreatedTime = Column(DateTime)

from sqlalchemy import Column, String, BigInteger, Float, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class PlantDevice(Base):
    __tablename__ = 'PlantDevice'

    Id = Column(BigInteger, primary_key=True, autoincrement=True)
    Deviceid = Column(BigInteger)
    DeviceSn = Column(String(250), unique=True, nullable=False)
    PlantId = Column(String)  # stationId
    Name = Column(String)     # stationName

    # General Info
    CollectorSn = Column(String)
    UserId = Column(String)
    ProductModel = Column(String)
    Model = Column(String)
    NationalStandards = Column(String)
    InverterSoftwareVersion = Column(String)
    InverterSoftwareVersion2 = Column(String)
    DcInputType = Column(Integer)
    AcOutputType = Column(Integer)
    StationType = Column(Integer)
    Tag = Column(String)
    Rs485ComAddr = Column(String)
    SimFlowState = Column(Integer)

    # Power info
    Power = Column(Float)
    Power1 = Column(Float)
    PowerStr = Column(String)
    PowerPercent = Column(Float)
    PowerPercentStr = Column(String)
    Pac = Column(Float)
    Pac1 = Column(Float)
    PacStr = Column(String)

    # State info
    State = Column(Integer)
    StateExceptionFlag = Column(Integer)
    IvSupport = Column(Integer)
    InverterConfig = Column(String)
    CurrentState = Column(String)

    # Runtime stats
    FullHour = Column(Float)
    TotalFullHour = Column(Float)
    MaxDcBus = Column(Float)
    MaxDcBusTime = Column(String)
    MaxUac = Column(Float)
    MaxUacTime = Column(String)
    MaxUpv = Column(Float)
    MaxUpvTime = Column(String)

    # Timezone and Timestamps
    TimeZone = Column(Float)
    TimeZoneStr = Column(String)
    TimeZoneName = Column(String)
    DataTimestamp = Column(String)
    DataTimestampStr = Column(String)
    FisTime = Column(String)
    FisTimeStr = Column(String)
    FisGenerateTime = Column(BigInteger)
    FisGenerateTimeStr = Column(String)

    # Firmware update/shelf
    InverterMeterModel = Column(Integer)
    UpdateShelfBeginTime = Column(BigInteger)
    UpdateShelfEndTime = Column(BigInteger)
    UpdateShelfEndTimeStr = Column(String)
    UpdateShelfTime = Column(String)
    ShelfState = Column(String)
    CollectorId = Column(String)

    # Parallel system
    DispersionRate = Column(Float)
    ParallelStatus = Column(Integer)
    ParallelAddr = Column(Integer)
    ParallelPhase = Column(Integer)
    ParallelBattery = Column(Integer)
    ParallelSign = Column(Integer)
    ParallelOnoff01 = Column(Float)
    ParallelOnoff02 = Column(Float)

    # MPPT power strings
    Pow1 = Column(Float)
    Pow2 = Column(Float)
    Pow3 = Column(Float)
    Pow4 = Column(Float)
    Pow5 = Column(Float)
    Pow6 = Column(Float)
    Pow7 = Column(Float)
    Pow8 = Column(Float)
    Pow9 = Column(Float)
    Pow10 = Column(Float)
    Pow11 = Column(Float)
    Pow12 = Column(Float)
    Pow13 = Column(Float)
    Pow14 = Column(Float)
    Pow15 = Column(Float)
    Pow16 = Column(Float)
    Pow17 = Column(Float)
    Pow18 = Column(Float)
    Pow19 = Column(Float)
    Pow20 = Column(Float)
    Pow21 = Column(Float)
    Pow22 = Column(Float)
    Pow23 = Column(Float)
    Pow24 = Column(Float)
    Pow25 = Column(Float)
    Pow26 = Column(Float)
    Pow27 = Column(Float)
    Pow28 = Column(Float)
    Pow29 = Column(Float)
    Pow30 = Column(Float)
    Pow31 = Column(Float)
    Pow32 = Column(Float)

    # Energy flow4\
    GridPurchasedTodayEnergy = Column(Float)
    GridPurchasedTodayEnergyStr = Column(String)
    GridSellTodayEnergy = Column(Float)
    GridSellTodayEnergyStr = Column(String)
    PsumCalPec = Column(String)
    BatteryPower = Column(Float)
    BatteryPowerStr = Column(String)
    BatteryPowerPec = Column(String)
    BatteryCapacitySoc = Column(Float)

    # Battery energy stats
    BatteryTodayChargeEnergy = Column(Float)
    BatteryTodayChargeEnergyStr = Column(String)
    BatteryTotalChargeEnergy = Column(Float)
    BatteryTotalChargeEnergyStr = Column(String)
    BatteryTodayDischargeEnergy = Column(Float)
    BatteryTodayDischargeEnergyStr = Column(String)
    BatteryTotalDischargeEnergy = Column(Float)
    BatteryTotalDischargeEnergyStr = Column(String)

    # Load stats
    BypassLoadPower = Column(Float)
    BypassLoadPowerStr = Column(String)
    BackupTodayEnergy = Column(Float)
    BackupTodayEnergyStr = Column(String)
    BackupTotalEnergy = Column(Float)
    BackupTotalEnergyStr = Column(String)
    FamilyLoadPower = Column(Float)
    FamilyLoadPowerStr = Column(String)
    TotalLoadPower = Column(Float)
    TotalLoadPowerStr = Column(String)
    HomeLoadTodayEnergy = Column(Float)
    HomeLoadTodayEnergyStr = Column(String)

    # Misc
    IsS5 = Column(Integer)
    BypassAcOnoffSet = Column(Float)
    RfState = Column(Integer)
    AfciSupportedSec = Column(Integer)
    DspSoftwareVersion3 = Column(Integer)
    DspSoftwareVersion4 = Column(Integer)
    DspSoftwareVersion5 = Column(Integer)

    # Energy metrics
    Etoday1 = Column(Float)
    Etotal1 = Column(Float)
    Etoday = Column(Float)
    Etotal = Column(Float)
    EtotalStr = Column(String)
    EtodayStr = Column(String)
    Psum = Column(Float)
    PsumCal = Column(Float)
    PsumStr = Column(String)
    PsumCalStr = Column(String)
    OfflineLongStr = Column(String)

    # Extra metadata
    GroupId = Column(String)

    # Audit fields
    CreatedOn = Column(DateTime)
    UpdatedOn = Column(DateTime)

class MeterGraph(Base):
    __tablename__ = "MeterGraph"
    Id = Column(Integer, primary_key=True, autoincrement=True)
    MeterId = Column(String)  # New column for Meter ID (mapped from deviceId)
    ForwardEnergy = Column(Float)
    ReverseEnergy = Column(Float)
    SwitchStatus = Column(String)
    Temperature = Column(Float)
    TotalPower = Column(Float)
    CreatedOn = Column(DateTime, default=datetime.now)
    
class APISuccessResponses(Base):
    __tablename__ = "APISuccessResponses"
    Id = Column(Integer, primary_key=True, autoincrement=True)
    PlantId = Column(String)
    Provider = Column(String)
    APIMethod = Column(String)
    Response = Column(String)
    CreatedOn = Column(DateTime)
    Hour = Column(Integer)
    Mapped = Column(Integer)
    
class EnergyGraph(Base):
    __tablename__ = "EnergyGraph"
    Id = Column(Integer, primary_key=True, autoincrement=True)
    Plantid = Column(String)
    Provider = Column(String)
    Energy = Column(Float)
    Timeunit = Column(String)
    FetchDate = Column(DateTime)
    Day = Column(String)
    Month = Column(String)
    EnergySaving = Column(Float)
    Year = Column(String)
    EnergyCalc = Column(Float)
    Import  = Column(Float)
    Export  = Column(Float)
    Comsumption = Column(Float)
    BatteryEnergyCharge = Column(Float)
    BatteryEnergyDischarge = Column(Float)
    
class PowerGraph(Base):
    __tablename__ = 'PowerGraph'

    Id = Column(Integer, primary_key=True, autoincrement=True)
    PlantId = Column(String(255))
    Power = Column(Float)
    InverterId = Column(String)
    CreatedOn = Column(DateTime)
    FetchDateTime = Column(DateTime)
    Provider = Column(String(50), nullable=False)
    IsDelted = Column(Integer, default=0)
    PowerDateTime = Column(DateTime)
    ClientId = Column(Integer)
    DeviceStatus = Column(String(200))

    DataTimestamp = Column(BigInteger)
    TimeStr = Column(String(25))
    AcOutputType = Column(Integer)
    DcInputType = Column(Integer)
    State = Column(Integer)
    Time = Column(String(10))
    Pac = Column(Float)
    PacStr = Column(String(10))
    PacPec = Column(String(20))
    EToday = Column(Float)
    ETotal = Column(Float)

    # PV voltages and currents
    for i in range(1, 33):
        vars()[f'UPv{i}'] = Column(Float)
        vars()[f'IPv{i}'] = Column(Float)

    UAc1 = Column(Float)
    IAc1 = Column(Float)
    UAc2 = Column(Float)
    IAc2 = Column(Float)
    UAc3 = Column(Float)
    IAc3 = Column(Float)
    InverterTemperature = Column(Float)
    PowerFactor = Column(Float)
    Fac = Column(Float)

    StorageBatteryVoltage = Column(Float)
    StorageBatteryCurrent = Column(Float)
    CurrentDirectionBattery = Column(Float)
    LlcBusVoltage = Column(Float)
    DcBus = Column(Float)
    DcBusHalf = Column(Float)

    BypassAcVoltage = Column(Float)
    BypassAcCurrent = Column(Float)
    BatteryCapacitySoc = Column(Float)
    BatteryHealthSoh = Column(Float)
    BatteryPower = Column(Float)
    BatteryVoltage = Column(Float)
    BstteryCurrent = Column(Float)
    BatteryChargingCurrent = Column(Float)
    BatteryDischargeLimiting = Column(Float)
    FamilyLoadPower = Column(Float)
    BypassLoadPower = Column(Float)

    BatteryTotalChargeEnergy = Column(Float)
    BatteryTodayChargeEnergy = Column(Float)
    BatteryYesterdayChargeEnergy = Column(Float)
    BatteryTotalDischargeEnergy = Column(Float)
    BatteryTodayDischargeEnergy = Column(Float)
    BatteryYesterdayDischargeEnergy = Column(Float)

    GridPurchasedTotalEnergy = Column(Float)
    GridPurchasedTodayEnergy = Column(Float)
    GridPurchasedYesterdayEnergy = Column(Float)

    GridSellTotalEnergy = Column(Float)
    GridSellTodayEnergy = Column(Float)
    GridSellYesterdayEnergy = Column(Float)

    HomeLoadTotalEnergy = Column(Float)
    HomeLoadTodayEnergy = Column(Float)
    HomeLoadYesterdayEnergy = Column(Float)

    TimeZone = Column(Integer)
    BatteryType = Column(Integer)
    SocDischargeSet = Column(Float)
    SocChargingSet = Column(Float)
    EpmFailSafe = Column(Float)
    PSum = Column(Float)
    UA = Column(Float)
    PepmSet = Column(Float)
    Pepm = Column(Float)
    UinitGnd = Column(Float)
    PlimitSet = Column(Float)
    PfactorLimitSet = Column(Float)
    PreactiveLimitSet = Column(Float)

# Create tables
Base.metadata.create_all(bind=engine)

# OLD - Solis API configuration
# API_ID = "1300386381676648045"
#API_SECRET = "be265f0e10b44241a666bcd59af84b6e"
#API_BASE_URL = "https://www.soliscloud.com:13333"
#PLANT_RESOURCE_PATH = "/v1/api/userStationList"
#DEVICE_RESOURCE_PATH = "/v1/api/inverterList"

# NEW - Solis API configuration - 17 JUNE 25
API_ID = "1300386381677823021"
API_SECRET = "14d17f95b63a480283dde2f5d24f4f0f"
API_BASE_URL = "https://www.soliscloud.com:13333"
PLANT_RESOURCE_PATH = "/v1/api/userStationList"
DEVICE_RESOURCE_PATH = "/v1/api/inverterList"



# billing meter stuff
B_API_REGION = "eu"
B_API_KEY = "a3gjfty73nynuyq8cqg7"
B_API_SECRET = "3524253b9c064e6fa4dbc2a3596181d3"
B_API_DEVICEID = "bf9542cfd38fa0cd87cm0y"
# ends


# Utility functions
def get_content_md5(body: str) -> str:
    hash_md5 = hashlib.md5(body.encode("utf-8")).digest()
    return base64.b64encode(hash_md5).decode("utf-8")

def get_gmt_time() -> str:
    return datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")

def get_signature(string_to_sign: str, secret: str) -> str:
    signature = hmac.new(secret.encode("utf-8"), string_to_sign.encode("utf-8"), hashlib.sha1).digest()
    return base64.b64encode(signature).decode("utf-8")

def parse_created_time(dt_str: str) -> datetime:
    dt_str = dt_str.split(' ')[0:2]
    dt_str = ' '.join(dt_str)
    try:
        dt = datetime.strptime(dt_str, "%d/%m/%Y %H:%M:%S")
    except ValueError:
        try:
            dt = datetime.strptime(dt_str, "%m/%d/%Y %H:%M:%S")
        except ValueError:
            dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    return dt

def build_address(record):
    parts = [
        record.get("addrOrigin"),
        record.get("countyStr"),
        record.get("cityStr"),
        record.get("regionStr")
    ]
    parts = [p for p in parts if p]
    return ", ".join(parts)

# billing meter stuff
# Function
def fetch_and_store_meter_data():
    session = SessionLocal()
    try:
        #device_id = "bf9542cfd38fa0cd87cm0y"  # Treat this as MeterId

        c = tinytuya.Cloud(
            apiRegion=B_API_REGION, 
            apiKey=B_API_KEY, 
            apiSecret=B_API_SECRET, 
            apiDeviceID=B_API_DEVICEID
        )

        result = c.getstatus(B_API_DEVICEID)

        forward_energy = result['result'][0]['value'] / 100
        reverse_energy = result['result'][22]['value'] / 100
        switch_status = result['result'][10]['value']
        switch_temp = result['result'][15]['value']
        totalPower = result['result'][23]['value'] / 1000

        
        meter_record = MeterGraph(
            MeterId=B_API_DEVICEID,  # Store as MeterId
            ForwardEnergy=forward_energy,
            ReverseEnergy=reverse_energy,
            SwitchStatus=switch_status,
            Temperature=switch_temp,
            TotalPower=totalPower
        )
        session.add(meter_record)
        session.commit()
        session.close()

        print("Meter data stored successfully.")

    except Exception as e:
        print(f"Error fetching/storing meter data: {e}")



# Fetch and store plant information
def fetch_and_store_plants():
    print(f"[{datetime.utcnow()}] Fetching plant data...")

    page = 1
    page_size = 10
    has_more = True

    while has_more:
        body = json.dumps({"pageNo": page, "pageSize": page_size})
        content_md5 = get_content_md5(body)
        date = get_gmt_time()
        content_type = "application/json"
        string_to_sign = f"POST\n{content_md5}\n{content_type}\n{date}\n{PLANT_RESOURCE_PATH}"
        signature = get_signature(string_to_sign, API_SECRET)

        headers = {
            "Content-MD5": content_md5,
            "Content-Type": content_type,
            "Date": date,
            "Authorization": f"API {API_ID}:{signature}"
        }

        url = f"{API_BASE_URL}{PLANT_RESOURCE_PATH}"
        with httpx.Client() as client:
            response = client.post(url, data=body, headers=headers)
            response.raise_for_status()
            data = response.json()

        if not data.get("success"):
            print("Failed to fetch plant data")
            return

        records = data["data"]["page"]["records"]
        if not records:
            break

        save_plants_to_db(records)
        page += 1
        has_more = page <= data["data"]["page"]["pages"]

def save_plants_to_db(records):
    session = SessionLocal()
    for record in records:
        plant = session.query(PlantInformation).filter_by(PlantId=str(record["id"])).first()
        if not plant:
            plant = PlantInformation(PlantId=str(record["id"]), CreatedOn=datetime.utcnow())

        plant.Provider = "Solis"
        plant.Name = record.get("stationName")
        plant.Capacity = record.get("capacity")
        plant.TotalEnergy = record.get("allEnergy1")
        plant.TodayEnergy = record.get("dayEnergy1")
        plant.UpdatedOn =datetime.utcnow() + timedelta(hours=5)
        plant.Address = build_address(record)

        create_date_str = record.get("createDateStr")
        if create_date_str:
            plant.CreatedTime = parse_created_time(create_date_str)

        session.add(plant)
    session.commit()
    session.close()
# Fetch and store inverter/device data
def fetch_and_store_devices():
    print(f"[{datetime.utcnow()}] Fetching device data...")

    page = 1
    page_size = 10
    has_more = True

    while has_more:
        body = json.dumps({"pageNo": page, "pageSize": page_size})
        content_md5 = get_content_md5(body)
        date = get_gmt_time()
        content_type = "application/json"
        string_to_sign = f"POST\n{content_md5}\n{content_type}\n{date}\n{DEVICE_RESOURCE_PATH}"
        signature = get_signature(string_to_sign, API_SECRET)

        headers = {
            "Content-MD5": content_md5,
            "Content-Type": content_type,
            "Date": date,
            "Authorization": f"API {API_ID}:{signature}"
        }

        url = f"{API_BASE_URL}{DEVICE_RESOURCE_PATH}"
        with httpx.Client() as client:
            response = client.post(url, data=body, headers=headers)
            response.raise_for_status()
            data = response.json()

        if not data.get("success"):
            print("Failed to fetch device data")
            return

        records = data["data"]["page"]["records"]
        if not records:
            break

        save_devices_to_db(records)
        page += 1
        has_more = page <= data["data"]["page"]["pages"]

def save_devices_to_db(records):
    session = SessionLocal()
    for record in records:
        device_sn = str(record.get("sn"))
        if not device_sn:
            continue

        device = session.query(PlantDevice).filter_by(DeviceSn=device_sn).first()
        if not device:
            device = PlantDevice(DeviceSn=device_sn, CreatedOn=datetime.utcnow())

        # Set audit/update time
        device.UpdatedOn = datetime.utcnow() + timedelta(hours=5)

        # Manual fields with different names
        device.PlantId = str(record.get("stationId"))
        device.Name = record.get("deviceName")
        device.Deviceid = record.get("id")

        # Dynamic field mapping
        for key, value in record.items():
            if key == "id":
                continue  # skip to avoid conflict with primary key
            
            model_attr = key[0].upper() + key[1:] 
            if hasattr(device, model_attr):
                try:
                    setattr(device, model_attr, value)
                except Exception as e:
                    print(f"Failed to set attribute {key}: {e}")

        session.add(device)

    session.commit()
    session.close()
    
def summarize_energygraph_monthly():
    session = SessionLocal()
    today = date.today()
    year_str = today.strftime("%Y")
    month_str = today.strftime("%m")
    full_month_str = f"{year_str}-{month_str}"
    first_day_str = f"{full_month_str}-01"
    today_str = today.strftime("%Y-%m-%d")

    plant_ids = session.query(PlantInformation.PlantId).all()

    for (plant_id,) in plant_ids:
        # Fetch daily records from Day 1 to Today for this month
        monthly_entries = session.query(EnergyGraph).filter(
            EnergyGraph.Plantid == plant_id,
            EnergyGraph.Timeunit == 'day',
            and_(
                EnergyGraph.Day >= first_day_str,
                EnergyGraph.Day <= today_str
            )
        ).all()

        if not monthly_entries:
            continue

        # Summing up required fields
        total_energy_calc = sum(e.EnergyCalc or 0 for e in monthly_entries)
        total_charge = sum(e.BatteryEnergyCharge or 0 for e in monthly_entries)
        total_discharge = sum(e.BatteryEnergyDischarge or 0 for e in monthly_entries)
        total_import = sum(e.Import or 0 for e in monthly_entries)
        total_export = sum(e.Export or 0 for e in monthly_entries)
        total_consumption = sum(e.Comsumption or 0 for e in monthly_entries)

        # Check if monthly summary already exists
        monthly_record = session.query(EnergyGraph).filter(
            EnergyGraph.Plantid == plant_id,
            EnergyGraph.Timeunit == 'month',
            EnergyGraph.Month == full_month_str,
            EnergyGraph.Year == year_str
        ).first()

        if monthly_record:
            monthly_record.EnergyCalc = total_energy_calc
            monthly_record.BatteryEnergyCharge = total_charge
            monthly_record.BatteryEnergyDischarge = total_discharge
            monthly_record.Import = total_import
            monthly_record.Export = total_export
            monthly_record.Comsumption = total_consumption

    session.commit()
    print("Monthly summary inserted or updated in EnergyGraph.")

def update_energy_graph_from_power_graph():
    session = SessionLocal()
    today = date.today()  # e.g., 2025-06-03
    today_str = today.strftime("%Y-%m-%d")
    start_of_day = datetime.combine(today, time.min)  # 00:00:00
    end_of_day = datetime.combine(today, time.max)  
    
    # Step 1: Get all plant IDs
    plant_ids = session.query(PlantInformation.PlantId).all()

    for (plant_id,) in plant_ids:
        # Step 2: Fetch latest PowerGraph record for today
        latest_power_data = (
            session.query(PowerGraph)
             .filter(
        PowerGraph.PlantId == plant_id,
        PowerGraph.PowerDateTime >= start_of_day,
        PowerGraph.PowerDateTime <= end_of_day
    )
    .order_by(PowerGraph.PowerDateTime.desc())
    .first()
    )

        if not latest_power_data:
            continue  # Skip if no data for today

        # Step 3: Fetch existing EnergyGraph entry for this plant and today
        energy_record = (
            session.query(EnergyGraph)
             .filter(
                EnergyGraph.Plantid == plant_id,
                EnergyGraph.Day == today_str
            )
            .first()
        )

        if energy_record:
            # Step 4: Map fields from PowerGraph to EnergyGraph
            energy_record.EnergyCalc = latest_power_data.EToday
            energy_record.Import = latest_power_data.GridPurchasedTodayEnergy
            energy_record.Export = latest_power_data.GridSellTodayEnergy
            energy_record.Comsumption = latest_power_data.HomeLoadTodayEnergy
            energy_record.BatteryEnergyCharge = latest_power_data.BatteryTodayChargeEnergy
            energy_record.BatteryEnergyDischarge = latest_power_data.BatteryTodayDischargeEnergy
            energy_record.FetchDate = datetime.now()

    session.commit()
    print("EnergyGraph table updated successfully.")
   
    
def map_and_save_energy_graph_from_api_responses():
    session = SessionLocal()
    try:
        # Step 1: Fetch unmapped Solis records
        api_records = session.query(APISuccessResponses).filter(
            APISuccessResponses.Mapped == 0,
            APISuccessResponses.Provider == 'Solis'
        ).all()

        for api_record in api_records:
            try:
                response_data = json.loads(api_record.Response)
            except json.JSONDecodeError:
                print(f"Invalid JSON for record id={api_record.id}")
                continue

            plantid = str(response_data.get("id"))
            energy = float(response_data.get("energy", 0))
            date_str = response_data.get("dateStr")
            provider = "Solis"
            timeunit = "day"
            month = None
            year = None
            energy_saving = 0.0  # Placeholder

            if not date_str:
                print(f"No dateStr for record id={api_record.id}")
                continue

            try:
                fetch_date = datetime.strptime(date_str, "%Y-%m-%d")
                month = fetch_date.strftime("%m")
                year = fetch_date.strftime("%Y")
            except Exception as e:
                print(f"Invalid dateStr format in record id={api_record.id}: {e}")
                continue

            # Step 2: Check if record exists
            existing_record = session.query(EnergyGraph).filter_by(
                Plantid=plantid,
                Day=date_str,
                Timeunit="day"
            ).first()

            if existing_record:
                existing_record.Provider = provider
                existing_record.Energy = energy
                existing_record.FetchDate = datetime.now()
                existing_record.Month = month
                existing_record.Year = year
                existing_record.EnergySaving = energy_saving
            else:
                new_entry = EnergyGraph(
                    Plantid=plantid,
                    Provider=provider,
                    Energy=energy,
                    Timeunit="day",
                    FetchDate=datetime.now(),
                    Day=date_str,
                    Month=month,
                    Year=year,
                    EnergySaving=energy_saving,
                )
                session.add(new_entry)

            # Step 3: Mark the response as mapped
            api_record.Mapped = 1

        session.commit()
        print(f"Processed and mapped {len(api_records)} API response records into EnergyGraph.")

    except Exception as e:
        session.rollback()
        print("Error processing EnergyGraph records:", e)

    finally:
        session.close()

def fetch_and_store_inverter_day_data():
    session = SessionLocal()
    try:
        devices = session.query(PlantDevice).all()
        if not devices:
            print("No devices found to fetch inverter day data")
            return

        today_str = datetime.utcnow().strftime("%Y-%m-%d")
        fetch_time = datetime.utcnow() + timedelta(hours=5)

        for device in devices:
            body = json.dumps({
                "id": "",
                "sn": device.DeviceSn,
                "money": "",
                "time": today_str,
                "timeZone": "5"
            })

            content_md5 = get_content_md5(body)
            date = get_gmt_time()
            content_type = "application/json"
            resource_path = "/v1/api/inverterDay"
            string_to_sign = f"POST\n{content_md5}\n{content_type}\n{date}\n{resource_path}"
            signature = get_signature(string_to_sign, API_SECRET)

            headers = {
                "Content-MD5": content_md5,
                "Content-Type": content_type,
                "Date": date,
                "Authorization": f"API {API_ID}:{signature}"
            }

            url = f"{API_BASE_URL}{resource_path}"
            with httpx.Client() as client:
                response = client.post(url, data=body, headers=headers)
                if response.status_code != 200:
                    print(f"Failed to fetch inverter day data for device {device.DeviceSn}")
                    continue

                data = response.json()

            if not data.get("success"):
                print(f"API response indicates failure for device {device.DeviceSn}")
                continue

            records = data.get("data", [])
            if not isinstance(records, list) or not records:
                print(f"No inverter day data records for device {device.DeviceSn}")
                continue

            for record in records:
                pac = record.get("pac")
                time_str = record.get("timeStr")

                if pac is None or time_str is None:
                    print(f"Missing pac or timeStr in record for device {device.DeviceSn}")
                    continue

                try:
                    power_datetime = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
                except Exception as e:
                    print(f"Invalid timeStr format for device {device.DeviceSn}: {e}")
                    continue

                existing = session.query(PowerGraph).filter_by(
                    PlantId=device.PlantId,
                    PowerDateTime=power_datetime,
                    InverterId=device.DeviceSn,
                ).first()

                if existing:
                    continue

                status = "Online" if float(pac) > 1 else "Offline"

                power_record = PowerGraph(
                    PlantId=device.PlantId,
                    Power=float(pac),
                    InverterId=device.DeviceSn,
                    CreatedOn=fetch_time,
                    FetchDateTime=fetch_time,
                    Provider="Solis",
                    IsDelted=0,
                    PowerDateTime=power_datetime,
                    DeviceStatus=status,
                )

                # Set dynamic attributes from the record
                for key, value in record.items():
                    attr_name = key[0].upper() + key[1:]
                    if hasattr(power_record, attr_name):
                        try:
                            setattr(power_record, attr_name, value)
                        except Exception as e:
                            print(f"Failed to set attribute {key}: {e}")

                session.add(power_record)

        session.commit()

    except Exception as e:
        session.rollback()
        print("Error fetching/storing inverter day data:", e)
    finally:
        session.close()

        
def upsert_monthly_energy_summary():
    session = SessionLocal()
    today = date.today()
    year = today.year
    month = today.month

    # First and last date of the current month
    first_day = date(year, month, 1)
    last_day = date(year, month, calendar.monthrange(year, month)[1])

    # Format month string as 'YYYY-MM'
    month_str = f"{year}-{month:02d}"

    # Step 1: Aggregate energy from daily records within the month
    daily_sums = (
        session.query(
            EnergyGraph.Plantid,
            EnergyGraph.Provider,
            func.sum(EnergyGraph.Energy).label("total_energy")
        )
        .filter(
            EnergyGraph.Timeunit == "day",
            EnergyGraph.Day >= first_day.strftime('%Y-%m-%d'),
            EnergyGraph.Day <= last_day.strftime('%Y-%m-%d')
        )
        .group_by(EnergyGraph.Plantid, EnergyGraph.Provider)
        .all()
    )

    # Step 2: Upsert into EnergyGraph as month-level summary
    for plant_id, provider, total_energy in daily_sums:
        existing = session.query(EnergyGraph).filter_by(
            Plantid=plant_id,
            Provider=provider,
            Timeunit="month",
            Month=month_str,
            Year=str(year),
            Day="NA"
        ).first()

        if existing:
            existing.Energy = total_energy
            existing.FetchDate = datetime.now()
        else:
            new_entry = EnergyGraph(
                Plantid=plant_id,
                Provider=provider,
                Energy=total_energy,
                Timeunit="month",
                FetchDate=datetime.now(),
                Day="NA",
                Month=month_str,
                Year=str(year),
                EnergySaving=0.0
            )
            session.add(new_entry)

    session.commit()
 
        
def call_station_day_energy_list():
    try:
        page = 1
        page_size = 10
        has_more = True

        while has_more:
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            body = json.dumps({"pageNo": page, "pageSize": page_size, "time": date_str})

            content_md5 = get_content_md5(body)
            content_type = "application/json"
            date = get_gmt_time()
            resource_path = "/v1/api/stationDayEnergyList"
            string_to_sign = f"POST\n{content_md5}\n{content_type}\n{date}\n{resource_path}"
            signature = get_signature(string_to_sign, API_SECRET)

            headers = {
                "Content-MD5": content_md5,
                "Content-Type": content_type,
                "Date": date,
                "Authorization": f"API {API_ID}:{signature}"
            }

            url = f"{API_BASE_URL}{resource_path}"
            with httpx.Client() as client:
                response = client.post(url, data=body, headers=headers)
                response.raise_for_status()
                data = response.json()

            if not data.get("success", False):
                print("API call failed:", data.get("msg"))
                return

            records = data.get("data", {}).get("records", [])
            if not records:
                print("No records found on page", page)
                break

            session = SessionLocal()
            try:
                for record in records:
                    plant_id = str(record.get("id"))
                    record_json = json.dumps(record)

                    api_response_entry = APISuccessResponses(
                        PlantId=plant_id,
                        Provider="Solis",
                        APIMethod="Solis_StationDayEnergyList",
                        Response=record_json,
                        CreatedOn=datetime.now(timezone.utc),
                        Hour=datetime.now(timezone.utc).hour,
                        Mapped=0,
                    )
                    session.add(api_response_entry)

                session.commit()
            except Exception as e:
                session.rollback()
                print(f"Error saving records on page {page}: {e}")
            finally:
                session.close()

            total_pages = data.get("data", {}).get("pages", 1)
            has_more = page < total_pages
            page += 1

        print("All pages fetched and stored successfully.")

    except Exception as e:
        print("Exception:", str(e))
       
    session.commit()
    session.close()

# Schedule
scheduler = BlockingScheduler()
scheduler.add_job(
    fetch_and_store_plants,
    'cron',
    hour=18,
    minute=59,
    timezone='UTC'
)
scheduler.add_job(fetch_and_store_devices,   'cron',
    hour=18,
    minute=59,
    timezone='UTC')
scheduler.add_job(call_station_day_energy_list, 'cron',
    hour=14,
    minute=00,
    timezone='UTC')
scheduler.add_job(map_and_save_energy_graph_from_api_responses, 'cron',
    hour=15,
    minute=00,
    timezone='UTC')
scheduler.add_job(fetch_and_store_inverter_day_data, "cron", minute=5)
scheduler.add_job(update_energy_graph_from_power_graph,'cron',
    hour=16,
    minute=30,
    timezone='UTC')
scheduler.add_job(upsert_monthly_energy_summary,'cron',
    hour=16,
    minute=45,
    timezone='UTC')
scheduler.add_job(summarize_energygraph_monthly,'cron',
    hour=17,
    minute=00,
    timezone='UTC')
scheduler.add_job(fetch_and_store_meter_data, "cron", minute="0,20,40")
# Run immediately
if __name__ == "__main__":
    logging.info("=== SolisCloud Sync Script Started ===")

    try:
        fetch_and_store_plants()
        fetch_and_store_devices()
        call_station_day_energy_list()
        map_and_save_energy_graph_from_api_responses()
        fetch_and_store_inverter_day_data()
        update_energy_graph_from_power_graph()
        upsert_monthly_energy_summary()
        fetch_and_store_meter_data()
        summarize_energygraph_monthly()
    except Exception as e:
        logging.exception("Error during initial sync operations")

    scheduler.start()
    
    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        logging.info("Scheduler stopped.")