import sys
import logging
import time
log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
# from pyspark.sql import SparkSession
# from pyspark.sql import Row
import csv

KEYSPACE = 'test'
def create_fsa_user():
    session = getSession(KEYSPACE)
    log.info("+-----------Get session successfully-----------+")
    session.execute("DROP TABLE IF EXISTS fsa_user")
    # log.info("+----------------------------------------------+")
    # log.info("+-----DROPED TABLE fsa_user successfully-----+")
    # log.info("+----------------------------------------------+")

    log.info("+----------------------------------------------+")
    log.info("+---------------creating table-----------------+")
    log.info("+----------------------------------------------+")
    session.execute("""
        CREATE TABLE IF NOT EXISTS fsa_user (
            login text PRIMARY KEY,  
            password text, 
            alias text,
            email text,
            date_registered date  
        )
        """)

    log.info("+----------------------------------------------+")
    log.info("------fsa_user table created successfully------+")
    log.info("+----------------------------------------------+")

def create_fsa_site():
    session = getSession(KEYSPACE)
    log.info("+-----------Get session successfully-----------+")
    session.execute("DROP TABLE IF EXISTS fsa_site")
    # log.info("+----------------------------------------------+")
    # log.info("+-----DROPED TABLE fsa_user successfully-----+")
    # log.info("+----------------------------------------------+")

    log.info("+----------------------------------------------+")
    log.info("+---------------creating table-----------------+")
    log.info("+----------------------------------------------+")
    session.execute("""
        CREATE TABLE IF NOT EXISTS fsa_site (
            idsite text PRIMARY KEY,  
            url text, 
            name text,
            login text  
        )
        """)

    log.info("+----------------------------------------------+")
    log.info("-------------fsa_site table created successfully--------+")
    log.info("+----------------------------------------------+")

def create_fsa_log_visit():
    session = getSession(KEYSPACE)
    log.info("+-----------Get session successfully-----------+")
    session.execute("DROP TABLE IF EXISTS fsa_log_visit")
    # log.info("+----------------------------------------------+")
    # log.info("+-----DROPED TABLE fsa_user successfully-----+")
    # log.info("+----------------------------------------------+")

    log.info("+----------------------------------------------+")
    log.info("+---------------creating table-----------------+")
    log.info("+----------------------------------------------+")
    session.execute("""
        CREATE TABLE IF NOT EXISTS fsa_log_visit (
            userid text,  
            fsa text, 
            fsid text,
            m_date int,
            idsite text,
            location_ipv4 text,
            location_ipv6 inet,
            location_browser_lan text,
            location_country_code text,
            location_country_name text,
            location_browser_en text,
            config_os text,
            config_browser_name text,
            config_browser_version text,
            config_resolution text,
            config_color_depth text,
            config_viewport_size text,
            config_java text,
            referal_xxx text,
            PRIMARY KEY(m_date,userid, fsa, fsid)
        )
        """)

    log.info("+----------------------------------------------+")
    log.info("-------------fsa_log_visit table created successfully--------+")
    log.info("+----------------------------------------------+")

def create_user_daily():


    session = getSession(KEYSPACE)
    log.info("+-----------Get session successfully-----------+")
    session.execute("DROP TABLE IF EXISTS user_daily")
    # log.info("+----------------------------------------------+")
    # log.info("+-----DROPED TABLE fsa_user successfully-----+")
    # log.info("+----------------------------------------------+")

    log.info("+----------------------------------------------+")
    log.info("+---------------creating table-----------------+")
    log.info("+----------------------------------------------+")
    session.execute("""
        CREATE TABLE IF NOT EXISTS user_daily (
            userid text,  
            fsa text, 
            fsid text,
            Date date,
            ck_new boolean,
            PRIMARY KEY(userid, fsa, fsid)
        )
        """)

    log.info("+----------------------------------------------+")
    log.info("-------------user_daily table created successfully--------+")
    log.info("+----------------------------------------------+")

def create_draft_user_daily():
    session = getSession(KEYSPACE)
    log.info("+-----------Get session successfully-----------+")
    session.execute("DROP TABLE IF EXISTS draft_user_daily")
    # log.info("+----------------------------------------------+")
    # log.info("+-----DROPED TABLE fsa_user successfully-----+")
    # log.info("+----------------------------------------------+")

    log.info("+----------------------------------------------+")
    log.info("+---------------creating table-----------------+")
    log.info("+----------------------------------------------+")
    session.execute("""
        CREATE TABLE IF NOT EXISTS draft_user_daily (
            userid text,  
            fsa text, 
            fsid text,
            Date date,
            ck_new boolean,
            PRIMARY KEY(userid, fsa, fsid)
        )
        """)

    log.info("+----------------------------------------------+")
    log.info("-------------draft_user_daily table created successfully--------+")
    log.info("+----------------------------------------------+")

def create_fsa_log_link_visit_action():
    session = getSession(KEYSPACE)
    log.info("+-----------Get session successfully-----------+")
    session.execute("DROP TABLE IF EXISTS fsa_log_link_visit_action")
    # log.info("+----------------------------------------------+")
    # log.info("+-----DROPED TABLE fsa_user successfully-----+")
    # log.info("+----------------------------------------------+")

    log.info("+----------------------------------------------+")
    log.info("+---------------creating table-----------------+")
    log.info("+----------------------------------------------+")
    session.execute("""
        CREATE TABLE IF NOT EXISTS fsa_log_link_visit_action (
            userid text,  
            fsa text, 
            fsid text,
            idaction text,
            PRIMARY KEY(userid, fsa, fsid, idaction)
        )
        """)

    log.info("+----------------------------------------------+")
    log.info("-------------fsa_log_link_visit_action table created successfully--------+")
    log.info("+----------------------------------------------+")

def create_fsa_log_action():
    session = getSession(KEYSPACE)
    log.info("+-----------Get session successfully-----------+")
    session.execute("DROP TABLE IF EXISTS fsa_log_action")
    # log.info("+----------------------------------------------+")
    # log.info("+-----DROPED TABLE fsa_user successfully-----+")
    # log.info("+----------------------------------------------+")

    log.info("+----------------------------------------------+")
    log.info("+---------------creating table-----------------+")
    log.info("+----------------------------------------------+")
    session.execute("""
        CREATE TABLE IF NOT EXISTS fsa_log_action (
            idaction text,
            name text,
            PRIMARY KEY(idaction)
        )
        """)

    log.info("+----------------------------------------------+")
    log.info("-------------fsa_log_action table created successfully--------+")
    log.info("+----------------------------------------------+")

def create_user_daily_report():
    session = getSession(KEYSPACE)
    log.info("+-----------Get session successfully-----------+")
    session.execute("DROP TABLE IF EXISTS user_daily_report")
    # log.info("+----------------------------------------------+")
    # log.info("+-----DROPED TABLE fsa_user successfully-----+")
    # log.info("+----------------------------------------------+")

    log.info("+----------------------------------------------+")
    log.info("+---------------creating table-----------------+")
    log.info("+----------------------------------------------+")
    session.execute("""
        CREATE TABLE IF NOT EXISTS user_daily_report (
            date bigint PRIMARY KEY,
            users int
        ) WITH bloom_filter_fp_chance = 0.01
            AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
            AND comment = ''
            AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
            AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
            AND crc_check_chance = 1.0
            AND dclocal_read_repair_chance = 0.1
            AND default_time_to_live = 0
            AND gc_grace_seconds = 864000
            AND max_index_interval = 2048
            AND memtable_flush_period_in_ms = 0
            AND min_index_interval = 128
            AND read_repair_chance = 0.0
            AND speculative_retry = '99PERCENTILE';
        """)
            # date text,
            # users text,
            # PRIMARY KEY(date)
    log.info("+----------------------------------------------+")
    log.info("-------------user_daily_report table created successfully--------+")
    log.info("+----------------------------------------------+")

def create_new_user_daily_report():
    session = getSession(KEYSPACE)
    log.info("+-----------Get session successfully-----------+")
    session.execute("DROP TABLE IF EXISTS new_user_daily_report")
    # log.info("+----------------------------------------------+")
    # log.info("+-----DROPED TABLE fsa_user successfully-----+")
    # log.info("+----------------------------------------------+")

    log.info("+----------------------------------------------+")
    log.info("+---------------creating table-----------------+")
    log.info("+----------------------------------------------+")
    session.execute("""
        CREATE TABLE IF NOT EXISTS new_user_daily_report (
            Date date,
            users int,
            PRIMARY KEY(Date)
        )
        """)

    log.info("+----------------------------------------------+")
    log.info("-------------new_user_daily_report table created successfully--------+")
    log.info("+----------------------------------------------+")

def insert_data_fsa_user(source_path):
    with open(source_path, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        session = getSession(KEYSPACE)
        log.info("+-----------Get session successfully-----------+")
        log.info("+----------------------------------------------+")
        log.info("+----------Begin insert data into fsa_user-------+")
        log.info("+----------------------------------------------+")
        query = SimpleStatement("""
            INSERT INTO fsa_user (
                login, 
                password,
                alias,
                email,
                date_registered
                )
            VALUES (
                %(login)s, 
                %(password)s,
                %(alias)s,
                %(email)s,
                %(date_registered)s
                )
            """, consistency_level=ConsistencyLevel.ONE)
        for row in reader:
            session.execute(
                query, 
                dict(
                    login=row[0], 
                    password=row[1],
                    alias=row[2],
                    email=row[3],
                    date_registered=str(row[4])
                ))
            pass
        log.info("+--------------------------------------------------------+")
        log.info("+-----Insert data into fsa_user successfully----+")
        log.info("+--------------------------------------------------------+")
    pass

def insert_data_fsa_site(source_path):
    with open(source_path, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        session = getSession(KEYSPACE)
        log.info("+-----------Get session successfully-----------+")
        log.info("+----------------------------------------------+")
        log.info("+----------Begin insert data into fsa_site-------+")
        log.info("+----------------------------------------------+")
        query = SimpleStatement("""
            INSERT INTO fsa_site (
                idsite, 
                url,
                name,
                login
                )
            VALUES (
                %(idsite)s, 
                %(url)s,
                %(name)s,
                %(login)s
                )
            """, consistency_level=ConsistencyLevel.ONE)
        for row in reader:
            session.execute(
                query, 
                dict(
                    idsite=row[0], 
                    url=row[1],
                    name=row[2],
                    login=row[3]
                ))
            pass
        log.info("+--------------------------------------------------------+")
        log.info("+-----Insert data into fsa_site successfully----+")
        log.info("+--------------------------------------------------------+")
    pass

def insert_data_fsa_log_visit(source_path):
    with open(source_path, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        session = getSession(KEYSPACE)
        log.info("+-----------Get session successfully-----------+")
        log.info("+----------------------------------------------+")
        log.info("+----------Begin insert data into fsa_log_visit-------+")
        log.info("+----------------------------------------------+")
        query = SimpleStatement("""
            INSERT INTO fsa_log_visit (
            userid,  
            fsa, 
            fsid,
            idsite,
            location_ipv4,
            location_ipv6,
            location_browser_lan,
            location_country_code,
            location_country_name,
            location_browser_en,
            config_os,
            config_browser_name,
            config_browser_version,
            config_resolution,
            config_color_depth,
            config_viewport_size,
            config_java,
            referal_xxx
                )
            VALUES (
                %(userid)s, 
                %(fsa)s,
                %(fsid)s,
                %(idsite)s,
                %(location_ipv4)s,
                %(location_ipv6)s,
                %(location_browser_lan)s,
                %(location_country_code)s,
                %(location_country_name)s,
                %(location_browser_en)s,
                %(config_os)s,
                %(config_browser_name)s,
                %(config_browser_version)s,
                %(config_resolution)s,
                %(config_color_depth)s,
                %(config_viewport_size)s,
                %(config_java)s,
                %(referal_xxx)s
                )
            """, consistency_level=ConsistencyLevel.ONE)
        for row in reader:
            session.execute(
                query, 
                dict(
                    userid=row[0], 
                    fsa=row[1],
                    fsid=row[2],
                    idsite=row[3],
                    location_ipv4=row[4],
                    location_ipv6=row[5],
                    location_browser_lan=row[6],
                    location_country_code=row[7],
                    location_country_name=row[8],
                    location_browser_en=row[9],
                    config_os=row[10],
                    config_browser_name=row[11],
                    config_browser_version=row[12],
                    config_resolution=row[13],
                    config_color_depth=row[14],
                    config_viewport_size=row[15],
                    config_java=row[16],
                    referal_xxx=row[17]
                ))
            pass
        log.info("+--------------------------------------------------------+")
        log.info("+-----Insert data into fsa_log_visit successfully----+")
        log.info("+--------------------------------------------------------+")
    pass

def create_new_user_daily_report(): 
    session = getSession(KEYSPACE) 
    log.info("+-----------Get session successfully-----------+") 
    session.execute("DROP TABLE IF EXISTS new_user_daily_report") 
    # log.info("+----------------------------------------------+") 
    # # log.info("+-----DROPED TABLE fsa_user successfully-----+")
    #  # log.info("+----------------------------------------------+") 
    # log.info("+----------------------------------------------+") 
    # log.info("+---------------creating table-----------------+") log.info("+----------------------------------------------+") 
    session.execute(""" CREATE TABLE IF NOT EXISTS new_user_daily_report ( Date date, users int, PRIMARY KEY(Date, users) ) """) 
    log.info("+----------------------------------------------+") 
    log.info("-------------new_user_daily_report table created successfully--------+") 
    log.info("+----------------------------------------------+")


def create_draft_device_report(): 
    session = getSession(KEYSPACE) 
    log.info("+-----------Get session successfully-----------+") 
    session.execute("DROP TABLE IF EXISTS draft_device_report") 
    # log.info("+----------------------------------------------+") 
    # # log.info("+-----DROPED TABLE fsa_user successfully-----+")
    #  # log.info("+----------------------------------------------+") 
    # log.info("+----------------------------------------------+") 
    # log.info("+---------------creating table-----------------+") log.info("+----------------------------------------------+") 
    session.execute(""" 
        CREATE TABLE IF NOT EXISTS draft_device_report 
        ( 
            bucket int, 
            config_device text,
            device_count int,
            PRIMARY KEY(bucket, config_device) ) """) 
    log.info("+----------------------------------------------+") 
    log.info("-------------new_user_daily_report table created successfully--------+") 
    log.info("+----------------------------------------------+")

def getSession(keySpaceName):
    cluster = Cluster(['10.88.113.74'])
    session = cluster.connect()
    log.info("+------------------------------------------------------+")
    log.info("+-------------------creating keyspace------------------+")
    log.info("+------------------------------------------------------+")
    try:
        session.execute("""
            CREATE KEYSPACE %s
            WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
            AND durable_writes = true;
            """ % keySpaceName)
        log.info("+--------------------------------------------------+")
        log.info("+---------------keyspace created successfully------+")
        log.info("+--------------------------------------------------+")
        
    except:
        log.info("+--------------------------------------------------+")
        log.info("+---------keyspace has been created already!-------+")
        log.info("+--------------------------------------------------+")
    session.set_keyspace(keySpaceName)
    return session
    pass
if __name__ == "__main__":
    # if len(sys.argv) !=4:
    #     log.info('+---------------------------------------------------------+')
    #     log.info('+----------USAGE: spark-submit example_cassandra ---------+')
    #     log.info('+------------------------------<movie input file>---------+')
    #     log.info('+------------------------------<cf result file input>-----+')
    #     exit(-1)
    # path_input1 = sys.argv[1]
    # path_input2 = sys.argv[2]
    # path_input3 = sys.argv[3]
    # create_fsa_user()
    # create_fsa_site()
    # insert_data_fsa_user(path_input1)
    # insert_data_fsa_site(path_input2)
    # create_user_daily()
    # create_draft_user_daily()
    # create_fsa_log_visit()
    create_draft_device_report()
    # insert_data_fsa_log_visit(path_input3)
    # create_user_daily()
    # create_fsa_log_link_visit_action()
    # create_fsa_log_action()
    # create_user_daily_report()
    # create_new_user_daily_report()
    # getSession(KEYSPACE)
    # create_new_user_daily_report()
    pass