import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
sp500_companies_table_drop = "drop table if exists sp500_companies"
sp500_tickers_table_drop = "drop table if exists sp500_tickers"
sp500_sma30_table_drop = "drop table if exists sp500_sma30"
sp500_bollinger_table_drop = "drop table if exists sp500_bollinger"
sp500_macd_table_drop = "drop table if exists sp500_macd"


# CREATE TABLES
sp500_companies_table_create= ("""
    CREATE TABLE IF NOT EXISTS public.sp500_companies (
            company_id int4 NOT NULL,
            symbol varchar(256) NOT NULL,
            company_name varchar(256) NOT NULL,
            wiki varchar(256),
            sector varchar(256) NOT NULL,
            subsector varchar(256),
            CONSTRAINT company_pkey PRIMARY KEY (company_id)
        );      
""")

sp500_tickers_table_create = ("""
    CREATE TABLE IF NOT EXISTS public.sp500_tickers (
            ticker_id int4 IDENTITY(1,1),
            "date" date NOT NULL,
            company varchar NOT NULL,
            "high" float NOT NULL,
            "low" float NOT NULL,
            "open" float NOT NULL,
            "close" float NOT NULL,
            "volume" float NOT NULL,
            adj_close float NOT NULL
        );
""")

sp500_sma30_table_create = ("""
    CREATE TABLE IF NOT EXISTS public.sp500_sma30 (
            "date" date NOT NULL,
            company varchar NOT NULL,
            adj_close DOUBLE PRECISION NOT NULL,
            sma_30 DOUBLE PRECISION NOT NULL
        );
""")

sp500_bollinger_table_create = ("""
    CREATE TABLE IF NOT EXISTS public.sp500_bollinger (
            "date" date NOT NULL,
            company varchar NOT NULL,
            adj_close DOUBLE PRECISION NOT NULL,
            "20_ma" DOUBLE PRECISION NOT NULL,
            "20_sd" DOUBLE PRECISION NOT NULL,
            upper_band DOUBLE PRECISION NOT NULL,
            lower_band DOUBLE PRECISION NOT NULL
        );
""")

sp500_macd_table_create = ("""
    CREATE TABLE IF NOT EXISTS public.sp500_macd (
            "date" date NOT NULL,
            company varchar NOT NULL,
            adj_close DOUBLE PRECISION NOT NULL,
            "30_mavg" DOUBLE PRECISION NOT NULL,
            "26_ema" DOUBLE PRECISION NOT NULL,
            "12_ema" DOUBLE PRECISION NOT NULL,
            macd DOUBLE PRECISION NOT NULL,
            signal DOUBLE PRECISION NOT NULL,
            crossover DOUBLE PRECISION NOT NULL
        );
""")


# empty the tables before insert the data
sp500_companies_truncate = (""" truncate table public.sp500_companies """)
sp500_tickers_truncate = (""" truncate table public.sp500_tickers """)
sp500_sma30_truncate = (""" truncate table public.sp500_sma30 """)
sp500_bollinger_truncate = (""" truncate table public.sp500_bollinger """)
sp500_macd_truncate = (""" truncate table public.sp500_macd """)

# COPY S3 data into database
# parameters
sp500_companies_data = config.get('S3', 'SP500_COMPANIES_DATA')
sp500_tickers_data = config.get('S3', 'SP500_TICKER_DATA')
sp500_sma30_data = config.get('S3', 'SP500_SMA30_DATA')
sp500_bollinger_data = config.get('S3', 'SP500_BOLLINGER_DATA')
sp500_macd_data = config.get('S3', 'SP500_MACD_DATA')
ARN = config.get('IAM_ROLE', 'ARN')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
#REGION = config.get('DWH', 'DWH_REGION')
REGION = 'us-west-2'

sp500_companies_copy = ("""
     COPY sp500_companies FROM {} 
     iam_role '{}' 
     region '{}' 
     DELIMITER as ';'
     timeformat 'epochmillisecs';
""").format(sp500_companies_data, ARN, REGION)

sp500_tickers_copy = ("""
     COPY sp500_tickers ("Date","company","high","low","open","close","volume","adj_close")  FROM {} 
     iam_role '{}' 
     region '{}' 
     DELIMITER as ','
     CSV
     ignoreheader as 1
     DATEFORMAT AS 'YYYY-MM-DD'
""").format(sp500_tickers_data, ARN, REGION)

sp500_sma30_copy = ("""
     COPY sp500_sma30 ("Date","adj_close","sma_30",company)  FROM {} 
     iam_role '{}' 
     region '{}' 
     DELIMITER as ','
     CSV
     ignoreheader as 1
     DATEFORMAT AS 'YYYY-MM-DD'
""").format(sp500_sma30_data, ARN, REGION)

sp500_bollinger_copy = ("""
     COPY sp500_bollinger ("Date","adj_close","20_ma", "20_sd", upper_band, lower_band, company)  FROM {} 
     iam_role '{}' 
     region '{}' 
     DELIMITER as ','
     CSV
     ignoreheader as 1
     DATEFORMAT AS 'YYYY-MM-DD'
""").format(sp500_bollinger_data, ARN, REGION)

sp500_macd_copy = ("""
     COPY sp500_macd ("Date","adj_close","30_mavg", "26_ema", "12_ema", macd, signal, crossover, company)  FROM {} 
     iam_role '{}' 
     region '{}' 
     DELIMITER as ','
     CSV
     ignoreheader as 1
     DATEFORMAT AS 'YYYY-MM-DD'
""").format(sp500_macd_data, ARN, REGION)




# to delete later ==> insert statements
#sp500_sma30_insert = ("""
#    INSERT INTO sp500_sma30 (date, company, adj_close, sma_30)
#    SELECT date,
#           company,
#           adj_close, 
#           AVG(adj_close)
#                 OVER(ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS sma_30
#    FROM sp500_tickers
#    where company = 'MMM'
#    order by date desc
#""")

"""
insert into sp5oo_stock_facts (date,company,adj_close,sma_30, bol_ma_20, bol_sd_20, bol_upper_band, bol_lower_band)
select date,
       company,
       adj_close,
       sma_30,
       bol_ma_20,
       bol_sd_20.
       bol_upper_band,
       bol_lower_band
from sp500_tickers
join
"""

# main call procedures
create_dim_table_queries       = [ sp500_companies_table_create, 
                               sp500_tickers_table_create]

create_fact_table_queries   = [ sp500_sma30_table_create,
                                sp500_bollinger_table_create,
                                sp500_macd_table_create ]

drop_dim_table_queries      = [ sp500_companies_table_drop, 
                                sp500_tickers_table_drop]

drop_fact_table_queries    = [ sp500_sma30_table_drop, 
                               sp500_bollinger_table_drop,
                               sp500_macd_table_drop ]

truncate_dim_table_queries  = [ sp500_companies_truncate, 
                                sp500_tickers_truncate]

truncate_fact_table_queries = [ sp500_sma30_truncate,
                                sp500_bollinger_truncate,
                                sp500_macd_truncate ]

copy_dim_table_queries      = [ sp500_companies_copy, 
                                sp500_tickers_copy ]

copy_fact_table_queries     = [ sp500_sma30_copy, 
                                sp500_bollinger_copy,
                                sp500_macd_copy ]



