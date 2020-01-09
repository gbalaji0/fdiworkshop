import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext, DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import to_date

## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
## @type: DataSource
## @args: [database = "marketdata", table_name = "bbg", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "marketdata", table_name = "bbg", transformation_ctx = "datasource0")
def NonEmptyTicker(rec):
    if not rec["ticker"]:
        return False
    return True

def FixDate(rec):
    dt = str(rec["settle_dt"])
    dt = '/'.join( (dt[4:6], dt[6:], dt[:4]) )
    rec["settle_dt_rs"] = dt
    return rec
    
flt_datasource0 = Filter.apply(frame = datasource0, f = NonEmptyTicker)
map_datasource0 = Map.apply(frame = flt_datasource0, f = FixDate)

## @type: ApplyMapping
## @args: [mapping = [("identifier1", "string", "identifier1", "string"), ("identifier2", "long", "identifier2", "long"), ("identifier3", "long", "identifier3", "long"), ("ticker", "string", "ticker", "string"), ("exch_code", "string", "exch_code", "string"), ("id_sedol1", "string", "id_sedol1", "string"), ("id_wertpapier", "string", "id_wertpapier", "string"), ("id_isin", "string", "id_isin", "string"), ("id_dutch", "string", "id_dutch", "string"), ("id_valoren", "string", "id_valoren", "string"), ("id_french", "string", "id_french", "string"), ("id_belgium", "string", "id_belgium", "string"), ("id_bb_company", "long", "id_bb_company", "long"), ("id_bb_security", "long", "id_bb_security", "long"), ("id_cusip", "string", "id_cusip", "string"), ("id_common", "string", "id_common", "string"), ("high_52week", "string", "high_52week", "string"), ("low_52week", "string", "low_52week", "string"), ("high_dt_52week", "date", "high_dt_52week", "date"), ("low_dt_52week", "long", "low_dt_52week", "long"), ("px_bid", "string", "px_bid", "string"), ("px_mid", "string", "px_mid", "string"), ("px_ask", "string", "px_ask", "string"), ("px_open", "string", "px_open", "string"), ("px_high", "string", "px_high", "string"), ("px_low", "string", "px_low", "string"), ("px_last", "string", "px_last", "string"), ("px_fixing", "string", "px_fixing", "string"), ("px_volume", "long", "px_volume", "long"), ("pricing_source", "string", "pricing_source", "string"), ("last_update", "string", "last_update", "string"), ("eqy_beta", "string", "eqy_beta", "string"), ("cur_mkt_cap", "string", "cur_mkt_cap", "string"), ("eqy_sh_out", "string", "eqy_sh_out", "string"), ("crncy", "string", "crncy", "string"), ("id_local", "string", "id_local", "string"), ("px_quote_lot_size", "double", "px_quote_lot_size", "double"), ("last_update_dt_exch_tz", "string", "last_update_dt_exch_tz", "string"), ("eqy_prim_exch", "string", "eqy_prim_exch", "string"), ("eqy_prim_exch_shrt", "string", "eqy_prim_exch_shrt", "string"), ("security_typ", "string", "security_typ", "string"), ("name", "string", "name", "string"), ("id_bb_prim_security_flag", "string", "id_bb_prim_security_flag", "string"), ("eqy_prim_security_comp_exch", "string", "eqy_prim_security_comp_exch", "string"), ("market_status", "string", "market_status", "string"), ("eqy_float", "string", "eqy_float", "string"), ("eqy_dvd_yld_ind", "string", "eqy_dvd_yld_ind", "string"), ("eqy_dvd_yld_12m", "string", "eqy_dvd_yld_12m", "string"), ("eqy_dvd_yld_12m_net", "string", "eqy_dvd_yld_12m_net", "string"), ("id_bb_unique", "string", "id_bb_unique", "string"), ("market_sector_des", "string", "market_sector_des", "string"), ("cntry_issue_iso", "string", "cntry_issue_iso", "string"), ("px_round_lot_size", "long", "px_round_lot_size", "long"), ("composite_exch_code", "string", "composite_exch_code", "string"), ("px_nasdaq_close", "string", "px_nasdaq_close", "string"), ("eqy_sh_out_real", "string", "eqy_sh_out_real", "string"), ("cntry_of_incorporation", "string", "cntry_of_incorporation", "string"), ("cntry_of_domicile", "string", "cntry_of_domicile", "string"), ("mkt_cap_listed", "string", "mkt_cap_listed", "string"), ("px_trade_lot_size", "long", "px_trade_lot_size", "long"), ("id_sedol2", "string", "id_sedol2", "string"), ("sedol1_country_iso", "string", "sedol1_country_iso", "string"), ("sedol2_country_iso", "string", "sedol2_country_iso", "string"), ("id_mic_prim_exch", "string", "id_mic_prim_exch", "string"), ("id_mic_local_exch", "string", "id_mic_local_exch", "string"), ("ticker_and_exch_code", "string", "ticker_and_exch_code", "string"), ("id_exch_symbol", "string", "id_exch_symbol", "string"), ("id_bb_connect", "long", "id_bb_connect", "long"), ("exch_mkt_grp", "string", "exch_mkt_grp", "string"), ("group_name_static", "string", "group_name_static", "string"), ("id_bb_sec_num_src", "long", "id_bb_sec_num_src", "long"), ("settle_dt", "long", "settle_dt", "long"), ("id_bb_global", "string", "id_bb_global", "string"), ("composite_id_bb_global", "string", "composite_id_bb_global", "string"), ("id_bb_sec_num_des", "string", "id_bb_sec_num_des", "string"), ("feed_source", "string", "feed_source", "string"), ("trading_system_identifier", "string", "trading_system_identifier", "string"), ("trading_system_identifier_des", "string", "trading_system_identifier_des", "string"), ("end-of-fields", "string", "end-of-fields", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = map_datasource0, mappings = [
    ("identifier1", "string", "identifier1", "string"), 
    ("identifier2", "long", "identifier2", "long"), 
    ("identifier3", "long", "identifier3", "long"), 
    ("ticker", "string", "ticker", "string"), 
    ("exch_code", "string", "exch_code", "string"), 
    ("id_sedol1", "string", "id_sedol1", "string"), 
    ("id_wertpapier", "string", "id_wertpapier", "string"), 
    ("id_isin", "string", "id_isin", "string"), 
    ("id_dutch", "string", "id_dutch", "string"), 
    ("id_valoren", "string", "id_valoren", "string"), 
    ("id_french", "string", "id_french", "string"), 
    ("id_belgium", "string", "id_belgium", "string"), 
    ("id_bb_company", "long", "id_bb_company", "long"), 
    ("id_bb_security", "long", "id_bb_security", "long"), 
    ("id_cusip", "string", "id_cusip", "string"), 
    ("id_common", "string", "id_common", "string"), 
    ("high_52week", "string", "high_52week", "string"), 
    ("low_52week", "string", "low_52week", "string"), 
    ("high_dt_52week", "date", "high_dt_52week", "date"), 
    ("low_dt_52week", "long", "low_dt_52week", "long"), 
    ("px_bid", "string", "px_bid", "string"), 
    ("px_mid", "string", "px_mid", "string"), 
    ("px_ask", "string", "px_ask", "string"), 
    ("px_open", "string", "px_open", "string"), 
    ("px_high", "string", "px_high", "string"), 
    ("px_low", "string", "px_low", "string"), 
    ("px_last", "string", "px_last", "string"), 
    ("px_fixing", "string", "px_fixing", "string"), 
    ("px_volume", "long", "px_volume", "long"), 
    ("pricing_source", "string", "pricing_source", "string"), 
    ("last_update", "string", "last_update", "string"), 
    ("eqy_beta", "string", "eqy_beta", "string"), 
    ("cur_mkt_cap", "string", "cur_mkt_cap", "string"), 
    ("eqy_sh_out", "string", "eqy_sh_out", "string"), 
    ("crncy", "string", "crncy", "string"), 
    ("id_local", "string", "id_local", "string"), 
    ("px_quote_lot_size", "double", "px_quote_lot_size", "double"), 
    ("last_update_dt_exch_tz", "string", "last_update_dt_exch_tz", "string"), 
    ("eqy_prim_exch", "string", "eqy_prim_exch", "string"), 
    ("eqy_prim_exch_shrt", "string", "eqy_prim_exch_shrt", "string"), 
    ("security_typ", "string", "security_typ", "string"), 
    ("name", "string", "name", "string"), 
    ("id_bb_prim_security_flag", "string", "id_bb_prim_security_flag", "string"), 
    ("eqy_prim_security_comp_exch", "string", "eqy_prim_security_comp_exch", "string"), 
    ("market_status", "string", "market_status", "string"), 
    ("eqy_float", "string", "eqy_float", "string"), 
    ("eqy_dvd_yld_ind", "string", "eqy_dvd_yld_ind", "string"), 
    ("eqy_dvd_yld_12m", "string", "eqy_dvd_yld_12m", "string"), 
    ("eqy_dvd_yld_12m_net", "string", "eqy_dvd_yld_12m_net", "string"), 
    ("id_bb_unique", "string", "id_bb_unique", "string"), 
    ("market_sector_des", "string", "market_sector_des", "string"), 
    ("cntry_issue_iso", "string", "cntry_issue_iso", "string"), 
    ("px_round_lot_size", "long", "px_round_lot_size", "long"), 
    ("composite_exch_code", "string", "composite_exch_code", "string"), 
    ("px_nasdaq_close", "string", "px_nasdaq_close", "string"), 
    ("eqy_sh_out_real", "string", "eqy_sh_out_real", "string"), 
    ("cntry_of_incorporation", "string", "cntry_of_incorporation", "string"), 
    ("cntry_of_domicile", "string", "cntry_of_domicile", "string"), 
    ("mkt_cap_listed", "string", "mkt_cap_listed", "string"), 
    ("px_trade_lot_size", "long", "px_trade_lot_size", "long"), 
    ("id_sedol2", "string", "id_sedol2", "string"), 
    ("sedol1_country_iso", "string", "sedol1_country_iso", "string"), 
    ("sedol2_country_iso", "string", "sedol2_country_iso", "string"),
    ("id_mic_prim_exch", "string", "id_mic_prim_exch", "string"), 
    ("id_mic_local_exch", "string", "id_mic_local_exch", "string"), 
    ("ticker_and_exch_code", "string", "ticker_and_exch_code", "string"),
    ("id_exch_symbol", "string", "id_exch_symbol", "string"), 
    ("id_bb_connect", "long", "id_bb_connect", "long"), 
    ("exch_mkt_grp", "string", "exch_mkt_grp", "string"), 
    ("group_name_static", "string", "group_name_static", "string"), 
    ("id_bb_sec_num_src", "long", "id_bb_sec_num_src", "long"), 
    ("settle_dt", "long", "settle_dt", "long"), 
    ("id_bb_global", "string", "id_bb_global", "string"), 
    ("composite_id_bb_global", "string", "composite_id_bb_global", "string"), 
    ("id_bb_sec_num_des", "string", "id_bb_sec_num_des", "string"), 
    ("feed_source", "string", "feed_source", "string"), 
    ("trading_system_identifier", "string", "trading_system_identifier", "string"), 
    ("trading_system_identifier_des", "string", "trading_system_identifier_des", "string"), 
    ("end-of-fields", "string", "end-of-fields", "string"),
    ("settle_dt_rs", "string", "settle_dt_rs", "string")], transformation_ctx = "applymapping1")
## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_cols", transformation_ctx = "resolvechoice2")
## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = resolvechoice2]
dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")

timestampedDf = dropnullfields3.toDF()
timestampedDf = timestampedDf.withColumn('settle_dt_rsdate', to_date('settle_dt_rs', 'MM/dd/yyyy'))
timestamped4 = DynamicFrame.fromDF(timestampedDf, glueContext, 'timestamped4')

## @type: DataSink
## @args: [catalog_connection = "RedshiftDev", connection_options = {"dbtable": "bbg", "database": "dev"}, redshift_tmp_dir = TempDir, transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = dropnullfields3]
datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = timestamped4, catalog_connection = "RedshiftDev", connection_options = {"dbtable": "bbg", "database": "dev"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink4")
job.commit()
