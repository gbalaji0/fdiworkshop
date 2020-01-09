import boto3


def create_glue_table(region="us-east-1"):
	conn = boto3.client('glue', region_name=region)

	response = conn.create_table(
    DatabaseName='marketdata',
    TableInput={
        'Name': 'bbg',
        'Description': 'BBG Data',
        'StorageDescriptor': {
            'Columns': [
				{
					"Name": "identifier1",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "identifier2",
					"Type": "bigint",
					"Comment": ""
				},
				{
					"Name": "identifier3",
					"Type": "bigint",
					"Comment": ""
				},
				{
					"Name": "ticker",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "exch_code",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "id_sedol1",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "id_wertpapier",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "id_isin",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "id_dutch",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "id_valoren",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "id_french",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "id_belgium",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "id_bb_company",
					"Type": "bigint",
					"Comment": ""
				},
				{
					"Name": "id_bb_security",
					"Type": "bigint",
					"Comment": ""
				},
				{
					"Name": "id_cusip",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "id_common",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "high_52week",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "low_52week",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "high_dt_52week",
					"Type": "date",
					"Comment": ""
				},
				{
					"Name": "low_dt_52week",
					"Type": "bigint",
					"Comment": ""
				},
				{
					"Name": "px_bid",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "px_mid",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "px_ask",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "px_open",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "px_high",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "px_low",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "px_last",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "px_fixing",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "px_volume",
					"Type": "bigint",
					"Comment": ""
				},
				{
					"Name": "pricing_source",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "last_update",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "eqy_beta",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "cur_mkt_cap",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "eqy_sh_out",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "crncy",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "id_local",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "px_quote_lot_size",
					"Type": "double",
					"Comment": ""
				},
				{
					"Name": "last_update_dt_exch_tz",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "eqy_prim_exch",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "eqy_prim_exch_shrt",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "security_typ",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "Name",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "id_bb_prim_security_flag",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "eqy_prim_security_comp_exch",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "market_status",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "eqy_float",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "eqy_dvd_yld_ind",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "eqy_dvd_yld_12m",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "eqy_dvd_yld_12m_net",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "id_bb_unique",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "market_sector_des",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "cntry_issue_iso",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "px_round_lot_size",
					"Type": "bigint",
					"Comment": ""
				},
				{
					"Name": "composite_exch_code",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "px_nasdaq_close",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "eqy_sh_out_real",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "cntry_of_incorporation",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "cntry_of_domicile",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "mkt_cap_listed",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "px_trade_lot_size",
					"Type": "bigint",
					"Comment": ""
				},
				{
					"Name": "id_sedol2",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "sedol1_country_iso",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "sedol2_country_iso",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "id_mic_prim_exch",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "id_mic_local_exch",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "ticker_and_exch_code",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "id_exch_symbol",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "id_bb_connect",
					"Type": "bigint",
					"Comment": ""
				},
				{
					"Name": "exch_mkt_grp",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "group_name_static",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "id_bb_sec_num_src",
					"Type": "bigint",
					"Comment": ""
				},
				{
					"Name": "settle_dt",
					"Type": "bigint",
					"Comment": ""
				},
				{
					"Name": "id_bb_global",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "composite_id_bb_global",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "id_bb_sec_num_des",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "feed_source",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "trading_system_identifier",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "trading_system_identifier_des",
					"Type": "string",
					"Comment": ""
				},
				{
					"Name": "end-of-fields",
					"Type": "string",
					"Comment": ""
				}
            ],
            'Location': 's3://mod-da6d820750784dd7-simplebucket-1jeg10o4329yx/data/eod_px',
            'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
            'Compressed': True,
            'SerdeInfo': {
                'Name': 'Test',
                'SerializationLibrary': 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe',
                'Parameters': {
                    'field.delim': '|'
                }
            },
            'BucketColumns': [],
            'SortColumns': [],
            'Parameters': {},
            'SkewedInfo': {},
            'StoredAsSubDirectories': False
        },
        'PartitionKeys': []
    }
)



if __name__ == "__main__":
	print(create_glue_table())
	pass

