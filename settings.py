from enum import Enum

class ColumnName(Enum):
    LTE = ['Site Name', 'Cell Name', 'eNodeB ID', 'PCI', 'RSI', 'TAC', 'Downlink bandwidth']
    UMTS = ['Site Name', 'Cell Name Short', 'Cell Name', 'Cell ID', 'LAC', 'RAC', 'PSC']
    GSM = ['Site Name', 'Cell Name', 'LAC', 'BCCH', 'NCC', 'BCC']

    LTE_NUMERIC = ['eNodeB ID', 'PCI', 'RSI', 'TAC']
    UMTS_NUMERIC= ['Cell ID', 'LAC', 'RAC', 'PSC']
    GSM_NUMERIC = ['LAC', 'BCCH', 'NCC', 'BCC']


db_user_name = 'kanat.atygayev'
db_password = 'Qwerty12345'
db_server_ip = server_ip='10.250.14.42'
db_name='ATOLL_MRAT'
sql_driver = 'SQL SERVER'
radio_data_file_path = 'radio_data.xlsx'
report_file_path = 'report.xlsx'
