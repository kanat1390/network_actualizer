from db_connection.database import DBConnection
from data_handler.data_handler import DBDataHandler, ExcelDataHandler, DataComporator
import settings


db_connection = DBConnection(user_name=settings.db_user_name, password=settings.db_password, server_ip=settings.db_server_ip, database_name=settings.db_name, driver=settings.sql_driver)

db_data = DBDataHandler(db_connection)

excel_data = ExcelDataHandler(settings.radio_data_file_path)

comporator = DataComporator(excel_data, db_data)