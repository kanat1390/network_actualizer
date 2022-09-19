import pypyodbc as odbc

class DBConnection:
    """Класс который устанавливает срединение с БД"""
    def __init__(self, user_name: str, password: str, server_ip: str, database_name: str, driver: str):
        self.user_name = user_name
        self.password = password
        self.server = server_ip
        self.database = database_name
        self.driver = driver
        self.connection_string = ''
        self.connection = ''
        self.cursor = ''
        self._set_connection_string()
        self._connect()

    def _set_connection_string(self):
        self.connection_string = f"""
            DRIVER={{{self.driver}}};
            SERVER={self.server};
            DATABASE={self.database};
            Trust_Connection=yes;
            uid={self.user_name};
            pwd={self.password};
        """

    def _connect(self) -> None:
        self.connection = odbc.connect(self.connection_string)
        self._set_cursor()

    def _set_cursor(self) -> None:
        self.cursor = self.connection.cursor()