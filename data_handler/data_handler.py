from db_connection.database import DBConnection
from typing import List
from settings import ColumnName
import pandas as  pd
import numpy as np
import settings


class DBDataHandler:
    """Класс который принимает в качестве аргумента принимает DBConnection класс и обрабатывает данные и хранит все данные в словаре атрибута self.data"""
    def __init__(self, db_connection: DBConnection):
        self.connection = db_connection
        self.data = {'LTE': self._get_lte_table(),
                     'UMTS': self._get_umts_table(),
                     'GSM': self._get_gsm_table()
                     }

    def _fetch_data(self, sql_command: str, columns: List):
        self.connection.cursor.execute(sql_command)
        return pd.DataFrame(self.connection.cursor.fetchall(), columns=columns)

    def _get_gsm_table(self):
        """Здесь основной процесс по получению данных LTE из БД"""
        gtransmitters = table = self._fetch_data(sql_command='SELECT TX_ID, CONTROL_CHANNEL, BSIC, LAC FROM gtransmitters',
                                          columns=['Cell Name', 'BCCH', 'BSIC', 'LAC'])

        self._add_site_name_column(gtransmitters)
        self._parse_BSIC_column(gtransmitters)
        return self._prepare_final_data(gtransmitters, ColumnName.GSM.value, ColumnName.GSM_NUMERIC.value)



    def _parse_BSIC_column(self, gtransmitters: pd.DataFrame):
        """Функция парсит колонку BSIC и разбивает ее на BCC, NCC"""
        gtransmitters['NCC'] = gtransmitters['BSIC'].str[-1]
        gtransmitters['BSIC'] = gtransmitters['BSIC'].fillna(value=np.nan).apply(
            lambda x: x if x != np.nan and len(str(x)) > 1 else ('0' + x) if x != np.nan else np.nan)
        gtransmitters['BCC'] = gtransmitters['BSIC'].str[0]
        gtransmitters['NCC'] = gtransmitters['BSIC'].str[1]




    def _get_lte_table(self):
        """ Здесь основной процесс по получению данных LTE из БД"""
        lcells = table = self._fetch_data(sql_command='SELECT CELL_ID, PHY_CELL_ID, PRACH_RSI_LIST, FBAND FROM lcells',
                                          columns=['Cell Name', 'PCI', 'RSI', 'Downlink bandwidth'])
        ltransmitters = self._fetch_data(sql_command='SELECT TX_ID, eNodeB_ID, TAC FROM ltransmitters',
                                         columns=['Cell Name', 'eNodeB ID', 'TAC'])

        self._add_site_name_column(lcells)
        self._add_site_name_column(ltransmitters)

        #Удаляем дубликаты из таблицы ltransmitters так как там данные на уровне БС TAC eNodeb ID
        ltransmitters = self._remove_table_dublicates_by_column(ltransmitters, 'Site Name')

        #Удаляем колонку Сell Name из transmitters так как она нам не нужна в дальнейшем
        ltransmitters = self._remove_table_column_by_name(ltransmitters, 'Cell Name')

        self._add_bandwidth_column_lcells(lcells)

        #Мерджим две таблицы
        final_result = self._merge_lte_tables(lcells, ltransmitters)

        return self._prepare_final_data(final_result, ColumnName.LTE.value, ColumnName.LTE_NUMERIC.value)

    def _get_umts_table(self):
        """ Здесь основной процесс по получению данных UMTS из БД"""
        ucells = self._fetch_data(sql_command='SELECT CELL_ID, SCRAMBLING_CODE, CELL_IDENTITY FROM ucells',
                                          columns=['Cell Name', 'PSC', 'Cell ID'])

        utransmitters = self._fetch_data(sql_command='SELECT TX_ID, LAC, RAC FROM utransmitters',
                                         columns=['Cell Name', 'LAC', 'RAC'])

        self._add_site_name_column(ucells)
        self._add_site_name_column(utransmitters)

        final_result = self._merge_umts_tables(ucells, utransmitters)

        self._add_umts_short_cell_name(final_result)

        return self._prepare_final_data(final_result, ColumnName.UMTS.value, ColumnName.UMTS_NUMERIC.value)

    def _add_umts_short_cell_name(self, ucells: pd.DataFrame):
        ucells['Cell Name Short'] = ucells['Cell Name'].apply(lambda x: x[:-2])

    def _add_site_name_column(self, table: pd.DataFrame) -> None:
        """Функция по добавлению колонки Site Name по имени Cell Name"""

        regex = "[A-Z]{2,3}\d{3,4}" #Regex для определения имени БС по имени Cell Name

        table['Site Name'] = np.where(table['Cell Name'].str.contains(regex, regex=True),
                                                  table['Cell Name'].str.findall(regex).str[0], None)

    def _remove_table_column_by_name(self, table: pd.DataFrame, column_name: str):
        table = table.drop([column_name], axis=1)
        return table

    def _add_bandwidth_column_lcells(self, lcells_table: pd.DataFrame) -> None:

        regex = "- (\d{2,3})" #Regex для определения ширины полосы типа 1860 FDD - 20 MHz Altel (E-UTRA Band 3) -> 20

        lcells_table['Downlink bandwidth'] = np.where(lcells_table['Downlink bandwidth'].str.contains(regex, regex=True),
                                         lcells_table['Downlink bandwidth'].str.findall(regex).str[0], None)
        #Приводим их к виду который будет в данных с excel файла
        lcells_table.loc[lcells_table['Downlink bandwidth'] == '20', 'Downlink bandwidth'] ='CELL_BW_N100'
        lcells_table.loc[lcells_table['Downlink bandwidth'] == '15', 'Downlink bandwidth'] = 'CELL_BW_N75'
        lcells_table.loc[lcells_table['Downlink bandwidth'] == '10', 'Downlink bandwidth'] = 'CELL_BW_N50'
        lcells_table.loc[lcells_table['Downlink bandwidth'] == '5', 'Downlink bandwidth'] = 'CELL_BW_N25'

    def _merge_lte_tables(self, lcells_table: pd.DataFrame, ltransmitters_table: pd.DataFrame):
        """Функция мерджит две таблицы lcells и ltransmitters"""
        return lcells_table.merge(ltransmitters_table, on='Site Name', how="inner")

    def _merge_umts_tables(self, ucells_table: pd.DataFrame, utransmitters_table: pd.DataFrame):
        return ucells_table.merge(utransmitters_table, on=['Site Name', 'Cell Name'], how="outer")

    def _remove_table_dublicates_by_column(self, table: pd.DataFrame, column_name: str):
        return table.drop_duplicates(subset=column_name)

    def _prepare_final_data(self, table: pd.DataFrame, columns_name_list: List, numeric_columns: List):
        table = table[columns_name_list]
        table = self._fillna_all_columns(table)
        self._convert_columns_to_numeric(table, numeric_columns)
        return table

    def _fillna_all_columns(self, table: pd.DataFrame):
        table = table.fillna(value=np.nan)
        return table

    def _convert_columns_to_numeric(self, table: pd.DataFrame, numeric_columns: List):
        for column_name in numeric_columns:
            table[column_name] = pd.to_numeric(table[column_name], errors='coerce',  downcast='float')

class ExcelDataHandler:
    """"Класс обрабатывающий файл radio_data.xlsx в качестве аргумента принимает путь до файла"""
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.data = {}
        self._get_excel_data()

    def _get_excel_data(self):
        self._read_file()
        self._prepare_final_data()

    def _columns_format_correction(self):
        self.data['UMTS']['LAC'] = self.data['UMTS']['LAC'].apply(lambda x: x.split('(')[-1][:-1])
        self.data['UMTS']['RAC'] = self.data['UMTS']['RAC'].apply(lambda x: x.split('(')[-1][:-1])
        self.data['GSM']['LAC'] = self.data['GSM']['LAC'].apply(lambda x: x.split('(')[-1][:-1])

    def _read_file(self):
        try:
            with pd.ExcelFile(self.file_path) as file:
                sheet_names = file.sheet_names
                self._append_data(sheet_names, file)
        except FileNotFoundError as e:
            print(f"File {self.file_path} not found")

    def _append_data(self, sheet_names: List, file):
        for sheet_name in sheet_names:
            self.data.update({sheet_name: file.parse(sheet_name)})

    def _prepare_final_data(self):

        self._columns_format_correction()

        self._add_site_name_column(self.data['LTE'])
        self._add_site_name_column(self.data['UMTS'])
        self._add_site_name_column(self.data['GSM'])

        self._add_umts_short_cell_name(self.data['UMTS'])

        self.data['LTE'] = self.data['LTE'][ColumnName.LTE.value]
        self.data['UMTS'] = self.data['UMTS'][ColumnName.UMTS.value]
        self.data['GSM'] = self.data['GSM'][ColumnName.GSM.value]

        self.data['LTE']= self._fillna_all_columns(self.data['LTE'])
        self.data['UMTS']  = self._fillna_all_columns(self.data['UMTS'])
        self.data['GSM'] = self._fillna_all_columns(self.data['GSM'])

        self._convert_columns_to_numeric(self.data['LTE'], ColumnName.LTE_NUMERIC.value)
        self._convert_columns_to_numeric(self.data['GSM'], ColumnName.GSM_NUMERIC.value)
        self._convert_columns_to_numeric(self.data['UMTS'], ColumnName.UMTS_NUMERIC.value)

    def _add_umts_short_cell_name(self, ucells: pd.DataFrame):
        ucells['Cell Name Short'] = ucells['Cell Name'].apply(lambda x: x[:-2])

    def _fillna_all_columns(self, table: pd.DataFrame):
        table = table.fillna(value=np.nan)
        return table

    def _convert_columns_to_numeric(self, table: pd.DataFrame, numeric_columns: List):
        for column_name in numeric_columns:
            table[column_name] = pd.to_numeric(table[column_name], errors='coerce', downcast='float')

    def _add_site_name_column(self, table: pd.DataFrame) -> None:
        """Функция по добавлению колонки Site Name по имени Cell Name"""

        regex = "[A-Z]{2,3}\d{3,4}" #Regex для определения имени БС по имени Cell Name

        table['Site Name'] = np.where(table['Cell Name'].str.contains(regex, regex=True),
                                                  table['Cell Name'].str.findall(regex).str[0], None)


class DataComporator:

    def __init__(self, excel_data: ExcelDataHandler, db_data: DBDataHandler):
        self.excel_data = excel_data.data
        self.db_data = db_data.data
        self._get_missing_cells()
        self._compare_data()

    def _get_missing_cells(self):
        umts = pd.concat([self.db_data['UMTS']['Cell Name Short'], self.excel_data['UMTS']['Cell Name Short'],
                          self.db_data['UMTS']['Cell Name Short']]).drop_duplicates(keep=False)
        umts = pd.merge(self.excel_data['UMTS'], umts, how='right')['Cell Name']
        gsm = pd.concat([self.db_data['GSM']['Cell Name'], self.excel_data['GSM']['Cell Name'],
                         self.db_data['GSM']['Cell Name']]).drop_duplicates(keep=False)
        lte = pd.concat([self.db_data['LTE']['Cell Name'], self.excel_data['LTE']['Cell Name'],
                         self.db_data['LTE']['Cell Name']]).drop_duplicates(keep=False)

        print(gsm)

        #Здесь нужно будет сохранять данные

    def _compare_data(self):
        lte = self.excel_data['LTE'].merge(self.db_data['LTE'], on=['Cell Name', 'Site Name'], how="inner")
        for column_name in ColumnName.LTE.value[2:]:
            lte[column_name] = lte[column_name + '_x'] == lte[column_name + '_y']

        umts = self.excel_data['UMTS'].merge(self.db_data['UMTS'], on=['Cell Name Short', 'Site Name'], how="inner")
        for column_name in ColumnName.UMTS.value[2:]:
            umts[column_name] = umts[column_name + '_x'] == umts[column_name + '_y']
        umts.drop(['Cell Name Short'], axis=1)


        gsm = self.excel_data['GSM'].merge(self.db_data['GSM'], on=['Cell Name', 'Site Name'], how="inner")
        for column_name in ColumnName.GSM.value[2:]:
            gsm[column_name] = gsm[column_name + '_x'] == gsm[column_name + '_y']

        self._save_report(lte, umts, gsm)

    def _save_report(self, lte, umts, gsm):
        with pd.ExcelWriter(settings.report_file_path) as writer:
            lte.to_excel(writer, sheet_name='LTE', index=False)
            umts.to_excel(writer, sheet_name='UMTS', index=False)
            gsm.to_excel(writer, sheet_name='GSM', index=False)