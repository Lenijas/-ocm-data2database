from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, Boolean, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy import insert, exists
import json
from geomet import wkt
from pprint import pprint
# todo replace migrate with alembic
from migrate.versioning.schema import Table as TableM, Column as ColumnM
import datetime
from tqdm import tqdm
from StringIteratorIO import StringIteratorIO
import psycopg2
import os
# from alembic import op

# "constants" declaration
TYPE_MAPPING = {
    int: Integer,
    str: String,
    float: Float,
    bool: Boolean,
    datetime.datetime: DateTime
}

DEFAULT_ID_COLUMN = "ID"


class JSON2DB:
    def __init__(self, engine_dsn):
        self.__dsn = engine_dsn
        self.__engine = create_engine(engine_dsn)
        self.__Session = sessionmaker(bind=self.__engine)
        self.__session = None
        self.debug = False
        self.__meta = None
        self.__count = 0

        self._table_structures = {}
        self._elements_to_insert = {}
        self._created_tables_mapping ={}
        self.__csv_separator = "\f"
        self.__csv_null ='null'

        pass

    @property
    def my_session(self):
        # todo check also for closed
        if self.__session is None:
            self.__count = self.__count + 1
            self.__session = self.__Session(expire_on_commit=False, autoflush=False)
        return self.__session

    @property
    def meta(self):
        # todo check also for closed
        if self.__meta is None:
            self.__meta = MetaData(bind=self.__engine)
        return self.__meta

    def __process_sublist(self, elements, tablename, parent_id, parent_tablename):
        for element in elements:
            _type = type(element)
            if _type == type(None):
                # improve todo how to deal with null in table creation
                pass
            elif list == _type:
                raise Exception("Cannot process multiple list")
            elif dict == _type:
                element[parent_tablename]=parent_id
                self._json_to_db(element, tablename)
            elif _type in (int, str, bool, float):
                object_to_store = {}
                object_to_store[tablename] = element
                self._json_to_db(object_to_store, tablename)
        else:
            # todo control unexpected types
            pass
        pass

    def __process_subdict(self, element, tablename, parent):
        pass

    def __save_table_structure(self, tablename, column_definitions):
        if tablename in self._table_structures:

            self._table_structures[tablename] = tuple(set(self._table_structures[tablename] + column_definitions))
        else:
            self._table_structures[tablename] = column_definitions


    def __create_tables(self):

        for tablename, column_definitions in self._table_structures.items():
            columns = (Column(name, TYPE_MAPPING[typ]) for name, typ in column_definitions)

            table = Table(
                tablename, self.meta, *columns
            )
            self._created_tables_mapping[tablename] = table
        self.meta.create_all(self.__engine)

    # def __create_update_table_if_not_exists(self, tablename, column_definitions):
    #     self.__save_table_structure(tablename, column_definitions)
    #     return
    #     # if not inspect(engine).has_table(tablename):
    #     table = None
    #     if not self.__engine.has_table(tablename):
    #         columns = (Column(name, TYPE_MAPPING[typ]) for name, typ in column_definitions)
    #
    #         table = Table(
    #             tablename, self.meta, *columns
    #         )
    #         self.meta.create_all(self.__engine)
    #     else:
    #         # todo check components
    #         table = TableM(tablename,self.meta, autoload=True, autoload_with=self.__engine)
    #         existing_cols = [name for name, info in table.columns.items()]
    #
    #         cols_to_add = [(name,ColumnM(name, TYPE_MAPPING[typ])) for name, typ in column_definitions if name not in existing_cols]
    #         for name, col in cols_to_add:
    #             col.create(table)
    #         pass
    #     return table

    def __prepare_element(self, element, tablename):
        self.__save_table_structure(tablename, tuple((key, type(value)) for key, value in element.items()))
        if tablename not in self._elements_to_insert:
            self._elements_to_insert[tablename] = {element[DEFAULT_ID_COLUMN]: element}
        elif element[DEFAULT_ID_COLUMN] in self._elements_to_insert:
            return
        else:
            self._elements_to_insert[tablename][element[DEFAULT_ID_COLUMN]]= element

    def __get_columns_names(self, tablename):
        return sorted([ele[0] for ele in self._table_structures[tablename]])

    def __elements_to_csv(self, tablename):
        columns = self.__get_columns_names(tablename)
        for id, element in tqdm(self._elements_to_insert[tablename].items(), desc=tablename):

            values = [str(element[col_name]) if col_name in element else self.__csv_null for col_name in columns]
            yield self.__csv_separator.join(values).replace("\n","\\n").replace("\r","\\r")+"\n"

    def __store_elements_pg(self):
        for tablename in self._elements_to_insert:
            columns = self.__get_columns_names(tablename)
            with psycopg2.connect(dsn=self.__dsn) as conn:
                with conn.cursor() as cur:
                    cur.copy_from(StringIteratorIO(self.__elements_to_csv(tablename)), '"{}"'.format(tablename), sep=self.__csv_separator, null=self.__csv_null,
                                  size=8192, columns=['"{}"'.format(col) for col in columns])

    def __store_elements(self):
        for tablename in self._elements_to_insert:
            table = self._created_tables_mapping[tablename]
            for id, element in tqdm(self._elements_to_insert[tablename].items(), desc=tablename):
                stmt = (
                    insert(table).
                        values(**element)
                )
                try:
                    self.my_session.execute(stmt)
                except Exception as e:
                    raise (e)
            self.my_session.commit()

    # def __create_element(self, element, tablename):
    #     if self.debug:
    #         print("Table: ", tablename)
    #         pprint(element, indent=2)
    #     table = self.__create_update_table_if_not_exists(tablename, tuple( (key, type(value)) for key, value in element.items()) )
    #
    #     self.__prepare_element(element,tablename)
    #     return
    #     # query = table.insert()
    #     # query.values(**element)
    #     # my_session = Session()
    #     # my_session.execute(query)
    #     # my_session.close()
    #
    #     # CHECK IF ELEMENT EXISTS
    #     id_hack = {DEFAULT_ID_COLUMN: element[DEFAULT_ID_COLUMN]}
    #     if self.my_session.query(table).filter_by(**id_hack).count():
    #         pass
    #     else:
    #
    #         stmt = (
    #             insert(table).
    #                 values(**element)
    #         )
    #         try:
    #             self.my_session.execute(stmt)
    #         except Exception as e:
    #             raise(e)
    #         self.my_session.commit()

    def _try_especial_data(self, ele, tablename):
        return str(ele)

    def _store(self):
        self.__create_tables()
        self.__store_elements()

    def _store_pg_optimized(self):
        self.__create_tables()
        self.my_session.commit()
        self.__store_elements_pg()

    def _json_to_db(self, ele, tablename):
        m2m_elements = []
        object_to_store = {}
        if DEFAULT_ID_COLUMN not in ele:
            return self._try_especial_data(ele, tablename)
        for key in ele:
            _type = type(ele[key])
            if _type == type(None):
                # todo improve how to deal with null in table creation
                pass
            elif list == _type:
                m2m_elements.append((ele[key],key))
            elif dict == _type:
                object_to_store[key] = self._json_to_db(ele[key],key)
            elif _type in (int, str, bool, float):
                object_to_store[key] = ele[key]
            else:
                # type not supported try to convert to string
                try:
                    object_to_store[key] = str(ele[key])
                except Exception as e:
                    pass
        pass

        self.__prepare_element(object_to_store, tablename)
        # self.__create_element(object_to_store, tablename)

        for elements, ele_tablename in m2m_elements:
            self.__process_sublist(elements, ele_tablename, ele[DEFAULT_ID_COLUMN], tablename)

        return object_to_store[DEFAULT_ID_COLUMN]

    def export_json(self, json_file,root_table_name="main_table"):
        with open(json_file) as f:
            data = json.load(f)
        for ele in data:
            self._json_to_db(ele, root_table_name)
        self._store()
        self.my_session.close()




class OCMData2DB(JSON2DB):

    def __init__(self, engine_dsn):
        super().__init__(engine_dsn)

    def export_json(self, json_file,root_table_name="main_table"):
        with open(json_file, encoding='utf-8') as f:
            for ele in tqdm(f, desc="procesing file"):
                self._json_to_db(json.loads(ele), root_table_name)

        # self.my_session.commit()
        # super()._store()
        super()._store_pg_optimized()
        self.my_session.close()

    def _try_especial_data(self, ele, tablename):
        try:
            if "$date" in ele and 1 == len(ele): # only date {"$date":"2019-04-06T04:01:00Z"}
                try:
                    return datetime.datetime.strptime(ele["$date"],"%Y-%m-%dT%H:%M:%SZ")
                except:
                    return datetime.datetime.strptime(ele["$date"], "%Y-%m-%dT%H:%M:%S.%fZ")
            # for geojsons {"type":"Point","coordinates":[-118.081014,34.050745]}
            elif "type" in ele and "coordinates" in ele:
                return wkt.dumps(ele)
            else:
                return str(ele)
        except:
            return str(ele)


if __name__ == "__main__":
    from timer import timer
    # exporter = JSON2DB('sqlite:///test.db')
    # exporter.export_json("sample_data/six_poi.json")
    #

    # os.remove("test2.db")
    # exporter = OCMData2DB('sqlite:///test2.db')
    # t = timer()
    # t._start("test")
    # exporter.export_json("sample_data/100_invalid.json", "initial_table")
    # t._stop("test")
    # print("Time {}".format(t.elapse))


    exporter = OCMData2DB('postgresql://ruignpdq:msoz_yMqzBpXQ5FVRBnTnSGXDYr3GDGf@tai.db.elephantsql.com:5432/ruignpdq')
    t = timer()
    t._start("test")
    # exporter.export_json("sample_data/100_invalid.json", "initial_table")
    exporter.export_json("sample_data/full_dataser.json", "initial_table")
    t._stop("test")
    print("Time {}".format(t.elapse))



