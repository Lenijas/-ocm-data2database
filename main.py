from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, Boolean
from sqlalchemy.orm import sessionmaker
from sqlalchemy import insert, exists
import json
from pprint import pprint
# todo replace migrate with alembic
from migrate.versioning.schema import Table as TableM, Column as ColumnM
# from alembic import op

# "constants" declaration
TYPE_MAPPING = {
    int: Integer,
    str: String,
    float: Float,
    bool: Boolean
}

DEFAULT_ID_COLUMN = "ID"


class JSON2DB:
    def __init__(self, engine_dsn):
        self.__engine = create_engine(engine_dsn)
        self.__Session = sessionmaker(bind=self.__engine)
        self.__session = None
        self.debug = False
        pass

    @property
    def my_session(self):
        # todo check also for closed
        if self.__session is None:
            self.__session = self.__Session()
        return self.__session

    def __process_sublist(self, elements, tablename, parent_id, parent_tablename):
        for element in elements:
            _type = type(element)
            if _type is None:
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

    def __create_update_table_if_not_exists(self, tablename, column_definitions):
        meta = MetaData(bind=self.__engine)
        # if not inspect(engine).has_table(tablename):
        table = None
        if not self.__engine.has_table(tablename):
            columns = (Column(name, TYPE_MAPPING[typ]) for name, typ in column_definitions)

            table = Table(
                tablename, meta, *columns
            )
            meta.create_all(self.__engine)
        else:
            # todo check components
            table = TableM(tablename ,meta, autoload=True, autoload_with=self.__engine)
            existing_cols = [name for name, info in table.columns.items()]

            cols_to_add = [(name,ColumnM(name, TYPE_MAPPING[typ])) for name, typ in column_definitions if name not in existing_cols]
            for name, col in cols_to_add:
                col.create(table)
            pass
        return table

    def __create_element(self, element, tablename):
        if self.debug:
            print("Table: ", tablename)
            pprint(element, indent=2)
        table = self.__create_update_table_if_not_exists(tablename, ( (key, type(value)) for key, value in element.items()) )

        # query = table.insert()
        # query.values(**element)
        # my_session = Session()
        # my_session.execute(query)
        # my_session.close()

        # CHECK IF ELEMENT EXISTS
        id_hack = {DEFAULT_ID_COLUMN: element[DEFAULT_ID_COLUMN]}
        if self.my_session.query(table).filter_by(**id_hack).count():
            pass
        else:

            stmt = (
                insert(table).
                    values(**element)
            )
            self.my_session.execute(stmt)
            self.my_session.commit()

    def _json_to_db(self, ele, tablename):
        m2m_elements = []
        object_to_store = {}
        if DEFAULT_ID_COLUMN not in ele:
            return str(ele)
        for key in ele:
            _type = type(ele[key])
            if _type is None:
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

        self.__create_element(object_to_store, tablename)

        for elements, ele_tablename in m2m_elements:
            self.__process_sublist(elements, ele_tablename, ele[DEFAULT_ID_COLUMN], tablename)

        return object_to_store[DEFAULT_ID_COLUMN]

    def export_json(self, json_file,root_table_name="main_table"):
        with open(json_file) as f:
            data = json.load(f)
        for ele in data:
            self._json_to_db(ele, root_table_name)
        self.my_session.close()


class OCMData2DB(JSON2DB):

    def __init__(self, engine_dsn):
        super().__init__(engine_dsn)

    def export_json(self, json_file,root_table_name="main_table"):
        with open(json_file) as f:
            for ele in f:
                self._json_to_db(json.loads(ele), root_table_name)

        # self.my_session.commit()
        self.my_session.close()


if __name__ == "__main__":
    exporter = JSON2DB('sqlite:///test.db')
    exporter.export_json("sample_data/six_poi.json")

    exporter = OCMData2DB('sqlite:///test2.db')
    exporter.export_json("sample_data/100_invalid.json")

