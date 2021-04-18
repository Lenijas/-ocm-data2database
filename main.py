from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, Boolean
from sqlalchemy.orm import sessionmaker
from sqlalchemy import insert, exists
import json
from pprint import pprint
# todo replace migrate with alembic
from migrate.versioning.schema import Table as TableM, Column as ColumnM
# from alembic import op



engine = create_engine('sqlite:///test.db', echo = True)
Session = sessionmaker(bind=engine)
# meta = MetaData()
#
# students = Table(
#    'students', meta,
#    Column('id', Integer, primary_key = True),
#    Column('name', String),
#    Column('lastname', String),
# )
# meta.create_all(engine)

#
# def insert_element_into_table():
#    engine.dialect.has_table(engine, Variable_tableName)

default_id_column = "ID"

type_mapping = {
    int: Integer,
    str: String,
    float: Float,
    bool: Boolean
}

def _process_sublist(elements, tablename, parent_id, parent_tablename):
    for element in elements:
        _type = type(element)
        if None== _type :
            # todo how to deal with null in table creation
            pass
        elif list == _type:
            raise Exception("Cannot process multiple list")
        elif dict == _type:
            # sub_ele_id= _process_subdict(ele[key],key,ele)
            element[parent_tablename]=parent_id
            json_to_db(element, tablename)
        elif int == _type:
            object_to_store = {}
            object_to_store[tablename] = element
            json_to_db(object_to_store, tablename)
        elif str == _type:
            object_to_store = {}
            object_to_store[tablename] = element
            json_to_db(object_to_store, tablename)
        elif bool == _type:
            object_to_store = {}
            object_to_store[tablename] = element
            json_to_db(object_to_store, tablename)
        elif float == _type:
            object_to_store = {}
            object_to_store[tablename] = element
            json_to_db(object_to_store, tablename)
    pass

def _process_subdict(element, tablename, parent):
    pass


def _create_update_table_if_not_exists(tablename, column_definitions):
    # meta = MetaData(bind=engine)
    meta = MetaData(bind=engine)
    # if not inspect(engine).has_table(tablename):
    table = None
    if not engine.has_table(tablename):
        columns = (Column(name, type_mapping[typ]) for name, typ in column_definitions)

        table = Table(
            tablename, meta, *columns
        )
        meta.create_all(engine)
    else:
        # todo check components
        # table = Table(tablename ,meta, autoload=True, autoload_with=engine)
        table = TableM(tablename ,meta, autoload=True, autoload_with=engine)
        existing_cols = [name for name, info in table.columns.items()]
        # cols_to_add = [(name,Column(name, type_mapping[typ])) for name, typ in column_definitions if name not in existing_cols]
        # table = meta.tables[tablename]
        # with engine.begin() as connection:
        #     alembic_cfg.attributes['connection'] = connection
        #     command.upgrade(alembic_cfg, "head")
        # for col in cols_to_add:
        #     op.add_column(*col)

        cols_to_add = [(name,ColumnM(name, type_mapping[typ])) for name, typ in column_definitions if name not in existing_cols]
        for name, col in cols_to_add:
            col.create(table)
        pass
    return table


def _create_element(element, tablename):
    print("Table: ", tablename)
    pprint(element, indent=2)
    table = _create_update_table_if_not_exists(tablename, ( (key, type(value)) for key, value in element.items()) )

    # query = table.insert()
    # query.values(**element)
    # my_session = Session()
    # my_session.execute(query)
    # my_session.close()

    # CHECK IF ELEMENT EXISTS
    my_session = Session()
    id_hack = {default_id_column: element[default_id_column]}
    if my_session.query(table).filter_by(**id_hack).count():
        pass
    else:

        stmt = (
            insert(table).
                values(**element)
        )
        my_session.execute(stmt)
        my_session.commit()
    my_session.close()


def json_to_db(ele, tablename):
    m_2M_elements = []
    object_to_store = {}
    if default_id_column not in ele:
        return str(ele)
    for key in ele:
        # print(key,"-->",type(ele[key]))
        _type = type(ele[key])
        if None== _type :
            # todo how to deal with null in table creation
            pass
        elif list == _type:
            m_2M_elements.append((ele[key],key))
        elif dict == _type:
            # sub_ele_id= _process_subdict(ele[key],key,ele)
            object_to_store[key] = json_to_db(ele[key],key)
        elif int == _type:
            object_to_store[key] = ele[key]
        elif str == _type:
            object_to_store[key] = ele[key]
        elif bool == _type:
            object_to_store[key] = ele[key]
        elif float == _type:
            object_to_store[key] = ele[key]
    pass

    _create_element(object_to_store, tablename)

    for elements, ele_tablename in m_2M_elements:
        _process_sublist(elements, ele_tablename, ele[default_id_column], tablename)


    return object_to_store[default_id_column]

with open("sample_data/six_poi.json") as f:
    data = json.load(f)

print(type(data))
for ele in data:
    # print(type(data))
    json_to_db(ele, "main")
    # break;

