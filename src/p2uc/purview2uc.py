import re
from databricks.sdk.runtime import *


class PurviewToUC:
  def __init__(self, client, execute):
    self.client = client
    self.regex = re.compile(r'<[^>]+>')
    self.execute = execute

  def remove_html(self, string):
    return self.regex.sub('', string) if (string != None) else ''

  def get_classification_map(self, client):
    typedefs = client.get_all_typedefs()
    classifcation_map = {}

    for cfc in typedefs['classificationDefs']:
      technical_name = cfc['name']
      if ('options' in cfc.keys() and cfc['options'] != None):
        display_name = cfc['options']['displayName']
        classifcation_map[technical_name] = display_name
      else:
        display_name = cfc['description']
        classifcation_map[technical_name] = display_name
    return classifcation_map

  def sanitize_classification(self, s):

      # Remove all non-word characters (everything except numbers and letters)
      s = re.sub(r"[^\w\s]", '', s)

      # Replace all runs of whitespace with a single underscore
      s = re.sub(r"\s+", '_', s)

      return s

  def add_column_tags(self, table_def, classifcation_map, catalog_schema):
    table_name = table_def['table']
    stmt = f'ALTER TABLE {catalog_schema}.{table_name} ALTER COLUMN '
    columns = table_def['columns']
    for column in columns:
      column_name = column['column_name']
      cfn_list = column['classification']
      if (len(cfn_list) > 0):
        for cfn in cfn_list:
          tech_classification = cfn
          if (tech_classification == "MICROSOFT.POWERBI.ENDORSEMENT"):
            result = stmt + f"{column_name} SET TAGS ('Certified');"
          else:
            classification = self.sanitize_classification(
                classifcation_map[tech_classification])
            result = stmt + \
                f"{column_name} SET TAGS ('classification_{classification}');"
          print(f"{result}")
          if (self.execute == "execute statements"):
            spark.sql(result)

  def add_column_comments(self, table_def, catalog_schema):
    table_name = table_def['table']
    stmt = f'ALTER TABLE {catalog_schema}.{table_name} ALTER COLUMN '
    columns = table_def['columns']
    for column in columns:
      column_name = column['column_name']
      if (column['description'] != ''):
        description = column['description']
        result = stmt + f"{column_name} COMMENT '{description}';"
        print(f"{result}")
        if (self.execute == "execute statements"):
          spark.sql(result)

  def get_catalog_schema(self, root):
    arr = root.split("/")
    size = len(arr)
    return f"{arr[size-3]}.{arr[size-2]}"

  def parse_tables(self, table_dump, classifcation_map):
    table_def = {}
    table = table_dump['entities'][0]

    if (table['typeName'] == 'azure_synapse_dedicated_sql_table'):

      root = table['attributes']['qualifiedName']
      catalog_schema = self.get_catalog_schema(root)

      table_def['table'] = table['attributes']['name']
      table_def['columns'] = []

      # process table tags
      labels = table_dump['entities'][0]['labels']
      if (len(labels) > 0):
        tags = ""
        for label in labels:
          tags = tags + f"'{label}',"
        tags = tags.rstrip(",")
        add_label_stmt = f"ALTER TABLE {catalog_schema}.{table_def['table']} SET TAGS ({tags})"

        if (self.execute == "execute statements"):
          spark.sql(add_label_stmt)
        print(f"{add_label_stmt}")

      # process table classifications
      cfns = table_dump['entities'][0]['classifications']
      for cfn in cfns:
        if (cfn['typeName'] == "MICROSOFT.POWERBI.ENDORSEMENT"):
          stmt = f"ALTER TABLE {catalog_schema}.{table_def['table']} SET TAGS ('{cfn['attributes']['endorsement']}')"
          print(f"{stmt}")
          if (self.execute == "execute statements"):
            spark.sql(stmt)
        else:
          tech_classification = cfn['typeName']
          readable_classification = self.sanitize_classification(
              classifcation_map[tech_classification])
          stmt = f"ALTER TABLE {catalog_schema}.{table_def['table']} SET TAGS ('classification_{readable_classification}')"
          print(f"{stmt}")
          if (self.execute == "execute statements"):
            spark.sql(stmt)

      # process table comments
      description = table['attributes']['userDescription']
      add_description_stmt = f"""COMMENT ON TABLE {catalog_schema}.{table_def['table']} IS "{self.remove_html(description)}";"""
      if (self.execute == "execute statements"):
        spark.sql(add_description_stmt)
      print(f"{add_description_stmt}")

      # process columns
      columns = table['relationshipAttributes']['columns']
      for column in columns:
        column_def = {}
        guid = column['guid']
        entity = table_dump['referredEntities'][guid]

        if (entity['typeName'] == 'azure_synapse_dedicated_sql_column' and guid == entity['guid']):
          description = entity['attributes']['userDescription']
          classification = []
          if ('classifications' in entity.keys()):
            cfns = entity['classifications']
            for cfn in cfns:
              classification.append(cfn['typeName'])

          column_def['column_name'] = entity['attributes']['name']
          column_def['data_type'] = entity['attributes']['data_type']
          column_def['length'] = entity['attributes']['length']
          column_def['description'] = self.remove_html(description)
          column_def['classification'] = classification
          table_def['columns'].append(column_def)

      self.add_column_tags(table_def, classifcation_map, catalog_schema)
      self.add_column_comments(table_def, catalog_schema)

  def create_table(self, table_dict):
    table_name = table_dict['table']
    stmt = f'CREATE TABLE IF NOT EXISTS {table_name} ('
    columns = table_dict['columns']
    for column in columns:
      column_name = column['column_name']
      data_type = column['data_type'] if column['data_type'] != 'varchar' else 'string'
      length = column['length']
      description = column['description']

      stmt = stmt + f"{column_name} {data_type}, "

    result = stmt[:-2] + ");"
    return result

  def parse_database(self, database_dump, classifcation_map):
    # get database name
    name = database_dump['entities'][0]['attributes']['name']
    description = database_dump['entities'][0]['attributes']['userDescription']

    # add tags
    labels = database_dump['entities'][0]['labels']
    if (len(labels) > 0):
      tags = ""
      for label in labels:
        tags = tags + f"'{label}',"
      tags = tags.rstrip(",")
      add_label_stmt = f"ALTER CATALOG {name} SET TAGS ({tags})"

    print(f"{add_label_stmt}")
    if (self.execute == "execute statements"):
      spark.sql(add_label_stmt)

    # get descriptions
    add_description_stmt = f"COMMENT ON CATALOG {name} IS '{self.remove_html(description)}';"
    print(f"{add_description_stmt}")
    if (self.execute == "execute statements"):
      spark.sql(add_description_stmt)

    # get classifications
    cfication = database_dump['entities'][0]['classifications']
    for c in cfication:
      if (c['typeName'] == "MICROSOFT.POWERBI.ENDORSEMENT"):
        stmt = f"ALTER CATALOG {name} SET TAGS ('{c['attributes']['endorsement']}')"
        print(f"{stmt}")
        if (self.execute == "execute statements"):
          spark.sql(stmt)
      else:
        tech_classification = c['typeName']
        readable_classification = self.sanitize_classification(
            classifcation_map[tech_classification])
        stmt = f"ALTER CATALOG {name} SET TAGS ('classification_{readable_classification}')"
        print(f"{stmt}")
        if (self.execute == "execute statements"):
          spark.sql(stmt)

  def parse_schema(self, schema_dump, classifcation_map):
    # get schama name
    name = schema_dump['entities'][0]['attributes']['name']

    # get database name
    db = schema_dump['entities'][0]['relationshipAttributes']['db']['displayText']

    # get classifications
    cfication = schema_dump['entities'][0]['classifications']
    for c in cfication:
      if (c['typeName'] == "MICROSOFT.POWERBI.ENDORSEMENT"):
        stmt = f"ALTER SCHEMA {db}.{name} SET TAGS ('{c['attributes']['endorsement']}')"
        if (self.execute == "execute statements"):
          spark.sql(stmt)
        print(f"{stmt}")
      else:
        tech_classification = c['typeName']
        readable_classification = self.sanitize_classification(
            classifcation_map[tech_classification])
        stmt = f"ALTER SCHEMA {db}.{name} SET TAGS ('classification_{readable_classification}')"
        if (self.execute == "execute statements"):
          spark.sql(stmt)
        print(f"{stmt}")

    # get description
    description = schema_dump['entities'][0]['attributes']['userDescription']
    add_description_stmt = f'COMMENT ON SCHEMA {db}.{name} IS "{self.remove_html(description)}";'
    if (self.execute == "execute statements"):
      spark.sql(add_description_stmt)
    print(f"{add_description_stmt}")

    # get tags
    labels = schema_dump['entities'][0]['labels']
    if (len(labels) > 0):
      tags = ""
      for label in labels:
        tags = tags + f"'{label}',"
      tags = tags.rstrip(",")
      add_label_stmt = f"ALTER SCHEMA {db}.{name} SET TAGS ({tags})"

      if (self.execute == "execute statements"):
        spark.sql(add_label_stmt)
      print(f"{add_label_stmt}")

  def process_root(self, purview_root: str, supported_types: str) -> None:
      classifcation_map = self.get_classification_map(self.client)
      search = self.client.discovery.search_entities(purview_root)
      for page in search:
          if (page['entityType'] in supported_types):
              if (page['entityType'] == 'azure_synapse_dedicated_sql_db'):
                  gen = self.client.get_entity(
                      guid=page['id'], qualifiedName=page['qualifiedName'], typeName=page['entityType'])
                  self.parse_database(gen, classifcation_map)
              elif (page['entityType'] == 'azure_synapse_dedicated_sql_schema'):
                  gen = self.client.get_entity(
                      guid=page['id'], qualifiedName=page['qualifiedName'], typeName=page['entityType'])
                  self.parse_schema(gen, classifcation_map)
              elif (page['entityType'] == 'azure_synapse_dedicated_sql_table'):
                  gen = self.client.get_entity(
                      guid=page['id'], qualifiedName=page['qualifiedName'], typeName=page['entityType'])
                  self.parse_tables(gen, classifcation_map)
              else:
                  print("Unsupported Entity Type found !")
