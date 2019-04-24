OUTPUT_FILE = "./full_drugbank_ingestion_with_relationships.cypher"
CSV_DIRECTORY = "<PATH/TO/LOCATION/OF/DRUGBANK/CSV/FILES>"
TABLE_LOAD_FILE = "PATH/TO/LOCATION/OF/DRUGBANK/TABLE/LOAD/FILE"
FOREIGN_KEY_FILE = "PATH/TO/LOCATION/OF/FOREIGN/KEY/FILE"


def output(line, type):
    print(line)
    with open(OUTPUT_FILE, type) as o:
        o.write(line+"\n")


def start_output(line): output(line, type='w')


def append_output(line): output(line, type='a')


def snake_plural_to_camel_singular(snake_string):
    components = snake_string.split("_")
    new_components = []
    for i, c in enumerate(components):
        new_c = c[0].upper() + c[1:]
        if i == len(components) - 1:  # We're at the last word
            if new_c[-1] == 's':      # if the last letter is s then remove it
                new_c = new_c[:-1]    # TODO: This can be vastly improved upon !
        new_components.append(new_c)
    return ''.join(new_components)


def process_content_array(c_arr):
    match_template = "MATCH (a:{first}), (b:{second})"
    where_template = "WHERE a.{first} = b.{second}"
    relationship_create_template = "CREATE (a)-[:{relationship}]->(b);"
    if c_arr[0] == "ALTER" and c_arr[1] == "TABLE":
        first_edge_type = snake_plural_to_camel_singular(snake_string=c_arr[2])
        second_edge_type = snake_plural_to_camel_singular(snake_string=c_arr[10])
        first_identifier = c_arr[8].split('"')[1]
        second_identifier = c_arr[11].split('"')[1]

        append_output(line=match_template.format(first=first_edge_type, second=second_edge_type))
        append_output(line=where_template.format(first=first_identifier, second=second_identifier))
        append_output(line=relationship_create_template.format(relationship=c_arr[2]))


if __name__ == "__main__":
    # Read in the csv file names and column names from load_tables.pgsql.sql
    with open(TABLE_LOAD_FILE) as f:
        start_output(line="")
        load_template = "LOAD CSV FROM 'file://{csv_directory}{csv_location}' AS n"
        create_template = "CREATE (:{node_type} {{{column_names}}});"
        content = f.readlines()
        c_arr = []

        node_type = None
        columns_names = None
        csv_location = None
        for i, c in enumerate(content):
            line_elements = c.strip().split()
            first_element = next((e for e in line_elements), None)
            if not first_element:
                continue

            if first_element == "COPY":
                node_type = snake_plural_to_camel_singular(snake_string=line_elements[1])
            if first_element.split('"')[0] == "(":
                column_names = [e[1] for e in [e.split('"') for e in line_elements]]
            if first_element == "FROM":
                csv_location = line_elements[1].split("/")[-1].split("'")[0]

                if node_type and column_names and csv_location:
                    append_output(line="USING PERIODIC COMMIT 500")
                    append_output(line=load_template.format(csv_location=csv_location, csv_directory=CSV_DIRECTORY))
                    column_names_string = ', '.join(["{}:n[{}]".format(e, i) for i, e in enumerate(column_names)])
                    append_output(line=create_template.format(node_type=node_type, column_names=column_names_string))

                    node_type = None
                    columns_names = None
                    csv_location = None
                    append_output(line="")

    # Read in the foreign key constraints from add_constraints.pgsql.sql
    with open(FOREIGN_KEY_FILE) as f:
        content = f.readlines()
        c_arr = []
        for i, c in enumerate(content):
            for item in c.strip().split():
                c_arr.append(item)
            if c.strip() == "":
                process_content_array(c_arr=c_arr)
                c_arr = []
                append_output(line="")
