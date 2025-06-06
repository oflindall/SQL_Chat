from langchain_core.prompts import ChatPromptTemplate

def generate_sql_query(llm, kernel, list_of_tables, user_request, database):
    """
    This simulates the plugin:  
    - fetch schema info from database  
    - build prompt for SQL generation  
    - get SQL query from LLM  
    - run query and return results
    """

    # You need to implement a method in Database to get table schema descriptions
    # For now, just pass the tables list as schema (simplify or enhance later)
    schema_description = "\n".join(list_of_tables.split(","))

    chat = ChatPromptTemplate.from_messages([
        ("system", f"""You create T-SQL queries based on the given schema and user request.
                      Just return T-SQL query, no markdown or explanation.
                      The schema is:
                      {schema_description}
                      """),
        ("human", user_request)
    ])

    prompt_chain = chat | llm

    response = prompt_chain.invoke({"input": user_request})

    sql_query = response.replace("```sql", "").replace("```", "").strip()

    results = database.execute_query(sql_query)

    return results
