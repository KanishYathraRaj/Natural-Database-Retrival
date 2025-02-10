import requests
import pymysql
import time
import os
import streamlit as st
import json
from rapidfuzz import process

DB_CONFIG = {
    "host": "localhost",
    "user": "github_user",
    "password": "root",
    "database": "elevate_v2",
    "port": 3306,
}

OLLAMA_URL = "http://localhost:11434/api/generate"

payload = {
    "model": "llama3.2",
    "prompt": "Testing prompt",
    "stream": False
}

tables = {
    "t_bu": "Business Unit",
    "t_bug": "Bug Reports",
    "t_commit_file": "Commit Files",
    "t_configs": "Config Settings",
    "t_cost_proj_advisor_checks": "Cost Advisor Checks",
    "t_cost_proj_advisor_low_util_ec2": "Low Utilization EC2",
    "t_cost_project_user_role": "Project User Roles",
    "t_em_answers": "Employee Answers",
    "t_em_arch_pillar": "Architecture Pillars",
    "t_em_cloud_service_master": "Cloud Service Master",
    "t_em_devops_questions": "DevOps Questions",
    "t_em_metric_master": "Metric Master",
    "t_em_metric_trend": "Metric Trend",
    "t_em_metric_trend_copy": "Metric Trend Copy",
    "t_em_project": "Projects",
    "t_em_project_cloud_service": "Project Cloud Services",
    "t_em_questions": "Questions",
    "t_em_well_arch_score": "Well-Architected Scores",
    "t_employee": "Employees",
    "t_employee_alias": "Employee Aliases",
    "t_employee_metric_trend": "Metric Trends",
    "t_manager_employee": "Manager Relationships",
    "t_org": "Organizations",
    "t_org_domain": "Organization Domains",
    "t_org_repo": "Repositories",
    "t_pr_commit": "Pull Request Commits",
    "t_pr_review": "Pull Request Reviews",
    "t_pr_review_copy": "Reviews Copy",
    "t_qa_test_case": "Test Cases",
    "t_qa_test_case_copy": "Test Cases Copy",
    "t_repo_pr": "Repository Pull Requests",
    "t_squad": "Squads",
    "t_squad_employee": "Squad Employees",
    "t_theme": "Themes"
}

with open('ner.json', 'r') as f:
    ner = json.load(f)

ALL_ENTITIES = {word.lower(): word for category in ner.values() for word in category}

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
RATE_LIMIT_DELAY = 1
MAX_RETRIES = 3

def get_LLM_response(prompt):
    last_request_time = 0
    retries = 0

    while retries < MAX_RETRIES:
        try:
            if retries > 0:
                backoff_delay = RATE_LIMIT_DELAY * (2 ** retries)
                time.sleep(backoff_delay)

            now = time.time()
            time_since_last_request = now - last_request_time
            if time_since_last_request < RATE_LIMIT_DELAY:
                time.sleep(RATE_LIMIT_DELAY - time_since_last_request)

            last_request_time = time.time()

            url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-thinking-exp-01-21:generateContent?key={GEMINI_API_KEY}"
            headers = {"Content-Type": "application/json"}
            payload = {
                "contents": [
                    {
                        "parts": [
                            {"text": prompt}
                        ]
                    }
                ]
            }

            response = requests.post(url, headers=headers, json=payload)

            if response.status_code != 200:
                raise Exception(f"API request failed with status {response.status_code}")

            data = response.json()
            review_suggestions = data['candidates'][0]['content']['parts'][0]['text']
            return review_suggestions

        except Exception as error:
            retries += 1
            if retries == MAX_RETRIES or "429" not in str(error):
                st.error(f"Error in API request: {error}")
                raise error
    return None

def get_columns(table_name):
    connection = pymysql.connect(host=DB_CONFIG['host'], user=DB_CONFIG['user'], password=DB_CONFIG['password'], database=DB_CONFIG['database'])
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SHOW COLUMNS FROM {table_name}")
            columns = cursor.fetchall()
            return columns
    finally:
        connection.close()

def execute_sql_queries(queries):
    connection = pymysql.connect(host=DB_CONFIG['host'], user=DB_CONFIG['user'], password=DB_CONFIG['password'], database=DB_CONFIG['database'])
    results = []
    try:
        with connection.cursor() as cursor:
            for query in queries:
                try:
                    cursor.execute(query)
                    data = {
                        'query': query,
                        'data': cursor.fetchall()
                    }
                    results.append(data)
                except Exception:
                    results.append({
                        'query': query,
                        'data': "No data"
                    })
        return results
    finally:
        connection.close()

def correct_spelling(user_input: str, threshold: int = 85) -> str:
    words = user_input.split()
    corrected_words = []
    for word in words:
        result = process.extractOne(word.lower(), ALL_ENTITIES.keys(), score_cutoff=threshold)
        if result:
            match = result[0]
        else:
            match = None
        if match:
            corrected_words.append(ALL_ENTITIES[match])
        else:
            corrected_words.append(word)
    return " ".join(corrected_words)

history = []

def main():
    st.title("Chatbase")
    st.sidebar.title("Input Configs")
    with st.sidebar.expander(" # **Input Configs**"):
        DB_CONFIG['host'] = st.text_input("Database Host", DB_CONFIG['host'])
        DB_CONFIG['user'] = st.text_input("Database User", DB_CONFIG['user'])
        DB_CONFIG['password'] = st.text_input("Database Password", DB_CONFIG['password'], type="password")
        DB_CONFIG['database'] = st.text_input("Database Name", DB_CONFIG['database'])
        DB_CONFIG['port'] = st.text_input("Database Port", DB_CONFIG['port'])

    user_input = st.sidebar.text_area("Ask Your Database", placeholder="Ask your question here...")
    spell_corrected_input = correct_spelling(user_input)
    tagged_input = []
    for word in spell_corrected_input.split():
        for entity, values in ner.items():
            if word.lower() in [v.lower() for v in values]:
                tag = entity
                break
        else:
            tag = "O"
                
        if tag == "O":
            tagged_input.append(word)
        else:
            tagged_input.append(f"{word}({tag})")
    tagged_input = " ".join(tagged_input)
    st.sidebar.info(f"Tagged Input: {tagged_input}")

    if st.sidebar.button("Send"):
        with st.spinner("Processing..."):
            query = f'''
            Select the relevant tables from the table names list based on the user input.

            user input: {user_input}
            ner tagged user input: {tagged_input}

            table names list with their descriptions: 
            {tables}

            strictly format your response with only the relevant table names
            and ensure you only generate the relevant tables name from the table list.

            Your response should be in the following format:
            <table_name_1>, <table_name_2>, <table_name_3>, ...
            '''
            relevant_tables = get_LLM_response(query)
            st.sidebar.success(f"Relevant Tables:\n {relevant_tables}")
            relevant_tables = relevant_tables.split(',')

            table_columns = {}

            for relevant_table in relevant_tables:
                columns = get_columns(relevant_table.strip())
                table_columns[relevant_table.strip()] = columns

            query = f'''
            You have been provided with the database tables and their columns.
            Generate necessary SQL queries based on the user input.
            Ensure the SQL queries give the relevant information as per user input.

            user input: {user_input}
            ner data: {ner}
            database tables with their columns: 
            {table_columns}

            Instruction:
            Strictly format your response with only the SQL queries
            and ensure you only generate the queries based on the given database structure.
            if you need to join tables, ensure you do so based on the given tables.
            if you need to use subqueries, ensure you do so based on the given tables.
            if user requests for past n months data, ensure to use DATE_SUB() function to get the data.
            don't use any hardcoded values in the queries.
            dont't add 'sql' or '```' in the queries.
            dont't generate create, update, delete queries even if the user asks for it.

            Your response should be in the following format:
            <sql_query_1>;
            <sql_query_2>; 
            <sql_query_3>; 

            Example:
            SELECT * FROM table_name WHERE condition;
            SELECT column1, column2 FROM table_name WHERE condition;
            SELECT column1 FROM table_name WHERE condition in (SELECT column2 FROM table_name WHERE condition);
            ...
            '''

            sql_queries = get_LLM_response(query)
            st.sidebar.warning(sql_queries)

            # Extract the SQL queries from the response
            sql_queries_list = sql_queries.strip().split(';')

            # Remove the ```sql and ``` from the list
            sql_queries_list = [query for query in sql_queries_list if query not in ['```sql', '```']]
            # st.sidebar.info(f"Generated SQL Queries: {sql_queries_list}")
            st.sidebar.title("SQL Queries")
            new_sql_queries_list = []
            for sql_query in sql_queries_list:
                sql_query = sql_query.replace('\n', ' ').strip() + ";"
                new_sql_queries_list.append(sql_query)
            for sql_query in new_sql_queries_list:
                st.sidebar.info(f"{sql_query}")

            # Execute the SQL queries and fetch the data
            data = execute_sql_queries(new_sql_queries_list)
            st.sidebar.warning(f"Query Results: {data}")

            query = f'''
            You have been provided with query results containing relevant information.  

            Answer the user query based on this data in a clear, informative, and natural way, 
            ensuring the response is free of database-specific elements such as IDs, column names, and table names.  

            ### User Query:  
            {user_input}  

            ### Relevant Data:  
            {data}  

            ### Instructions:  
            - Provide a well-structured and descriptive response.  
            - Avoid referencing database-specific elements such as column names, table names, or raw IDs.  
            - Ensure the response is natural and understandable to the user.  
            - Strictly format your response with only the answer to the user query.  
            - Your response should be enclosed within `<answer>` tags.  

            ### Response Format:
            <answer>  
            Your response here  
            </answer>
            '''

            answer = get_LLM_response(query)
            answer = answer.replace("<answer>", "").replace("</answer>", "").strip()
            history.append({"user": user_input, "bot": answer})

            # Save history to session state
            if 'history' not in st.session_state:
                st.session_state.history = []
            st.session_state.history.append({"user": user_input, "bot": answer})

            # Display chat history
            for item in reversed(st.session_state.history):
                st.info(f"**User🧑‍💻:** {item['user']}")
                st.success(f"**Bot🤖:** {item['bot']}")

if __name__ == "__main__":
    main()
