import streamlit as st
import PyPDF2
import docx
import ollama
import pandas as pd

def extract_text_from_pdf(file):
    reader = PyPDF2.PdfReader(file)
    text = "\n".join([page.extract_text() for page in reader.pages if page.extract_text()])
    return text

def extract_text_from_docx(file):
    doc = docx.Document(file)
    text = "\n".join([para.text for para in doc.paragraphs])
    return text

def extract_text_from_csv(file):
    df = pd.read_csv(file)
    return df.to_string(index=False)

def get_ollama_response(prompt):
    response_placeholder = st.empty()
    full_response = ""
    for chunk in ollama.chat(model="llama3.2", messages=[{"role": "user", "content": prompt}], stream=True):
        full_response += chunk["message"]["content"]
        response_placeholder.markdown(full_response + "▌")  # Show typing effect

    response_placeholder.markdown(full_response) 
    return full_response

def main():
    st.set_page_config(page_title="Docbot", page_icon="🤖")
    st.title("DocChat")
    st.sidebar.title("Upload & Settings")
    
    uploaded_file = st.sidebar.file_uploader("Chat with Your files", type=["pdf", "docx", "txt", "csv"])
    
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    user_query = st.chat_input("Type your question here:")

    if uploaded_file:
        file_type = uploaded_file.name.split(".")[-1]
        if file_type == "pdf":
            document_text = extract_text_from_pdf(uploaded_file)
        elif file_type == "docx":
            document_text = extract_text_from_docx(uploaded_file)
        elif file_type == "csv":
            document_text = extract_text_from_csv(uploaded_file)
        else:
            document_text = uploaded_file.getvalue().decode("utf-8")
        
        with st.sidebar.expander("📑 Extracted Document Content:"):
            st.text_area("", document_text, height=200)

        if user_query:
            context = f'''
            You are given a document to read. You have to answer the following questions based on the content of the document.
            if the question is not related to the document, please answer short in general context.

            Document Content: {document_text}

            User Question : {user_query}"
            '''
            response = get_ollama_response(context)
            st.session_state.chat_history.append({"role": "user", "content": user_query})
            st.session_state.chat_history.append({"role": "bot", "content": response})
    else:
        if user_query:
            response = get_ollama_response(user_query)
            st.session_state.chat_history.append({"role": "user", "content": user_query})
            st.session_state.chat_history.append({"role": "bot", "content": response})

    # if st.session_state.chat_history:
    #     for chat in st.session_state.chat_history:
    #         st.write(f"{chat['role'].capitalize()}: {chat['content']}")

if __name__ == "__main__":
    main()