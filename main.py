from fastapi import FastAPI, File, UploadFile, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
import shutil
import os
import datetime
import pdfplumber
import docx
import pandas as pd
import boto3
app = FastAPI()

UPLOAD_FOLDER = "uploads"
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins (change for security)
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods
    allow_headers=["*"],  # Allow all headers
)

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)  # Create uploads directory if not exists


region = 'eu-north-1'
service = 'es'
credentials = boto3.Session().get_credentials()
auth = AWSV4SignerAuth(credentials, region, service)

# OpenSearch Configuration
OPENSEARCH_HOST = 'search-opensearch-app-zp7shakew3s6gy56wuszxetqvq.eu-north-1.es.amazonaws.com'
OPENSEARCH_PORT = 443


client = OpenSearch(
    hosts=[{"host": OPENSEARCH_HOST, "port": OPENSEARCH_PORT}],
     http_auth = auth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection,
    pool_maxsize = 20
)

INDEX_NAME = "documents"



# Extract text and metadata from different file types
def extract_text_and_metadata(file_path, content_type):
    extracted_text = ""
    metadata = {}

    if content_type == "application/pdf":
        with pdfplumber.open(file_path) as pdf:
            extracted_text = " ".join([page.extract_text() or "" for page in pdf.pages])
            metadata['author'] = pdf.metadata.get('Author', 'Unknown')
            metadata['creation_date'] = pdf.metadata.get('CreationDate', 'Unknown')

    elif content_type in ["application/msword", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"]:
        doc = docx.Document(file_path)
        extracted_text = " ".join([para.text for para in doc.paragraphs])
        metadata['author'] = 'Unknown'
        metadata['creation_date'] = 'Unknown'

    elif content_type in ["application/vnd.ms-excel", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"]:
        df = pd.read_excel(file_path)
        extracted_text = " ".join(df.astype(str).values.flatten())
        metadata['author'] = 'Unknown'
        metadata['creation_date'] = 'Unknown'

    elif content_type.startswith("text/"):  # TXT files
        with open(file_path, "r", encoding="utf-8") as file:
            extracted_text = file.read()
        metadata['author'] = 'Unknown'
        metadata['creation_date'] = 'Unknown'

    return extracted_text.strip(), metadata

@app.get("/")
def main():
    return {"message": "Welcome to the Document Search API!"}

# Upload files and index content
@app.post("/upload/")
async def upload_files(files: list[UploadFile] = File(...)):
    uploaded_files = []
    upload_date = datetime.datetime.now().isoformat()

    for file in files:
        file_path = os.path.join(UPLOAD_DIR, file.filename)
        
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        extracted_text, metadata = extract_text_and_metadata(file_path, file.content_type)

        # Check if file already exists in OpenSearch
        search_query = {"query": {"match": {"filename": file.filename}}}
        existing_docs = client.search(index=INDEX_NAME, body=search_query)

        if existing_docs["hits"]["hits"]:
            print(f"File '{file.filename}' already exists in OpenSearch. Skipping indexing.")
        else:
            document = {
                "filename": file.filename,
                "path": file_path,
                "size": file.size,
                "content_type": file.content_type,
                "upload_date": upload_date,
                "content": extracted_text,
                "author": metadata['author'],
                "creation_date": metadata['creation_date']
            }
            client.index(index=INDEX_NAME, body=document)

        uploaded_files.append({"filename": file.filename, "status": "success"})

    return {"uploaded_files": uploaded_files}


# Search API with content-based search and highlighting
@app.get("/search/")
def search_files(query: str = Query(None)):
    search_query = {
        "query": {
            "multi_match": {
                "query": query,
                "fields": ["filename", "content"]
            }
        },
        "highlight": {
            "fields": {
                "content": {}
            }
        }
    }
    response = client.search(index=INDEX_NAME, body=search_query)

    results = []
    for hit in response["hits"]["hits"]:
        filename = hit["_source"].get("filename", "Unknown file")
        author = hit["_source"].get("author", "Unknown")
        creation_date = hit["_source"].get("creation_date", "Unknown")
        highlighted_content = hit.get("highlight", {}).get("content", [hit["_source"].get("content", "No preview available")[:200]])[0]

        results.append({
            "filename": filename,
            "snippet": highlighted_content,
            "author": author,
            "creation_date": creation_date
        })

    return {"results": results}

# View a file
from fastapi.responses import Response

@app.get("/view/{filename}")
async def view_file(filename: str):
    file_path = os.path.join(UPLOAD_DIR, filename)
    
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")

    with open(file_path, "rb") as file:
        file_data = file.read()

    content_type = "application/pdf" if filename.lower().endswith(".pdf") else "application/octet-stream"
    return Response(content=file_data, media_type=content_type)

# List all uploaded files (Now fetches from OpenSearch too)
@app.get("/files/")
def list_files():
    files = os.listdir(UPLOAD_DIR)
    search_query = {"query": {"match_all": {}}}
    response = client.search(index=INDEX_NAME, body=search_query)
    indexed_files = [hit["_source"]["filename"] for hit in response["hits"]["hits"]]
    return {"files": list(set(files + indexed_files))}

# Download a specific file
@app.get("/files/{filename}")
def get_file(filename: str):
    file_path = os.path.join(UPLOAD_DIR, filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(file_path, filename=filename)

# Delete a specific file
@app.delete("/files/{filename}")
def delete_file(filename: str):
    file_path = os.path.join(UPLOAD_DIR, filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found")
    
    os.remove(file_path)

    delete_query = {"query": {"match": {"filename": filename}}}
    client.delete_by_query(index=INDEX_NAME, body=delete_query)

    return {"message": f"File '{filename}' has been deleted"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8001)
