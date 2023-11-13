from openai import OpenAI


import os
import glob
import weaviate

client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

def upsert_large_file_to_weaviate():
    """
    Reads all .txt files in the current directory piece by piece, generates OpenAI embeddings for each piece,
    and upserts the pieces to Weaviate along with metadata.

    Returns:
        None
    """
    # Initialize OpenAI API
    
    
    # Initialize Weaviate client
    #client = weaviate.Client(os.environ.get("WEAVIATE_URL"), os.environ.get("WEAVIATE_API_KEY"))

    clientw = weaviate.Client(
    url="https://jp-dictionary-0205r7ke.weaviate.network",  # Replace w/ your endpoint
    auth_client_secret=weaviate.AuthApiKey(api_key="paPLctCQ4LZdkBlRGZ6DAxJU1KJk9R153tFb"),  # Replace w/ your API Key for the Weaviate instance. Delete if authentication is disabled.
    additional_headers={
        "X-OpenAI-Api-Key": "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",
    },
    )

    clientw.is_ready()

    # Create Weaviate class if it doesn't exist
    weaviate_class_name = os.environ.get("WEAVIATE_CLASS")

    # Get all .txt files in the current directory
    txt_files = glob.glob("*.txt")

    # Read each text file piece by piece and upsert to Weaviate
    for text_file_path in txt_files:
        with open(text_file_path, "r") as f:
            text = f.read()
            text_pieces = [text[i:i+1000] for i in range(0, len(text), 1000)]
            for i, text_piece in enumerate(text_pieces):
                # Get OpenAI embedding for text piece
                response = client.completions.create(
                    model="text-embedding-ada-002",
                    prompt=text_piece,
                    max_tokens=1024,
                    n=1,
                    stop=None,
                    temperature=0.5
                )
                embedding = response.choices[0].text

                # Upsert to Weaviate
                try:
                    clientw.data_object.create(
                        {
                            "text": text_piece,
                            "embedding": embedding,
                            "metadata": {
                                "source": "file",
                                "source_id": str(i),
                                "url": str(i),
                                "created_at": str(i),
                                "author": str(i),
                                "filename": text_file_path,
                                "document_id": str(i)  # Convert to string
                            }
                        },
                        weaviate_class_name,
                    )
                    print(f"Upserted {i+1} of {len(text_pieces)} pieces of {text_file_path} to Weaviate")
                except Exception as e:
                    print(f"Failed to upsert {i+1} of {len(text_pieces)} pieces of {text_file_path} to Weaviate: {e}")

    print("Finished upserting all files to Weaviate")

# Call the function
upsert_large_file_to_weaviate()
