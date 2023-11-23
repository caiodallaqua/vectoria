from sentence_transformers import SentenceTransformer
from concurrent.futures import ThreadPoolExecutor
from datasets import load_dataset
import multiprocessing
import pandas as pd
import requests

num_images = 100000
model = SentenceTransformer('all-MiniLM-L6-v2')
tokenizer = model.tokenizer

def valid_url(url: str) -> bool:
    try:
        response = requests.get(url)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False

def valid_sequence_len(input_text: str) -> bool:
  return len(tokenizer.tokenize(input_text)) <= tokenizer.model_max_length

def get_embeddings(df: pd.DataFrame) -> pd.DataFrame:
  urls = df['image_url'].to_list()
  descriptions = df['caption'].to_list()

  embeddings = model.encode(descriptions, batch_size=100, convert_to_numpy=False)

  return pd.DataFrame({'description': descriptions, 'embedding': [e.tolist() for e in embeddings], 'url': urls})

if __name__ == "__main__":
    df = load_dataset('sbu_captions', split='train').to_pandas().sample(n=num_images, random_state=42)

    with ThreadPoolExecutor(max_workers=4*multiprocessing.cpu_count()) as executor:
        df['status_ok'] = list(executor.map(valid_url, df['image_url']))
        df['valid_sequence_len'] = list(executor.map(valid_sequence_len, df['caption']))

    filtered_df = df[df['status_ok'] & df['valid_sequence_len']]

    final_df = get_embeddings(filtered_df[['image_url', 'caption']])
    final_df.to_parquet('sbu_captions_embeddings.parquet', engine='pyarrow', compression='snappy', index=False)