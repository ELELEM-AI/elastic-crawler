import os
import json
from google.cloud import secretmanager
from elasticsearch import Elasticsearch, NotFoundError
from typing import Any, Dict, List, Optional


EMBEDDING_MODEL_ID = "text-embedding-005"
JOIN_HEADERS_PROCESSOR = {
    "join": {
        "field": "headings",
        "separator": ", ",
        "on_failure": [{"set": {"field": "headings", "value": ""}}],
    }
}
SET_BODY_PROCESSOR = {
    "set": {
        "field": "text",
        "value": """
      Meta Description: {{{meta_description}}}
      Headings: {{{headings}}}
      Body: {{{body}}} {{{body_content}}}
    """,
    }
}
REMOVE_FIELDS_PROCESSOR = {
    "script": {
        "source": """
          String[] fieldsToRemove = new String[] {
            "body",
            "body_content",
            "meta_description",
            "headings",
            "links",
            "url_port",
            "url_host",
            "url_path",
            "url_path_dir1",
            "url_path_dir2",
            "url_path_dir3",
            "additional_urls",
            "domains",
            "url_scheme"
          };
          for (field in fieldsToRemove) {
              if (ctx.containsKey(field)) {
                  ctx.remove(field);
              }
          }
        """,
        "description": "Runs a script to remove unnecessary fields",
    }
}
SET_DATES_PROCESSOR = {
    "script": {
        "source": """
        if (ctx.containsKey("last_crawled_at") && ctx.last_crawled_at != null) {
            ctx.date = ctx.last_crawled_at;
        } else {
            ctx.date = (new Date()).getTime();
            ctx.last_crawled_at = ctx.date;
        }
        """,
        "description": "Sets the date field based on last_crawled_at or current time",
    }
}
SPLIT_URL_PROCESSOR = {
    "uri_parts": {
        "field": "url",
        "target_field": "temp_url_parts",
        "keep_original": True,
        "ignore_failure": True,
    }
}
NORMALIZED_URL_PROCESSOR = {
    "script": {
        "if": "ctx.containsKey('temp_url_parts')",
        "source": """
    def parts = ctx.temp_url_parts;
    String path = parts.path;
    
    // 1. Handle the path (remove trailing slash)
    if (path != null && path.endsWith('/') && path.length() > 0) {
      path = path.substring(0, path.length() - 1);
    } else if (path == null) {
      path = "";
    }
    
    // 2. Rebuild the normalized URL
    String normalized = parts.scheme + "://" + parts.domain;
    
    // 3. Add port only if it exists
    if (parts.port != null) {
      normalized += ":" + parts.port;
    }
    
    // 4. Add the cleaned path
    normalized += path;
    ctx.normalized_url = normalized;
  """,
    }
}
REMOVE_TEMP_URL_PARTS_PROCESSOR = {
    "remove": {
        "field": "temp_url_parts",
        "ignore_failure": True,
    }
}
VERTEXAI_EMBEDDINGS_PROCESSOR = {
    "inference": {
        "model_id": "vertexai_embeddings",
        "input_output": {
            "input_field": "text",
            "output_field": f"embeddings.{EMBEDDING_MODEL_ID}",
        },
    }
}


def get_es_client() -> Elasticsearch:
    ES_HOST = os.environ["ES_HOST"]
    ES_PORT = os.environ["ES_PORT"]
    ES_API_KEY = os.environ["ES_API_KEY"]
    es_client = Elasticsearch(hosts=f"{ES_HOST}:{ES_PORT}", api_key=ES_API_KEY)
    assert es_client.ping(), "Elasticsearch cluster is not reachable"
    return es_client


def fetch_gcp_secret() -> str:
    """Fetch secret from Google Secret Manager"""
    SECRET_NAME = "snippets-api-project-service-account-json"
    SECRET_MANAGER_PROJECT_ID = "rag-query-analytics"
    client = secretmanager.SecretManagerServiceClient()
    secret_version = client.secret_version_path(
        SECRET_MANAGER_PROJECT_ID, SECRET_NAME, "latest"
    )
    response = client.access_secret_version(name=secret_version)
    secret_value = response.payload.data.decode("UTF-8")
    try:
        _ = json.loads(secret_value)
    except json.JSONDecodeError:
        raise Exception("Secret is not a valid JSON string")
    return secret_value


def setup_ingest_pipeline(
    es_client: Elasticsearch,
    pipeline_id: str,
    description: str,
    processors: List[Dict[str, Any]],
) -> Optional[str]:
    try:
        es_client.ingest.get_pipeline(id=pipeline_id)
        print(f"Ingest pipeline '{pipeline_id}' already exists.")
        return pipeline_id
    except NotFoundError:
        try:
            es_client.ingest.put_pipeline(
                id=pipeline_id, description=description, processors=processors
            )
            print(f"Ingest pipeline '{pipeline_id}' has been created.")
            return pipeline_id
        except Exception as e:
            print(f"Error creating ingest pipeline '{pipeline_id}': {e}")
            return None


def create_vertexai_embedding_inference_endpoint(
    es_client: Elasticsearch, inference_id: str
) -> Optional[str]:
    TASK_TYPE = "text_embedding"
    SERVICE_ACCOUNT_PROJECT_ID = "snippets-api-434014"
    SERVICE_ACCOUNT_LOCATION = "us-central1"
    SERVICE_ACCOUNT_JSON = fetch_gcp_secret()
    try:
        es_client.inference.get(task_type=TASK_TYPE, inference_id=inference_id)
        print(f"Inference endpoint '{inference_id}' already exists.")
        return inference_id
    except NotFoundError:
        try:
            es_client.inference.put_googlevertexai(
                googlevertexai_inference_id=inference_id,
                task_type="text_embedding",
                service_settings={
                    "model_id": EMBEDDING_MODEL_ID,
                    "service_account_json": SERVICE_ACCOUNT_JSON,
                    "location": SERVICE_ACCOUNT_LOCATION,
                    "project_id": SERVICE_ACCOUNT_PROJECT_ID,
                },
            )
            print(f"Inference endpoint '{inference_id}' has been created.")
            return inference_id
        except Exception as e:
            print(f"Error creating inference endpoint '{inference_id}': {e}")
            return None


def create_normalizer_pipeline(es_client: Elasticsearch) -> bool:
    pipeline_id = "es-crawler-normalizer-pipeline"
    description = "Pipeline to normalize crawled data"
    processors = [
        JOIN_HEADERS_PROCESSOR,
        SET_BODY_PROCESSOR,
        REMOVE_FIELDS_PROCESSOR,
        SET_DATES_PROCESSOR,
        SPLIT_URL_PROCESSOR,
        NORMALIZED_URL_PROCESSOR,
        REMOVE_TEMP_URL_PARTS_PROCESSOR,
    ]
    return setup_ingest_pipeline(es_client, pipeline_id, description, processors)


def create_embedding_pipeline(es_client: Elasticsearch) -> bool:
    pipeline_id = "es-crawler-embedding-pipeline"
    description = "Pipeline to generate embeddings for crawled data"
    processors = [VERTEXAI_EMBEDDINGS_PROCESSOR]
    return setup_ingest_pipeline(es_client, pipeline_id, description, processors)


def create_self_served_crawler_pipelines(es_client: Elasticsearch) -> bool:
    normalizer_pipeline_id = create_normalizer_pipeline(es_client)
    if not normalizer_pipeline_id:
        print("Failed to create normalizer pipeline.")
        return False

    embedding_pipeline_id = create_embedding_pipeline(es_client)
    if not embedding_pipeline_id:
        print("Failed to create embedding pipeline.")
        return False

    self_served_pipeline_id = setup_ingest_pipeline(
        es_client,
        pipeline_id="self-served-crawler-pipeline",
        description="Pipeline for self-served crawler to normalize and generate embeddings",
        processors=[
            {"pipeline": {"name": "search-default-ingestion"}},
            {"pipeline": {"name": normalizer_pipeline_id}},
            {"pipeline": {"name": embedding_pipeline_id}},
        ],
    )
    if not self_served_pipeline_id:
        print("Failed to create self-served crawler pipeline.")
        return False
    return True


if __name__ == "__main__":
    try:
        print("Setting up self-served crawler pipelines...")
        es_client = get_es_client()
        success = create_self_served_crawler_pipelines(es_client)
    except Exception as e:
        print(f"An error occurred: {e}")
        success = False
    else:
        if success:
            print("Self-served crawler pipelines have been set up successfully.")
            exit(0)
        else:
            print("Failed to set up self-served crawler pipelines.")
            exit(1)
