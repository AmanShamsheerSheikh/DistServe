from dotenv import load_dotenv
import os
import requests
from contants import settings

runpod_url = "https://api.runpod.io/graphql"
headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {settings.RUNPOD_API_KEY}"
}

def fetch_runpod_gpu_catalog():    
    query = """
        query {
            gpuTypes {
                id
                displayName
                memoryInGb
                communityPrice
                securePrice
                maxGpuCount
                maxGpuCountSecureCloud
            }
        }
    """

    try:
        response = requests.post(runpod_url, json={'query': query}, headers=headers)
        if response.status_code == 200:
            data = response.json()
            gpu_list = data.get("data", {}).get("gpuTypes", [])
            gpu_list = [gpu for gpu in gpu_list if "NVIDIA" in gpu["id"]]
            return {
                "gpu_count": len(gpu_list),
                "available_gpus": gpu_list
            }
        else:
            return {
                "error": f"Failed to fetch catalog; {response.status_code} - {response.text}"
            }
    except Exception as e:
        return {
            "error": f"Error occured: {str(e)}"
        }