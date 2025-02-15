from fastapi import FastAPI, Response
import requests
import json
import uvicorn

app = FastAPI()

@app.get("/problem-models")
def get_problem_models():
    url = "https://kenkoooo.com/atcoder/resources/problem-models.json"
    headers = {"Accept-Encoding": "gzip, deflate"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    data = response.json()

    expected_keys = [
        "problem_id",
        "slope",
        "intercept",
        "variance",
        "difficulty",
        "discrimination",
        "irt_loglikelihood",
        "irt_users",
        "is_experimental"
    ]

    lines = []
    for problem_id, model in data.items():
        output_record = { key: model.get(key) for key in expected_keys }
        output_record["problem_id"] = problem_id
        lines.append(json.dumps(output_record, separators=(',', ':')))

    output = "\n".join(lines)

    return Response(content=output, media_type="application/x-ndjson")

if __name__ == '__main__':
    uvicorn.run(app, host="0.0.0.0", port=8000)
