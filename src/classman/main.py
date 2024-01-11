from fastapi import FastAPI

app = FastAPI()


CATEGORIES = {"PHI", "HIPAA", "DSA1", "DSA2"}
ACCESS = {"user-1": ["HIPAA", "PHI", "DSA1"], "user-2": ["PHI"], "user-3": [], "user-4": ["DSA2"]}


@app.get("/access/{user_id}")
async def get_access(user_id: str):
    return {"user_id": ACCESS.get(user_id, [])}


@app.get("/categories")
async def get_categories():
    return list(CATEGORIES)


@app.post("/categories/new")
async def new_category(name: str):
    CATEGORIES.add(name)
    return name
